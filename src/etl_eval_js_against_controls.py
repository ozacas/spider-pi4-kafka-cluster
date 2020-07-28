#!/usr/bin/python3
import os
import pymongo
import json
import argparse
import sys
import hashlib
import pylru
from kafka import KafkaConsumer, KafkaProducer
from utils.features import *
from utils.models import JavascriptArtefact, JavascriptVectorSummary, BestControl
from utils.io import next_artefact, find_or_update_analysis_content, batch
from utils.misc import *
from dataclasses import asdict

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
add_kafka_arguments(a,
                    consumer=True,
                    producer=True, # and save to a topic
                    default_from='analysis-results',
                    default_group='etl-eval-js-against-controls',
                    default_to="javascript-artefact-control-results")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--file", help="Debug specified file and exit []", type=str, default=None)
a.add_argument("--min-size", help="Skip all controls less than X bytes [1500]", type=int, default=1500)
a.add_argument("--defensive", help="Validate each message from kafka for corruption (slow)", action="store_true")
a.add_argument("--max-distance", help="Limit control hits to those with AST*Call distance < [300.0]", type=float, default=300.0)
g = a.add_mutually_exclusive_group(required=False)
g.add_argument("--report-bad", help="Report artefacts to stdout which dont hit anything [False]", action="store_true")
g.add_argument("--report-good", help="Report artefacts to stdout which hit a control [False]", action="store_true")
g.add_argument("--report-all", help="Report all artefacts to stdout regardless of distance [False]", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Ctrl-C pressed. Cleaning up...")
    try:
        consumer.close()
        mongo.close()
    except NameError:
        pass # NameError occurs when using --file as consumer has not been setup since it is not required
    rm_pidfile('pid.eval.controls', root='.')
    sys.exit(0)

if args.v:
   print(db.javascript_controls.distinct('family'))

if args.file:
   with open(args.file, 'rb') as fp:
       content = fp.read()
       jsr = JavascriptArtefact(url=args.file, sha256=hashlib.sha256(content).hexdigest(), js_id='0' * 24,
                                md5=hashlib.md5(content).hexdigest(), size_bytes=len(content))
       byte_content, failed, stderr = analyse_script(content, jsr, feature_extractor=args.extractor)
       if failed:
           raise ValueError("Failed to analyse script: {}\n{}".format(jsr, stderr))
       m = json.loads(byte_content.decode())
       m.update(asdict(jsr))
       print(m)
       best_control, next_best_control = find_best_control(db, m,
                                                           max_distance=args.max_distance, debug=True)
       update_literal_distance(db, best_control, m['literals_by_count'], fail_if_difference=best_control.sha256_matched) # check that literals are ok too...

       print("*** WINNING CONTROL HIT")
       print(best_control)
       print("*** NEXT BEST CONTROL HIT")
       print(next_best_control)
       cleanup()

def save_vetting(db, hit: BestControl) -> dict:
    d = asdict(hit)
    assert 'cited_on' in d and len(d['cited_on']) > 0
    assert 'control_url' in d  # control url may be empty for poor quality (or no) hits
    assert 'origin_url' in d and len(d['origin_url']) > 0
    assert isinstance(d['origin_js_id'], str) or d['origin_js_id'] is None

    control_url = hit.control_url
    origin_url = hit.origin_url
    ret = db.vet_against_control.find_one_and_update({ 'origin_url': origin_url, 'control_url': control_url },
                                                     { "$set": d},
                                                     upsert=True,
                                                     return_document=pymongo.ReturnDocument.AFTER)
    assert ret is not None and '_id' in ret
    xref = str(ret.get('_id'))
    assert xref is not None
    d['xref'] = xref
    return d

def process_hit(db, m, args):
    """
    Process a message (m) indicating an artefact under test and identify the best available control
    May raise ValueError if the result is illogical: sha256_matched but literal differences identified (often this is an
    indicator of data error inside the MongoDB). Results are published to kafka (args.to topic) and db.vet_against_control in Mongo
    """
    assert db is not None
    assert isinstance(m, dict)
    # example m: {'url': 'https://homes.mirvac.com/-/media/Project/Mirvac/Residential/Base-Residential-Site/Base-Residential-Site-Theme/scripts/navigationmin.js',
    #             'sha256': 'd6941fcfdd12069e20f4bb880ecbab12d797d9696cae1b05ec9d59fb9bd90b51', 'md5': '2b10377115ab0747535acb1ad38b26bd',
    #             'inline': False, 'content_type': 'text/javascript', 'when': '2020-06-04 03:28:28.483855', 'size_bytes': 14951,
    #             'origin': 'https://homes.mirvac.com/homes-portfolio',  'js_id': '5e8ef7df582045cdd24ce8ae' }
    best_control_is_hit = False
    best_control, next_best_control = find_best_control(db, m, debug=args.v, control_cache=vector_cache,
                                                        max_distance=args.max_distance)
    ovec = m['literals_by_count']
    if best_control is None or len(best_control.control_url) == 0:
        pass
    else:
        # 2. check that sha256's match each artefact iff find_best_control() said the hashes matched
        #    (since next_best_control has distance it cannot be a hash match)
        if args.defensive and best_control.sha256_matched:
            cntl_doc = db.javascript_controls.find_one({ 'origin': best_control.control_url })
            assert cntl_doc is not None
            ######### Vet the control URL
            # temporarily printed as assert's are rarely failing - most likely an error elsewhere in the pipeline
            #print("artefact hash: ", m['sha256'], m['md5'], " control_hashes: ", cntl_doc.get('sha256'), cntl_doc.get('md5'))
            assert cntl_doc.get('size_bytes') == m['size_bytes']
            assert cntl_doc.get('sha256') == m['sha256']  # these exist to catch sha256 hash collisions if any, to verify whether if they happen in production...
            assert cntl_doc.get('md5') == m['md5']

        update_literal_distance(db, best_control, ovec, fail_if_difference=args.defensive and best_control.sha256_matched)
        best_control_is_hit = True

    d = save_vetting(db, best_control)
    best_control.xref = d['xref']
    assert best_control.xref is not None
    producer.send(args.to, d)
    if (args.report_bad or args.report_all) and len(best_control.control_url) == 0:
        print(best_control)
    if (args.report_good or args.report_all) and len(best_control.control_url) > 0:
        print(best_control)

    if next_best_control is not None and len(next_best_control.control_url) > 0:
        assert not next_best_control.sha256_matched  # must not be true since it would have been the best_control if it were
        update_literal_distance(db, next_best_control, ovec)
        d2 = save_vetting(db, next_best_control)
        next_best_control.xref = d2['xref']
        assert len(next_best_control.xref) > 0
        if next_best_control.literal_dist < 0.0: # if the literal distance calculation fails (eg. missing control features) we ignore the hit
            return False

        best_mult = best_control.distance()
        next_best_mult = next_best_control.distance()
        if next_best_mult <= best_mult and next_best_mult < 50.0: # only report good hits though... otherwise poor hits will generate lots of false positives
            print("NOTE: next best control looks as good as best control")
            print(next_best_control)
            producer.send(args.to, d2)
            assert d2['xref'] != d['xref']  # xref must not be the same document otherwise something has gone wrong

    return best_control_is_hit

def calculate_vectors(db, m, defensive, java, extractor, force=False):
    vectors_as_dict = find_or_update_analysis_content(db, m, defensive=defensive, force=force, java=java, extractor=extractor)
    assert isinstance(vectors_as_dict, dict)
    for t in ['statements_by_count', 'calls_by_count', 'literals_by_count']:
         m[t] = vectors_as_dict[t]  # YUK... side effect m
    return vectors_as_dict  # most callers wont use it... but hey why not...

if __name__ == "__main__":
    group = args.group
    if len(group) < 1:
        group = None
    consumer = KafkaConsumer(args.consume_from, group_id=group, auto_offset_reset=args.start, enable_auto_commit=False, max_poll_interval_ms=60000000,
                             bootstrap_servers=args.bootstrap, value_deserializer=json_value_deserializer())
    producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)
    setup_signals(cleanup)
    # 1. process the analysis results topic to get vectors for each javascript artefact which has been processed by 1) kafkaspider AND 2) etl_make_fv
    save_pidfile('pid.eval.controls', root='.')
    print("Creating required index in vet_against_control collection... please wait")
    db.vet_against_control.create_index([( 'origin_url', pymongo.ASCENDING), ('control_url', pymongo.ASCENDING )], unique=True)
    print("Index creation complete.")

    vector_cache = pylru.lrucache(25 * 1000) # bigger the better since we want to avoid large-scale mongo queries
    batch_size = 2000
    batch_id = 0                     # used to keep track of last_successful_batch and last batch
    last_successful_batch_id = None
    fail_runif_consecutive_bad = 5   # abort run if more than this number of batches consecutively are considered bad
    expected_hits = 200              # NB: relative to batch_size ie. at least 10% must be hits or we fail the batch
    for batch_of_messages in batch(next_artefact(consumer, args.n, filter_cb=javascript_only(), verbose=args.v), n=batch_size):
        good_in_batch = 0
        for m in sorted(batch_of_messages, key=lambda v: v.get('sha256')):
            try:
                calculate_vectors(db, m, args.defensive, args.java, args.extractor)
            except ValueError as ve:
                print("WARNNG - ignoring script which could not be analysed: {}".format(m))
                print(str(ve))
                continue

            try:
                is_hit = process_hit(db, m, args)
            except ValueError as ve:
                # if we get a ValueError with args.defensive it likely means data corruption since
                # there should be no literal differences when sha256 matches. So we update the database record and try again.
                # Otherwise raise as per normal
                if args.defensive:
                    print(str(ve))
                    print("WARNING: recalculating db entry in case of data-corruption:")
                    calculate_vectors(db, m, args.defensive, args.java, args.extractor, force=True)
                    is_hit = process_hit(db, m, args)
                else:
                    raise ve

            if is_hit:
                good_in_batch += 1

        print("Got {} hits in batch of {} (require at least {} to be successful batch).".format(good_in_batch, batch_size, expected_hits))
        consumer.commit() # update consumer offsets since the batch is now processed
        if good_in_batch < expected_hits:
            tmp = last_successful_batch_id if last_successful_batch_id is not None else 0
            assert batch_id - tmp < fail_runif_consecutive_bad
            print("WARNING: batch id {} failed: only {} hits".format(batch_id, good_in_batch))
        else:
            last_successful_batch_id = batch_id
        batch_id += 1

    cleanup()
