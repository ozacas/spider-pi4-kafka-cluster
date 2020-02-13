#!/usr/bin/python3
import json
import csv
import sys
from kafka import KafkaConsumer


def consumer():
    return KafkaConsumer('analysis-results', bootstrap_servers='kafka1', auto_offset_reset='earliest',
                          consumer_timeout_ms=1000, group_id=None, # ensure consumer does not wait once it consumes all messages
                          value_deserializer=lambda m: json.loads(m.decode('utf-8'))) # dont set group id, we dont want to keep an offset for this program

def identify_fields_in_results():
    fields = set()
    # NB: only interested in AST statement counts for now
    for message in consumer():
       d = message.value 
       fields.update(d['statements_by_count'].keys())
    return fields


if __name__ == "__main__":
    fields = identify_fields_in_results()
    f = list(fields)
    f.append('url') 
    with sys.stdout as fp:
        writer = csv.DictWriter(fp, delimiter='\t', fieldnames=f)
        writer.writeheader()
        for message in consumer(): 
            d = message.value['statements_by_count']
            d['url'] = message.value['id']
            writer.writerow(d)
    exit(0)
