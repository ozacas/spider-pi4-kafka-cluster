#!/usr/bin/python3
from utils.features import is_artefact_packed
from timeit import default_timer

files = ['banners.js', 'customize-preview.min.js', 
         'js_FstyfktqH7YIIhgoh7Bh5TxaRwbxopnnm45m3ntsnHg.js', 'packed2.js',
         'best-control-test-1.js', 'fieldRequiredWhenNotAfterGoLiveValidation.js',
         'json2_4.9.2.min.js', 'packed3.js',
         'bootstrap-popover.js', 'google-analytics.js', 'layerslider.kreaturamedia.jquery.js?ver=6.10.0', 
         'personalisation.js', 'brandAnalytics.js', 'jquery-1.12.4.js', 'mediaelement-and-player.min.js',
         'plugin.js', 'ca.js', 'jquery.getUrlParam.js', 'modernizr-2.6.2.custom.min.js', 'polyfill.min.js',
         'customize-preview.js', 'js.cookie.min.js?ver=2.1.4', 
         'packed1.js', 'ui-bootstrap-tpls.min.js' ]

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.timer = default_timer
        
    def __enter__(self):
        self.start = self.timer()
        return self
        
    def __exit__(self, *args):
        end = self.timer()
        self.elapsed_secs = end - self.start
        self.elapsed = self.elapsed_secs * 1000  # millisecs
        if self.verbose:
            print('elapsed time: {} ms'.format(self.elapsed))

for file in files:
    content = None
    with open("test-javascript/{}".format(file), 'rb') as fp:
        content = fp.read()
    print("Running test for {}".format(file))
    with Timer(verbose=True): 
        for int in range(10000):
            t = is_artefact_packed(file, content)
