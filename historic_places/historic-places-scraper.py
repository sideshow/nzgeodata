#!/usr/bin/env python
# encoding: utf-8
"""
historic-places-scraper.py

Scrape the NZ Historic Places register from http://www.historic.org.nz/

Copyright (c) 2008 Robert Coup.

Requires BeautifulSoup and SimpleJson libraries. Optionally,
the WorkerPool library allows us to do multithreaded grabbing.

Outputs a JSON array with an object for each entry, containing
each of the defined fields for that entry. Strips <br/> tags and
redundant whitespace. Will skip any errors it finds, outputting
a message to stderr.

By default, running this script will download all the entries and print
the JSON to stdout. Add a set of records IDs (eg. 22) to the command line 
to only output the specified records. "-v" as the first argument will
print out the HTML page and HTTP headers for each entry retrieved.

  Normal usage:
    ./historic-places-scraper.py > historic.json

  Just grab entries 22 and 7:
    ./historic-places-scraper.py 22 7 > historic.json

  Debug entry 22, printing the HTML and headers:
    ./historic-places-scraper.py -v 22

Note that their database is missing plenty of ID numbers in the sequence 
(they'll return 500 errors), and the fields aren't remotely consistent across
all entries.

Please be considerate of their site.
"""

import sys
import os
import urllib2
import re
import pprint
import random
from BeautifulSoup import BeautifulSoup
import simplejson

try:
    import workerpool
    THREADED = True
    THREADPOOL_SIZE = 3     # concurrent download/parse threads
except ImportError:
    THREADED = False

URL_RECENT = 'http://www.historic.org.nz/Register/Recent_Registrations.html'
URL_REGISTER = "http://www.historic.org.nz/Register/ListingDetail.asp?RID=%s&p=print"

FIELD_MAP = {   # map of field names in the HTML to keys in our output
    u'brief history:': u'history',
    u'city/district council:': u'council',
    u'construction dates:': u'construction_dates',
    u'construction professionals:': u'construction_by',
    u'current use:': u'current_use',
    u'date registered:': u'date_registered',
    u'gps references:': u'gps_ref',
    u'notable features:': u'notable_features',
    u'other names:': u'other_names',
    u'region:': u'region',
    u'register number:': u'register_no',
    u'registration type:': u'registration_type',
    u'former uses:': u'former_uses',
    u'other information:': u'other_info',
    u'nz archaeological association site number:': u'nzaa_site_no',
    u'entry written by:': u'entry_by',
    u'entry completed:': u'entry_completed',
    u'links:': u'links',
    u'location description:': u'location_desc',
    u'status explanation:': u'status_desc',
    u'area description:': u'area_desc',
}
RE_GPS = re.compile(r'Easting:\s*(\d+).*Northing:\s*(\d+)', re.I | re.U)

RE_STRIP_WS = (
    (re.compile(r'\s+', re.M), u' '),
    (re.compile(r'<\s*BR\s*/?>', re.I | re.M), u'\n'),
    (re.compile(r'\n+', re.M), u'\n'),
)

def strip_ws(s):
    try:
        s = s.decode('utf-8').strip()
        for f,r in RE_STRIP_WS:
            s = f.sub(r, s)
        s = s.strip()
        return s
    except Exception, e:
        print >>sys.stderr, u"Got error processing (%s)" % e
        raise

def find_max_index():
    """ 
    Figure out the newest entry added to the register by looking at the 
    recent changes page.
    """
    RE_MAX = re.compile(r'RID=(?P<id>\d+)')
    page = urllib2.urlopen(URL_RECENT)
    soup = BeautifulSoup(page)
    r_max = 1
    for link in soup('a', href=re.compile('/Register/ListingDetail\.asp')):
        m = RE_MAX.search(link['href'])
        if m:
            r_max = max(r_max, int(m.group('id')))
    return r_max

def get_info(id, verbose=False):
    """
    Get a dictionary of info about a particular entry. At a minimum, the
    entry will have an 'id' and a 'title' attribute.
    
    verbose: print out the page headers & parsed HTML for debugging purposes.
    """
    page = urllib2.urlopen(URL_REGISTER % id)
    if verbose:
        print >>sys.stderr, page.info()
    
    content = page.read()
    try:
        soup = BeautifulSoup(content, convertEntities=BeautifulSoup.HTML_ENTITIES)
    except:
        if verbose:
            # Unicode or serious parse errors, so dump the raw HTML
            print >>sys.stderr, content
        raise
    
    if verbose:
        print >>sys.stderr, soup.prettify()
    
    page_info = {
        u'id': int(id),
    }
    
    # the site returns 200-OK responses where it means 404
    # FIXME: Checking for a title is the best way to test an entry?
    tn = soup.find('td', {'class':"ListingHeader"})
    if tn:
        page_info[u'title'] = strip_ws(tn.renderContents())
    else:
        raise Exception("No title! (probably 404, but the website sucks)")
    
    tn = soup.find('td', {'class':"ListingSubHeader"})
    if tn:
        page_info[u'subtitle'] = strip_ws(tn.renderContents())
    else:
        print >>sys.stderr, "Subtitle not found"
    
    for field in soup.findAll('td', {'class':'listingfieldname'}):
        f_str = strip_ws(field.string)
        f_val = strip_ws(field.nextSibling.renderContents())
        f_key = FIELD_MAP.get(f_str.lower())
        if not f_key:
            # new field, we should add it to the FIELD_MAP
            print >>sys.stderr, 'Field not found in map: "%s"' % f_str
            continue
        
        if f_key in ('gps_ref', 'nzaa_site_no'):
            # parse the GPS references into X & Y values
            # these are in NZMG (EPSG:27200)
            m = RE_GPS.search(f_val)
            if m:
                page_info['gps_x'] = int(m.group(1))
                page_info['gps_y'] = int(m.group(2))
        else:
            page_info[f_key] = f_val
    
    return page_info

def do_item(id, verbose):
    # grab the info for an entry and JSONify it to stdout
    try:
        info = get_info(id, verbose)
        print simplejson.dumps(info, indent=(verbose and 2 or None)) + u","
    except Exception, e:
        print >>sys.stderr, "Got an error with entry %d: %s" % (id, str(e))

def main(args):
    verbose = False
    
    # TODO: proper argument handling via OptionParser
    if len(args):
        if args[0] in ('-h', '--help'):
            print __doc__
            sys.exit(2)
        
        if args[0] == "-v":
            # verbose mode
            args = args[1:]
            verbose = True
    
    print "["
    try:
        if len(args):
            # we just want to grab specific IDs
            for id in args:
                id_n = int(id)
                print >>sys.stderr, "%d ..." % id_n
                do_item(int(id_n), verbose)
            
        else:
            # grab everything
            r_max = find_max_index()
            print >>sys.stderr, "Searching through %d register entries..." % (r_max+2)
            entries = list(range(1, r_max+1))
            
            if THREADED:
                # work in parallel via a workerpool of threads
                class GrabJob(workerpool.Job):
                    def __init__(self, id):
                        self.id = id
                    def run(self):
                        do_item(self.id, verbose)
                
                pool = workerpool.WorkerPool(size=THREADPOOL_SIZE)
                for id in entries:
                    pool.put(GrabJob(id))
                
                # Send shutdown jobs to all threads, and wait until all the jobs have been completed
                # FIXME: allow KeyboardInterrupt
                pool.shutdown()
                pool.wait()                
            else:
                # do it in a random order, more likely to pick up errors that way
                random.shuffle(entries)
                for id in entries:
                    print >>sys.stderr, id
                    do_item(id)
    finally:
        # FIXME: we have a trailing comma here (most libraries don't care though)
        print "]"

if __name__ == '__main__':
    main(sys.argv[1:])
