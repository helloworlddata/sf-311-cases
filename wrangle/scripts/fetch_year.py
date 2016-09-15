"""
python fetch_year.py 2012 > 2012.csv


This script is overly complicated because we want to idempotenly
fetch data, year by year. The Socrata API does not allow for more than
50,000 records to be fetched in a single call.
"""

from copy import copy
from csv import DictReader, DictWriter
from loggy import loggy
from sys import stdout
import argparse
import requests

LOGGY = loggy('fetch_year.py')

MAX_LIMIT = 50000

BASE_SRC_URL = "https://data.sfgov.org/resource/vw6y-z8j6.csv"
DEFAULT_PARMS = {
    '$limit': MAX_LIMIT,
    '$order': 'case_id',
    '$offset': 0,
}

def query_timespan(gte_year, lte_year, offset):
    parms = copy(DEFAULT_PARMS)
    parms['$offset'] = offset
    parms['$where'] = "opened >= '{0}' and opened <= '{1}'".format(
        '%s-01-01T00:00:00' % lte_year,
        '%s-12-31T23:59:59' % gte_year)
    resp = requests.get(BASE_SRC_URL, params=parms)
    if resp.status_code == 200:
        return resp
    else:
        LOGGY.error("Receieved status code %s" % resp.status_code)
        LOGGY.error(resp.url)
        LOGGY.error(resp.text)
        raise IOError("Receieved status code %s" % resp.status_code)

def fetch_full_timespan(gte_year, lte_year):
    offset = 0
    all_lines = []

    while True:
        resp = query_timespan(gte_year, lte_year, offset)
        txt = resp.text
        lines = txt.splitlines()

        LOGGY.info(resp.url)
        LOGGY.info("\tOffset: %s" % offset)
        LOGGY.info("\tLines received (+ header): %s" % len(lines))

        if not all_lines: # need to include header
            all_lines.extend(lines)
        else:
            all_lines.extend(lines[1:])

        if len(lines) < MAX_LIMIT + 1:
            LOGGY.info("\tBreaking loop; %s lines received is less than limit of %s" % (len(lines), MAX_LIMIT + 1))
            break
        else:
            offset += MAX_LIMIT

    LOGGY.info("Total lines (+ header): %s" % len(all_lines))
    return DictReader(all_lines)





if __name__ == '__main__':
    parser = argparse.ArgumentParser("Download data from %s by year" % BASE_SRC_URL)
    parser.add_argument('year', type=int)
    parser.add_argument('--lte', action='store_true', help="Include dates less than or equal to year")
    args = parser.parse_args()
    year = args.year
    gte_year = year
    lte_year = '1900' if args.lte else year
    csvin = fetch_full_timespan(gte_year=gte_year, lte_year=lte_year)
    # errors are thrown during the fetching process
    # there shouldn't be apoint where csvout contains partial data
    csvout = DictWriter(stdout, fieldnames=csvin.fieldnames)
    csvout.writeheader()
    for row in sorted(csvin, key=lambda r: (r['CaseID'])):
        csvout.writerow(row)


