import argparse
from csv import DictReader, DictWriter
from datetime import datetime
from loggy import loggy
from re import compile, search
from sys import stdout

LOGGY = loggy('clean')

WS_RX = compile(r'\s+')


RAW_HEADERS = ["CaseID","Opened","Closed","Updated","Status","Status Notes",
"Responsible Agency","Category","Request Type","Request Details",
"Address","Supervisor District","Neighborhood","Point", "Source",
"Media URL"]

CLEAN_HEADERS = ['case_id', 'opened_datetime', 'closed_datetime',
    'updated_datetime', 'status', 'status_notes',
    'responsible_agency', 'category', 'request_type', 'request_details',
    'address', 'supervisor_district', 'neighborhood', 'source',
    'media_url', 'latitude', 'longitude']


def clean_row(row):
    """
    returns a new dict
    """
    x = {}
    # fill in the boilerplate
    x['case_id'] = row['CaseID']
    x['status'] = row['Status']
    x['status_notes'] = WS_RX.sub(' ', row['Status Notes']).strip()
    x['responsible_agency'] = WS_RX.sub(' ', row["Responsible Agency"]).strip()
    x['category'] = row['Category']
    x['request_type'] = WS_RX.sub(' ', row['Request Type']).strip()
    x['request_details'] = WS_RX.sub(' ', row['Request Details']).strip()
    x['address'] = WS_RX.sub(' ', row['Address'])
    x['supervisor_district'] = row['Supervisor District']
    x['neighborhood'] = row['Neighborhood']
    x['source'] = row['Source']
    x['media_url'] = row['Media URL']

    # now fill in the derived values
    # some Occurrence Date values are blank
    timefields = [('opened_datetime', 'Opened'),
                    ('closed_datetime', 'Closed'),
                    ('updated_datetime', 'Updated')]

    for cx, bx in timefields:
        try:
            dt = datetime.strptime(row[bx], '%m/%d/%Y %I:%M:%S %p')
            x[cx] = dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as err:
            x[cx] = None

    # Now try to split the Location 1 value into latitude/longitude
    _mtch = search(r'\((.+?), (.+?)\)', row['Point'])
    if _mtch:
        # don't need all the degrees of precision
        x['latitude'], x['longitude'] = [round(float(z), 6) for z in _mtch.groups()]
    else:
        x['latitude'] = x['longitude'] = None
    return x


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Clean data")
    parser.add_argument('infile', type=argparse.FileType('r'))
    args = parser.parse_args()
    infile = args.infile
    LOGGY.info("Reading %s" % infile.name)
    csvin = DictReader(infile, fieldnames=RAW_HEADERS)
    csvout = DictWriter(stdout, fieldnames=CLEAN_HEADERS)
    csvout.writeheader()

    for row in csvin:
        if 'CaseID' in row.values():
            # ignore headers
            pass
        else:
            csvout.writerow(clean_row(row))

