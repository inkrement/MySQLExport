#!/usr/bin/env python

import MySQLdb
import click
from MySQLdb.converters import conversions
import MySQLdb.cursors
import gzip, csv

def conv_date_to_timestamp(str_date):
    import time
    import datetime

    date_time = MySQLdb.times.DateTime_or_None(str_date)
    #unix_timestamp = int((date_time - datetime.datetime(1970,1,1)).total_seconds())

    #return unix_timestamp
    return int(time.mktime(date_time.timetuple()))

def Connect(host, database, user, password):
    ## fix conversion. datetime as str and not datetime object
    conv=conversions.copy()
    conv[12]=conv_date_to_timestamp
    return MySQLdb.connect(host=host, db=database, user=user, passwd=password,
        conv=conv, cursorclass=MySQLdb.cursors.SSCursor, charset='utf8', use_unicode=True)


def genFilename(prefix, num, ext):
    return '%s%d.%s' % (prefix, num, ext)

@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
@click.option('-s', '--split', default=4000000, help='split output')
@click.option('-c', '--compress', is_flag=True, default=False, help='compress output')
@click.argument('prefix')
def CSVExport(host, database, user, password, table, split, compress, prefix):
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM %s" % (table))

    ''' without splitting file:
    with open(filename, "w") as writer:
        for row in cursor:
            writer.write(json.dumps(row) + "\n")
    '''

    count = 0
    at = 0
    dest = None
    batchsize = 1000
    batch = []

    for row in cursor:
        if count % split == 0:
            if compress:
                dest = csv.writer(gzip.open(genFilename(prefix, at, 'csv.gz'), 'wt'), quoting=csv.QUOTE_NONNUMERIC)
            else:
                #todo
                dest = csv.writer(open(genFilename(prefix, at, 'csv'), 'wt'))
            at += 1

            ## write out
            _= dest.writerows(batch)
            batch = []

        # write out
        if count % batchsize == 0:
            _= dest.writerows(batch)
            batch = []

        # append
        count += 1
        batch.append(row)

if __name__ == '__main__':
    ## run the command
    CSVExport()
