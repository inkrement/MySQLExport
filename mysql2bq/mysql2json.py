#!/usr/bin/env python

import MySQLdb
import click
from MySQLdb.converters import conversions
import MySQLdb.cursors
import json, gzip

def conv_date_to_timestamp(str_date):
    import time
    import datetime

    date_time = MySQLdb.times.DateTime_or_None(str_date)
    unix_timestamp = (date_time - datetime.datetime(1970,1,1)).total_seconds()

    return unix_timestamp

def Connect(host, database, user, password):
    ## fix conversion. datetime as str and not datetime object
    conv=conversions.copy()
    conv[12]=conv_date_to_timestamp
    return MySQLdb.connect(host=host, db=database, user=user, passwd=password,
        conv=conv, cursorclass=MySQLdb.cursors.SSDictCursor, charset='utf8', use_unicode=True)


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
def JsonExport(host, database, user, password, table, split, compress, prefix):
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
    for row in cursor:
        if count % split == 0:
            if dest: dest.close()
            if compress:
                dest = gzip.open(genFilename(prefix, at, 'json.gz'), 'wt')
            else:
                dest = open(genFilename(prefix, at, 'json'), 'w')
            at += 1
        dest.write(json.dumps(row) + "\n")
        count += 1
    if dest: dest.close()

if __name__ == '__main__':
    ## run the command
    JsonExport()
