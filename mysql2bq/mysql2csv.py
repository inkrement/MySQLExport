#!/usr/bin/env python

import MySQLdb
import click
from MySQLdb.converters import conversions
import MySQLdb.cursors
import csv, sys

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
def CSVExport(host, database, user, password, table):
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM %s" % (table))

    csvwriter = csv.writer(sys.stdout, quoting=csv.QUOTE_NONNUMERIC)

    for row in cursor:
        _= csvwriter.writerow(row)


if __name__ == '__main__':
    ## run the command
    CSVExport()
