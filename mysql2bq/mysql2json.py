#!/usr/bin/env python

import MySQLdb
import click
from MySQLdb.converters import conversions
import MySQLdb.cursors
import ujson, sys

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

@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
def JsonExport(host, database, user, password, table):
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM %s" % (table))

    for row in cursor:
        sys.stdout.write(ujson.dumps(row) + "\n")

if __name__ == '__main__':
    ## run the command
    JsonExport()
