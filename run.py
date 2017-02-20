#!/usr/bin/env python

import MySQLdb
import click
import MySQLdb.cursors
import json

def Connect(host, database, user, password):
    return MySQLdb.connect(host=host, db=database, user=user, passwd=password,
         cursorclass=MySQLdb.cursors.SSDictCursor, charset='utf8', use_unicode=True)

@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
@click.argument('filename')
def MySQLexport(host, database, user, password, table, filename):
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM %s" % (table))

    cur_batch = []
    count = 0
    with open(filename, "w") as writer:
        for row in cursor:
            writer.write(json.dumps(row) + "\n")

if __name__ == '__main__':
    ## run the command
    MySQLexport()
