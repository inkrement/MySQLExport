#!/usr/bin/env python

import MySQLdb
import os
import click
import MySQLdb.cursors
import json

bqTypeDict = { 'int' : 'INTEGER',
               'varchar' : 'STRING',
               'double' : 'FLOAT',
               'tinyint' : 'INTEGER',
               'decimal' : 'FLOAT',
               'text' : 'STRING',
               'smallint' : 'INTEGER',
               'char' : 'STRING',
               'bigint' : 'INTEGER',
               'float' : 'FLOAT',
               'longtext' : 'STRING',
               'datetime' : 'TIMESTAMP'
              }


def Connect(host, database, user, password):
    return MySQLdb.connect(host=host, db=database, user=user, passwd=password,
        cursorclass=MySQLdb.cursors.SSCursor, charset='utf8', use_unicode=True)

@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
@click.argument('filename')
def BuildSchema(host, database, user, password, table, filename):
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()
    cursor.execute("DESCRIBE %s;" % table)

    tableDecorator = cursor.fetchall()
    schema = []

    for col in tableDecorator:
        colType = col[1].split("(")[0]
        if colType not in bqTypeDict:
            print("Unknown type detected, using string: %s", str(col[1]))

        field_mode = "NULLABLE" if col[2] == "YES" else "REQUIRED"
        field = {"name": col[0], "type":bqTypeDict.get(colType, "STRING"), "mode": field_mode}

        schema.append(field)

    with open(filename, 'w') as fp:
        json.dump(schema, fp, indent=4)



if __name__ == '__main__':
    ## run the command
    BuildSchema()
