#!/usr/bin/python
#
# Generate CSV columns and types from a schema
#
# Allows converting Hive or Postgres SQL schema into a CSV of
# column,type fields for use with GD to import data
#
# USAGE: gen-columns.py [-o OUTPUT] SCHEMA
#

from __future__ import print_function

# Standard python modules
import argparse
import re
import logging
import sys


# Defaults

# Decode types from this table 
TABLE_NAME = 'billing_events'


# Constants
# Map some types to simpler form
TYPES_MAP = {
    # PG
    'timestamp without time zone' : 'timestamp'
}


def csv_esc(s):
    """ Escape a CSV string """
    if ',' in s:
        return '"' + s.replace('"', '""') + '"'
    else:
        return s

def generate_csv_types(schema_file, table_name, output_file):
    """ Generate CSV types from the schema file with table @table_name """

    # Output columns
    columns = []

    LINE_SPLIT_RE = re.compile(r'^\s*(\w+)\s*(.+)')
    COMMENT_RE = re.compile(r'\s*--.*$')

    seen_ct = False
    try:
        with open(schema_file) as f:
            for line in f:
                line = line.replace('\n', '').strip()

                if line.startswith('CREATE TABLE `' + table_name + '`'):
                    seen_ct=True
                    continue
                if not seen_ct:
                    continue
                if line.startswith(')'):
                    break

                line = COMMENT_RE.sub('', line)
                if line.endswith(','):
                    line = line[:-1]

                m = LINE_SPLIT_RE.match(line)
                if m is None:
                    raise Exception("Failed to match line '{line}'".format(line=line))

                (name, typ)= m.groups()
                typ = typ.lower()

                mapped_typ = TYPES_MAP.get(typ, None)
                if mapped_typ is None:
                    mapped_typ = typ

                columns.append((name, mapped_typ))
    except IOError, e:
        logging.error("Failed to open {file} - {e}".format(file=schema_file, e=str(e)))
        return 1

    of = sys.stdout
    if output_file is not None:
        try:
            of = open(output_file, 'w')
        except IOError, e:
            logging.error("Failed to write to {file} - {e}".format(file=output_file, e=str(e)))
            return 1

    of.write("column,type\n")
    for (name, typ) in columns:
        of.write("{name},{typ}\n".format(name=csv_esc(name), typ=csv_esc(typ)))

    if output_file is not None:
        of.close()

    return 0


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='Generate CSV columns and types from a schema')
    parser.add_argument('schema',
                        metavar='SCHEMA', nargs=1, help='SCHEMA filename')
    parser.add_argument('-d', '--debug',
                        action = 'store_true',
                        default = False,
                        help = 'debug messages (default: False)')
    parser.add_argument('-o', '--output',
                        default = None,
                        help = 'output file name')
    parser.add_argument('-t', '--table',
                        default = TABLE_NAME,
                        help = 'set table name (default: ' + TABLE_NAME + ')')
    parser.add_argument('-v', '--verbose',
                        action = 'store_true',
                        default = False,
                        help = 'verbose messages (default: False)')

    args = parser.parse_args()
    schema_file = args.schema[0]
    table_name = args.table
    output_file = args.output
    debug = args.debug
    verbose = args.verbose

    ######################################################################

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    rc = generate_csv_types(schema_file, table_name, output_file)
    sys.exit(rc)


if __name__ == '__main__':
    main()
