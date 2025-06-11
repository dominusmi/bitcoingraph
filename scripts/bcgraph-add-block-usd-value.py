#!/usr/bin/env python

import argparse
import csv
from neo4j import GraphDatabase
from datetime import datetime

def add_values(driver,first,last,file):

    print("Starting...")
    with open(file) as f:
        reader = csv.DictReader(f)
        dv = list(reader)


    if not any(d['Date'] == first for d in dv):
        raise SystemExit("Your start date is not in the CSV file")
    if not any(d['Date'] == last for d in dv):
        raise SystemExit("Your end date is not in the CSV file")

    ff = datetime.strptime(first, "%Y-%m-%d")
    fl = datetime.strptime(last, "%Y-%m-%d")
    if ff > fl:
        raise SystemExit("Your start date is later than your end date")

    for d in dv:
        date = d['Date']
        valueUSD = float(d['Average'])
        ustart = int(d['unixstart'])
        uend = int(d['unixend'])
        if ff <= datetime.strptime(date, "%Y-%m-%d") <= fl:
            print("Adding value to " + date + " blocks")
            with driver.session() as session:
                result = session.run("""
                    MATCH (b:Block)
                    WHERE b.timestamp >= $ustart AND b.timestamp <= $uend
                    WITH b
                    SET b.valueUSD = $valueUSD
                    RETURN b.height, b.valueUSD
                """, ustart=ustart, uend=uend, valueUSD=valueUSD).values()
                if len(result) > 0:
                    r = result
                    print(f"Added value {r[0][1]} to blocks {r[0][0]} -> {r[-1][0]}")

parser = argparse.ArgumentParser(
    description="Use data from a CSV file in the format 'Date,Average_value,Date_start_unixtime,Date_end_unixtime' " 
                "(in UTC) to add a USD value to the transactions in a range of blocks. "
                "The CSV header expected is 'Date,Average,unixstart,unixend'. A CSV going from 2010 to mid 2023 is "
                "provided in the assets folder of the repository. "
                "Edit 'valueUSD' in this script to (also) add a value in another currency. "
)
parser.add_argument('--host', default="0.0.0.0", help='Neo4j host')
parser.add_argument('--port', default=7687, help='Neo4j port', type=int)
parser.add_argument('-u', '--username', required=True, help='Neo4j user')
parser.add_argument('-p', '--password', required=True, help='Neo4j password')
parser.add_argument('-F', '--first', required=True, help='First date to process, YYYY-MM-DD')
parser.add_argument('-L', '--last', required=True, help='Last date to process, YYYY-MM-DD')
parser.add_argument('-f', '--file', required=True, help='CSV file to parse')

def main():
    if __name__ == "__main__":
        args = parser.parse_args()
        driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}",
                                  auth=(args.username, args.password),
                                  connection_timeout=3600)
        if args.last < args.first:
            raise SystemExit('The value of --last cannot be lower than that of --first.')
        add_values(driver, args.first, args.last, args.file)

if __name__ == "__main__":
    main()

