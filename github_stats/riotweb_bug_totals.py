# -*- coding: utf-8 -*-
"""Stats script to generate the number of opened/closed issues by priority for every
day."""
import sys
sys.dont_write_bytecode = True

import argparse
from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import GithubStats, Helper

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--mysql-password', required=True)
parser.add_argument('--github-token', required=True)
args = parser.parse_args()

TABLE_NAME = 'riotweb_bug_totals'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    state VARCHAR(11) NOT NULL,
    priority INT,
    cumulative_total INT NOT NULL
);
""" % TABLE_NAME

# Connect to and setup db:
db = MySQLdb.connect(host='localhost',
                     user='businessmetrics',
                     passwd=args.mysql_password,
                     db='businessmetrics',
                     port=3306)
Helper.create_table(db, SCHEMA)

stats = GithubStats(args.github_token)


# Process results from github:
today = date.today()
opened_bugs = defaultdict(lambda: defaultdict(int))
closed_bugs = defaultdict(lambda: defaultdict(int))

for issue in stats.query(date_field='created', query='repo:vector-im/riot-web is:issue label:bug'):
    opened = issue.created_at.date()
    closed = issue.closed_at.date() if issue.closed_at is not None else today + timedelta(days=1)

    for date in [opened + timedelta(days=n)
                 for n in range((closed - opened).days)]:
        opened_bugs[date][Helper.get_priority(issue)] += 1
    for date in [closed + timedelta(days=n)
                 for n in range((today - closed).days + 1)]:
        closed_bugs[date][Helper.get_priority(issue)] += 1

# Persist to db:
cursor = db.cursor()
delete_entries = """
DELETE FROM %s
""" % TABLE_NAME
cursor.execute(delete_entries)
db.commit()

record_bug_totals = ("""
INSERT INTO %s
(date, state, priority, cumulative_total) """ % TABLE_NAME) + "VALUES (%s, %s, %s, %s)"

dates = list(opened_bugs.keys()) + list(closed_bugs.keys())
start_date = min(dates)
end_date = max(dates)

for date in [start_date + timedelta(days=n)
             for n in range((end_date - start_date).days + 1)]:
    for priority in (None, 1, 2, 3, 4, 5):
        cursor.execute(record_bug_totals, (date, 'OPEN', priority, opened_bugs[date][priority]))
        cursor.execute(record_bug_totals, (date, 'CLOSED', priority, closed_bugs[date][priority]))
        db.commit()
