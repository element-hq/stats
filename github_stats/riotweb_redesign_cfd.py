# -*- coding: utf-8 -*-
"""Stats script to generate the number of opened/closed issues by priority for every
day FOR TRAVIS/FIRST IMPRESSION project."""
import sys
sys.dont_write_bytecode = True

from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import GithubStats, ProjectStats, Helper, CONFIG

TABLE_NAME = 'riotweb_redesign_issue_totals'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    state VARCHAR(11) NOT NULL,
    issuecount INT NOT NULL
);
""" % TABLE_NAME

# Connect to and setup db:
db = MySQLdb.connect(host='localhost',
                     user='businessmetrics',
                     passwd=CONFIG.MYSQL_PASSWORD,
                     db='businessmetrics',
                     port=3306)
Helper.create_table(db, SCHEMA)

stats = GithubStats()

today = date.today()
open_issues = defaultdict(lambda: defaultdict(int))
closed_issues = defaultdict(lambda: defaultdict(int))

for issue in stats.query(date_field='created', query='repo:vector-im/riot-web is:issue label:redesign'):
    label_names = [label.name for label in issue.labels]
        
    opened = issue.created_at.date()
    closed = issue.closed_at.date() if issue.closed_at is not None else today + timedelta(days=1)
    issue_type = 'placeholder'
    for date in [opened + timedelta(days=n)
                 for n in range((closed - opened).days)]:
        open_issues[date][issue_type] += 1
    for date in [closed + timedelta(days=n)
                 for n in range((today - closed).days + 1)]:
        closed_issues[date][issue_type] += 1



# Pull stats
counts = ProjectStats.get_column_counts(CONFIG.GITHUB_TOKEN,
                                        'vector-im/riot-web/projects/12',
                                        archived_state={'Done': 'all'})

# Persist stats
cursor = db.cursor()
today = date.today()
clear_today = """
DELETE FROM riotweb_redesign_issue_totals
WHERE `date` = %s
"""
cursor.execute(clear_today, (today, ))

record_counts = """
INSERT INTO riotweb_redesign_issue_totals
(date, state, issuecount)
VALUE (%s, %s, %s)
"""
for column, count in counts.items():
    cursor.execute(record_counts, (today, column, count))
db.commit()
