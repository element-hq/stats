# -*- coding: utf-8 -*-
"""Stats script for tracking issue categories over time."""
from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import GithubStats, Helper, CONFIG

TABLE_NAME = 'riotweb_issue_totals'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    state VARCHAR(11) NOT NULL,
    type VARCHAR(11) NOT NULL,
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

for issue in stats.query(date_field='created', query='repo:vector-im/riot-web is:issue'):
    label_names = [label.name for label in issue.labels]
    issue_type = ('feature' if 'feature' in label_names
                  else 'bug' if 'bug' in label_names
                  else 'maintenance' if 'maintenance' in label_names
                  else 'other')
        
    opened = issue.created_at.date()
    closed = issue.closed_at.date() if issue.closed_at is not None else today + timedelta(days=1)
    for date in [opened + timedelta(days=n)
                 for n in range((closed - opened).days)]:
        open_issues[date][issue_type] += 1
    for date in [closed + timedelta(days=n)
                 for n in range((today - closed).days + 1)]:
        closed_issues[date][issue_type] += 1

cursor = db.cursor()
delete_entries = """
DELETE FROM %s
""" % TABLE_NAME
cursor.execute(delete_entries)

record_issue_totals = """
INSERT INTO riotweb_issue_totals
(date, state, type, issuecount)
VALUES (%s, %s, %s, %s)
""" 

dates = list(open_issues.keys()) + list(closed_issues.keys())
start_date = min(dates)
end_date = max(dates)

for date in [start_date + timedelta(days=n)
             for n in range((end_date - start_date).days + 1)]:
    for issue_type in ('feature', 'bug', 'maintenance', 'other'):
        cursor.execute(record_issue_totals, (date, 'OPEN', issue_type, open_issues[date][issue_type]))
        cursor.execute(record_issue_totals, (date, 'CLOSED', issue_type, closed_issues[date][issue_type]))

db.commit()
