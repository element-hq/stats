# -*- coding: utf-8 -*-
"""Stats script for tracking issue categories over time."""
from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import GithubStats, Helper, CONFIG

TABLE_NAME = 'riotweb_pr_totals'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    state VARCHAR(11) NOT NULL,
    repo VARCHAR(255) NOT NULL,
    contributor VARCHAR(11) NOT NULL,
    prcount INT NOT NULL
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

PAID_TEAM = ['bwindels',
             'ara4n',
             'dbkr',
             'lampholder',
             't3chguy',
             'Kegsay',
             'erikjohnston',
             'NegativeMjark',
             'richvdh',
             'lukebarnard1',
             'uhoreg',
             'benparsons',
             'Half-Shot',
             'rxl881',
             'illicitonion',
             'superdump',
             'RiotTranslateBot',
             'anoadragon453',
             'RiotTranslate',
             'manuroe',
             'leonerd',
             'wmwragg',
             'aviraldg',
             'turt2live']

for issue in stats.query(query='repo:vector-im/riot-web repo:matrix-org/matrix-react-sdk repo:matrix-org/matrix-js-sdk is:pr'):
    # repo = '%s/%s' % (issue.repository.organization.name, issue.repository.name)
    contributor = 'paid' if issue.user.login in PAID_TEAM else 'community'
    if contributor == 'community':
        print(issue.user.login)

    opened = issue.created_at.date()
    closed = issue.closed_at.date() if issue.closed_at is not None else today + timedelta(days=1)
    for date in [opened + timedelta(days=n)
                 for n in range((closed - opened).days)]:
        open_issues[date][contributor] += 1
    for date in [closed + timedelta(days=n)
                 for n in range((today - closed).days + 1)]:
        closed_issues[date][contributor] += 1

cursor = db.cursor()
delete_entries = """
DELETE FROM %s
""" % TABLE_NAME
cursor.execute(delete_entries)

record_pr_totals = """
INSERT INTO riotweb_pr_totals
(date, state, contributor, prcount)
VALUES (%s, %s, %s, %s)
""" 

dates = list(open_issues.keys()) + list(closed_issues.keys())
start_date = min(dates)
end_date = max(dates)

for date in [start_date + timedelta(days=n)
             for n in range((end_date - start_date).days + 1)]:
    for contributor in ('paid', 'community'):
        cursor.execute(record_pr_totals, (date, 'OPEN', contributor, open_issues[date][contributor]))
        cursor.execute(record_pr_totals, (date, 'CLOSED', contributor, closed_issues[date][contributor]))

db.commit()
