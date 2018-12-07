# -*- coding: utf-8 -*-
"""Stats script to generate the number of opened/closed issues by priority for every
day."""
import sys
sys.dont_write_bytecode = True

from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import GithubStats, Helper, CONFIG

TABLE_NAME = 'riotweb_pr_churn'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    state VARCHAR(11) NOT NULL,
    contributor VARCHAR(11) NOT NULL,
    prchurn INT NOT NULL
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

# Process results from github:
opened_prs = defaultdict(lambda: defaultdict(int))
closed_prs = defaultdict(lambda: defaultdict(int))

for pr in stats.query(query='repo:vector-im/riot-web repo:matrix-org/matrix-react-sdk repo:matrix-org/matrix-js-sdk is:pr'):
    contributor = 'paid' if pr.user.login in PAID_TEAM else 'community'
    opened_prs[pr.created_at.date()][contributor] += 1
    if pr.closed_at is not None:
        closed_prs[pr.closed_at.date()][contributor] += 1


# Persist to db:
cursor = db.cursor()
delete_entries = """
DELETE FROM %s
""" % TABLE_NAME
cursor.execute(delete_entries)

record_pr_churn = ("""
INSERT INTO %s
(date, state, contributor, prchurn) """ % TABLE_NAME) + "VALUES (%s, %s, %s, %s)"

dates = list(opened_prs.keys()) + list(closed_prs.keys())
start_date = min(dates)
end_date = max(dates)

for date in [start_date + timedelta(days=n)
             for n in range((end_date - start_date).days + 1)]:
    for contributor in ('paid', 'community'):
        cursor.execute(record_pr_churn, (date, 'OPEN', contributor, opened_prs[date][contributor]))
        cursor.execute(record_pr_churn, (date, 'CLOSED', contributor, closed_prs[date][contributor]))

db.commit()
