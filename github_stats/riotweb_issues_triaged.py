# -*- coding: utf-8 -*-
"""Stats script for tracking the number of untriaged issues"""
from datetime import date

import MySQLdb

from github_stats import GithubStats, Helper, CONFIG

TABLE_NAME = 'riotweb_untriaged_count'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    total INT NOT NULL
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

date_format = '%Y-%m-%d'
today = date.today()

# Thanks for only returning 1000 results in your paginated result set, github.
untriaged = stats.small_query(query='repo:vector-im/riot-web is:open is:issue no:label')
untriaged_count = len(list(untriaged))

# Persist to db:
cursor = db.cursor()
delete_entries = """
DELETE FROM riotweb_untriaged_count
WHERE `date` = %s
"""
cursor.execute(delete_entries, (today, ))

record_untriaged_issue_count = """
INSERT INTO riotweb_untriaged_count
(date, total)
VALUES (%s, %s)
""" 
cursor.execute(record_untriaged_issue_count, (today, untriaged_count))
db.commit()
