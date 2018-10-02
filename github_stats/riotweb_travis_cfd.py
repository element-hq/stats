# -*- coding: utf-8 -*-
"""Stats script to generate the number of opened/closed issues by priority for every
day FOR TRAVIS/FIRST IMPRESSION project."""
import sys
sys.dont_write_bytecode = True

from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import ProjectStats, Helper, CONFIG

TABLE_NAME = 'riotweb_first_impressions_cfd_columns'
SCHEMA = """
CREATE TABLE IF NOT EXISTS
%s (
    date DATE NOT NULL,
    col VARCHAR(255) NOT NULL,
    cardcount INT NOT NULL
);
""" % TABLE_NAME

# Connect to and setup db:
db = MySQLdb.connect(host='localhost',
                     user='businessmetrics',
                     passwd=CONFIG.MYSQL_PASSWORD,
                     db='businessmetrics',
                     port=3306)
Helper.create_table(db, SCHEMA)

# Pull stats
counts = ProjectStats.get_column_counts(CONFIG.GITHUB_TOKEN,
                                        'vector-im/riot-web/projects/12',
                                        archived_state={'Done': 'all'})

# Persist stats
cursor = db.cursor()
today = date.today()
clear_today = """
DELETE FROM riotweb_first_impressions_cfd_columns
WHERE `date` = %s
"""
cursor.execute(clear_today, (today, ))

record_counts = """
INSERT INTO riotweb_first_impressions_cfd_columns
(date, col, cardcount)
VALUE (%s, %s, %s)
"""
for column, count in counts.items():
    cursor.execute(record_counts, (today, column, count))
db.commit()
