# -*- coding: utf-8 -*-
"""Stats script to generate the number of opened/closed issues by priority for every
day."""
import sys
sys.dont_write_bytecode = True

from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb

from github_stats import ProjectStats, Helper, CONFIG

TABLE_NAME = 'synapse_cfd_columns'
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
                                        'orgs/matrix-org/projects/2',
                                        archived_state={'Done - Operations': 'all',
                                                        'Done - Planned Project': 'all'})

aggregated_counts = {
    'Done': counts['Done - Operations'] + counts['Done - Planned Project'],
    'To Do': counts['To Do'] + counts['To Do S2S r0'],
    'In Progress': counts['In Progress: Planned Project Work'] + counts['In Progress: Operational/bug fixes'],
}

# Persist stats
cursor = db.cursor()
today = date.today()
clear_today = """
DELETE FROM synapse_cfd_columns
WHERE `date` = %s
"""
cursor.execute(clear_today, (today, ))

record_counts = """
INSERT INTO synapse_cfd_columns
(date, col, cardcount)
VALUE (%s, %s, %s)
"""
for column, count in aggregated_counts.items():
    cursor.execute(record_counts, (today, column, count))
db.commit()
