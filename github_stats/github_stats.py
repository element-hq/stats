# -*- coding: utf-8 -*-
"""Helper functions for pulling issue stats from Github and writing them to a db
to be scraped by grafana."""

import re
import time
from collections import defaultdict
from datetime import date, timedelta, datetime

import MySQLdb
from github import Github
from github.Label import Label

class GithubStats:

    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, github_token):
        self.github = Github(github_token)
    
    def query(self, query, date_field='created', start='2015-01-01', end=None):
        start_date = datetime.strptime(start, self.DATE_FORMAT).date()
        end_date = datetime.strptime(end, self.DATE_FORMAT).date() if end is not None else date.today()

        window_start = start_date
        while window_start <= end_date:
            window_end = window_start + timedelta(days=30)

            time.sleep(2)
            for issue in self.github.search_issues('%s:%s..%s ' % (date_field,
                                                                   window_start.strftime(self.DATE_FORMAT),
                                                                   window_end.strftime(self.DATE_FORMAT))
                                                   + query):
                yield issue
                time.sleep(2.0 / 30)
            
            # github date search is inclusive start and end
            window_start = window_end + timedelta(days=1)


class Helper:
    """Misc helper methods"""

    @staticmethod
    def get_priority(issue):
        priority = ([int(label.name[1]) for label in sorted(issue.labels, key=lambda x: x.name)
                     if re.match('p[1-5]', label.name)] + [None])[0]
        return priority

    @staticmethod 
    def create_table(db, schema):
        """This method executes a CREATE TABLE IF NOT EXISTS command
        _without_ generating a mysql warning if the table already exists."""
        cursor = db.cursor()  
        cursor.execute('SET sql_notes = 0;')
        cursor.execute(schema)
        cursor.execute('SET sql_notes = 1;')
        db.commit()
