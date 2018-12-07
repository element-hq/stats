# -*- coding: utf-8 -*-
"""Helper functions for pulling issue stats from Github and writing them to a db
to be scraped by grafana."""

import re
import time
from collections import defaultdict
from datetime import date, timedelta, datetime

import requests
from requests.auth import HTTPBasicAuth

import MySQLdb
from github import Github
from github.Label import Label

import yaml
from os.path import expanduser

class Config:

    def __init__(self):
        with open(expanduser('~') + '/.githubstats', 'r') as config_file:
            config = yaml.safe_load(config_file)
            self.GITHUB_TOKEN = config['github_token']
            self.MYSQL_PASSWORD = config['mysql_password']

CONFIG = Config()


class GithubStats:

    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, github_token=CONFIG.GITHUB_TOKEN):
        self.github = Github(github_token)
   
    def small_query(self, query):
        for issue in self.github.search_issues(query):
            yield issue
            time.sleep(2.0 / 30)
 
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


class ProjectStats:

    @staticmethod
    def _extractNextLinkFromHeader(link_header):
        """Pull the URL for the next page of resources out of the supplied 'Link:'
        header"""
        if link_header is None:
            return None
    
        next_links = [link for link in link_header.split(',')
                      if link[-10:] == 'rel="next"']
    
        if len(next_links) == 1:
            return re.split('<|>', next_links[0])[1]
    
        return None
    
    @staticmethod 
    def pagination_processor(github_url, headers=None, auth=None, initial_params={}, fetcher=None):
            """A github response is:
                 - some JSON
                 - maybe another link to follow to get more of that response"""
            github_response = requests.get(github_url,
                                           headers=headers,
                                           params=initial_params,
                                           auth=auth)
    
            while True:
                response_json = github_response.json()
                for json_entity in response_json:
                    yield json_entity
    
                next_url = ProjectStats._extractNextLinkFromHeader(
                    github_response.headers.get('Link',
                                                None)
                )
    
                if next_url is None:
                    break
    
                github_response = requests.get(next_url,
                                               headers=headers,
                                               auth=auth)
   
    @staticmethod
    def get_project_number_from_project_string(github_token, project_string):
        """A merry dance to go from the repo and project number that we know to the columns and their
        counts via some totally opaque internal github ids.
        Just supply a github-url style string, such as:
            orgs/matrix-org/projects/2
            repos/vector-im/riot-web/projects/11
        and it'll return an id you can use with the /projects/<id>/columns api
        """
        if project_string.startswith('orgs'):
            # We're an org-level project, e.g. orgs/matrix-org/projects/2
            (_, org, _, project_number) = project_string.split('/')
            list_projects = 'https://api.github.com/orgs/%s/projects' % org
        else:
            # We're a repo-level project, e.g. vector-im/riot-web/projects/11
            (org, repo, _, project_number) = project_string.split('/')
            list_projects = 'https://api.github.com/repos/%s/%s/projects' % (org, repo)

        headers = {'Accept': 'application/vnd.github.inertia-preview+json'}
        auth = HTTPBasicAuth('lampholder',
                             github_token)
       
        projects = [project
                    for project in requests.get(list_projects, headers=headers, auth=auth).json()
                    if project['number'] == int(project_number)] + [None]
        project = projects[0]

        return project
 
    @staticmethod 
    def get_column_counts(github_token, project_string, archived_state={}):
        project = ProjectStats.get_project_number_from_project_string(github_token, project_string) 
        get_project = 'https://api.github.com/projects/%d/columns'
       
        headers = {'Accept': 'application/vnd.github.inertia-preview+json'}
        auth = HTTPBasicAuth('lampholder',
                             github_token)

        counts = {column['name']: len(list(ProjectStats.pagination_processor(column['cards_url'],
                                                                             initial_params={'archived_state': archived_state.get(column['name'], 'not_archived')},
                                                                             headers=headers,
                                                                             auth=auth)))
                  for column in requests.get(get_project % project['id'], headers=headers, auth=auth).json()}
        return counts


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
