import config as cfg
import requests
from utils import json_read, json_dump
from datetime import datetime, timedelta


class PoolHandler:
    def __init__(self, part_id, site='main'):
        self.part_id = part_id
        self.site = site
        self.host = cfg.host[site]
        with open(f'oauth_token_{site}', 'r') as token_file:
            self.oauth_token = token_file.read()
        self.project_pool_settings = {
            'project1': json_read(f'project1_pool_settings_{site}.json'),
            'project2': json_read(f'project2_pool_settings_{site}.json'),
            'project3': json_read(f'project3_pool_settings_{site}.json'),
        }
        self.project_pool_ids = {
            'project1': None,
            'project2': None,
            'project3': None,
        }
        self.headers = {
            "Authorization": f'OAuth {self.oauth_token}',
            "Content-Type": "application/JSON",
        }

    def create_pool(self, project):
        pool_settings = self.project_pool_settings[project].copy()
        pool_settings['private_name'] = f'Pool {self.part_id}'
        pool_settings['will_expire'] = (datetime.utcnow() + timedelta(days=365)).isoformat()
        resp = requests.post(
            f'{self.host}/api/v1/pools/',
            headers=self.headers,
            json=pool_settings).json()
        self.project_pool_ids[project] = resp['id']

    def open_pool(self, project):
        resp = requests.post(
            f'{self.host}/api/v1/pools/{self.project_pool_ids[project]}/open',
            headers=self.headers).json()
        return resp['status'] != 'FAIL'

    def is_closed(self, project):
        resp = requests.get(
            f'{self.host}/api/v1/pools/{self.project_pool_ids[project]}',
            headers=self.headers).json()
        return resp['status'] == 'CLOSED'

    def is_accepted(self, project):
        resp = requests.get(
            f'{self.host}/api/v1/assignments?pool_id={self.project_pool_ids[project]}&status=REJECTED',
            headers=self.headers).json()
        return len(resp['items']) == 0
