import json
import requests
import config as cfg
import pandas as pd
from time import sleep
from collections import defaultdict, Counter


class TaskHandler:
    def __init__(self, part_id, project_pool_ids, site='main'):
        self.part_id = part_id
        self.project_pool_ids = project_pool_ids
        self.site = site
        self.host = cfg.host[site]
        with open(f'oauth_token_{site}', 'r') as token_file:
            self.oauth_token = token_file.read()
        self.headers = {
            "Authorization": f'OAuth {self.oauth_token}',
            "Content-Type": "application/JSON",
        }

    def send_tasks(self, tasks):
        resp = requests.post(
            f'{self.host}/api/v1/tasks?allow_defaults=true',
            headers=self.headers,
            json=tasks).json()
        return resp

    def load_stage1_tasks(self):
        stage1_images = pd.read_csv(f'part_{self.part_id}/images.tsv', sep='\t', quoting=3)
        tasks = []
        for i, row in stage1_images.iterrows():
            task = {
                'pool_id': self.project_pool_ids['project1'],
                'input_values': {
                    'image': row['INPUT:image']
                }
            }
            tasks.append(task)
        self.stage1_tasks = self.send_tasks(tasks)['items']

    def load_stage1_controls(self):
        stage1_control = pd.read_csv('stage1_control.tsv', sep='\t', quoting=3)
        tasks = []
        for i, row in stage1_control.iterrows():
            task = {
                'pool_id': self.project_pool_ids['project1'],
                'input_values': {
                    'image': row['INPUT:image']
                },
                'known_solutions': [
                    {
                        'output_values': {
                            'result': row['GOLDEN:result']
                        },
                        'correctness_weight': 1
                    }
                ],
                'infinite_overlap': True
            }
            tasks.append(task)
        self.stage1_controls = self.send_tasks(tasks)['items']

    def get_stage1_results(self):
        resp = requests.get(
            f'{self.host}/api/v1/assignments?pool_id={self.project_pool_ids["project1"]}',
            headers=self.headers).json()
        task_results = defaultdict(list)
        for item in resp['items']:
            task_ids = [task['id'] for task in item['tasks']]
            results = [solution['output_values']['result'] for solution in item['solutions']]
            for task_id, result in zip(task_ids, results):
                task_results[task_id].append(result)
        self.stage1_results = {}
        for task_id, results in task_results.items():
            self.stage1_results[task_id] = Counter(results).most_common(1)[0][0]

    def load_stage2_tasks(self):
        presents_ids = set()
        for task_id, result in self.stage1_results.items():
            if result == 'PRESENT':
                presents_ids.add(task_id)
        tasks = []
        for item in self.stage1_tasks.values():
            if item['id'] in presents_ids:
                task = {
                    'pool_id': self.project_pool_ids['project2'],
                    'input_values': item['input_values']
                }
                tasks.append(task)
        self.stage2_tasks = self.send_tasks(tasks)['items']

    def get_stage2_results(self):
        resp = requests.get(
            f'{self.host}/api/v1/assignments?pool_id={self.project_pool_ids["project2"]}&status=SUBMITTED',
            headers=self.headers).json()
        task_results = []
        for item in resp['items']:
            assignment_id = item['id']
            image = item['tasks'][0]['input_values']['image']
            result = item['solutions'][0]['output_values']['result']
            task_results.append({
                'assignment_id': assignment_id,
                'image': image,
                'result': result
            })
        self.stage2_results = task_results

    def load_stage3_tasks(self):
        tasks = []
        for item in self.stage2_results:
            task = {
                'pool_id': self.project_pool_ids['project3'],
                'input_values': {
                    'assignment_id': item['assignment_id'],
                    'image': item['image'],
                    'selection': item['result']
                }
            }
            tasks.append(task)
        self.stage3_tasks = self.send_tasks(tasks)['items']

    def load_stage3_controls(self):
        stage3_control = pd.read_csv('stage3_control.tsv', sep='\t')
        tasks = []
        for i, row in stage3_control.iterrows():
            task = {
                'pool_id': self.project_pool_ids['project3'],
                'input_values': {
                    'image': row['INPUT:image'],
                    'selection': json.loads(row['INPUT:selection']),
                    'assignment_id': row['INPUT:assignment_id']
                },
                'known_solutions': [
                    {
                        'output_values': {
                            'result': 'TRUE' if row['GOLDEN:result'] else 'FALSE'
                        },
                        'correctness_weight': 1
                    }
                ],
                'infinite_overlap': True
            }
            tasks.append(task)
        self.stage3_controls = self.send_tasks(tasks)

    def get_stage3_results(self):
        resp = requests.post(
            f'{self.host}/api/v1/aggregated-solutions/aggregate-by-pool',
            headers=self.headers,
            json={
                'pool_id': self.project_pool_ids['project3'],
                "type": "WEIGHTED_DYNAMIC_OVERLAP",
                "answer_weight_skill_id": cfg.skill_id_project3[self.site],
                "fields": [
                    {
                        "name": "result"
                    }
                ]
            }).json()
        op_id = resp['id']
        while True:
            sleep(5)
            resp = requests.get(
                f'{self.host}/api/v1/operations/{op_id}',
                headers=self.headers).json()
            if resp['status'] == 'SUCCESS':
                break
        resp = requests.get(
            f'{self.host}/api/v1/aggregated-solutions/{op_id}',
            headers=self.headers).json()
        task_results = {}
        overall = len(self.stage3_tasks)
        accepted = 0
        for item in resp['items']:
            task_results[item['task_id']] = item['output_values']['result']
            if item['output_values']['result'] == 'TRUE':
                accepted += 1
        self.stage3_results = task_results
        return (accepted, overall)

    def load_validation_results(self):
        for item in self.stage3_tasks.values():
            task_id = item['id']
            assignment_id = item['input_values']['assignment_id']
            if self.stage3_results[task_id] == 'TRUE':
                assignment_result = {
                    'status': 'ACCEPTED',
                    'public_comment': ''
                }
            else:
                assignment_result = {
                    'status': 'REJECTED',
                    'public_comment': 'Объект не выделен или выделен неверно'
                }
            resp = requests.patch(
                f'{self.host}/api/v1/assignments/{assignment_id}',
                headers=self.headers,
                json=assignment_result).json()

    def get_final_results(self):
        results = {
            'image': [],
            'selection': []
        }
        resp = requests.get(
            f'{self.host}/api/v1/assignments?pool_id={self.project_pool_ids["project2"]}&status=ACCEPTED',
            headers=self.headers).json()
        for item in resp['items']:
            image = item['tasks'][0]['input_values']['image']
            selection = item['solutions'][0]['output_values']['result']
            results['image'].append(image)
            results['selection'].append(selection)
        results_df = pd.DataFrame.from_dict(results)
        results_df.to_csv(f'part_{self.part_id}/selection.tsv', sep='\t', index=False)
