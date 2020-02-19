import argparse
from pool_handler import PoolHandler
from task_handler import TaskHandler
from time import sleep


parser = argparse.ArgumentParser()
parser.add_argument('part_id', type=int)


if __name__ == '__main__':
    args = parser.parse_args()
    part_id = args.part_id

    pool_handler = PoolHandler(part_id)
    task_handler = TaskHandler(part_id, pool_handler.project_pool_ids)

    # create pools

    print('Creating pool for project 1..')
    pool_handler.create_pool('project1')

    print('Creating pool for project 2..')
    pool_handler.create_pool('project2')

    print('Creating pool for project 3..')
    pool_handler.create_pool('project3')

    # load controls

    print('Loading control tasks for project 1..')
    task_handler.load_stage1_controls()

    print('Loading control tasks for project 3..')
    task_handler.load_stage3_controls()

    # load project 1

    print('Loading tasks to stage 1..')
    task_handler.load_stage1_tasks()

    print('Starting pool..')
    pool_handler.open_pool('project1')
    print('Pool has been started!')

    # wait for finishing

    print('Waiting for tasks to be done..')
    while True:
        sleep(30)
        if pool_handler.is_closed('project1'):
            break
    print('Stage 1 has been completed!')

    print('Getting results..')
    task_handler.get_stage1_results()

    # load project 2

    print('Loading tasks to stage 2..')
    task_handler.load_stage2_tasks()

    print('Starting pool..')
    pool_handler.open_pool('project2')
    print('Pool has been started!')

    # run validation phase

    while not pool_handler.is_accepted('project2'):

        # wait for finishing

        print('Waiting for tasks to be done..')
        while True:
            sleep(30)
            if pool_handler.is_closed('project2'):
                break
        print('Tasks have been submitted!')

        print('Getting results..')
        task_handler.get_stage2_results()

        # load stage 3

        print('Loading results to stage 3 for validation..')
        task_handler.load_stage3_tasks()

        print('Starting pool..')

        pool_handler.open_pool('project3')

        # wait for finishing

        print('Waiting for validation..')
        while True:
            sleep(30)
            if pool_handler.is_closed('project3'):
                break
        print('Validation has been finished!')

        print('Getting results..')
        acc, ovr = task_handler.get_stage3_results()
        print(f'Accepted {acc} tasks of {ovr}.')

        # load validation results

        print('Loading validation results..')
        task_handler.load_validation_results()

        # repeat until all tasks are accepted

    print('Downloading final results..')
    task_handler.get_final_results()
    print('Annotation has been finished!')
