import logging
import traceback
from .executor import Executor


class SequentialExecutor(Executor):

    def __init__(self):
        super(SequentialExecutor, self).__init__()

    def update_and_try_execute(self, dagrun_id, tasks_allowed_enqueue):
        print('Num tasks_allowed_enqueue that need run: %d' % len(tasks_allowed_enqueue))
        true_results = {}
        for task in tasks_allowed_enqueue:
            try:
                print('Run[%s]: task_id=[%s] ..' % (dagrun_id, task.task_id))
                # print(task.task_id, task.get_task_state(dagrun_id))
                # state -> running
                task.switch2_task_queued(dagrun_id)
                task.switch2_task_running(dagrun_id)
                rv = task.run(dagrun_id)
                _id = (dagrun_id, task.task_id)
                true_results[_id] = rv
                # state -> done
                task.switch2_task_done(dagrun_id)
            except Exception as e:
                logging.error('Running Task: %s' % str(task.task_id))
                logging.error(traceback.format_exc())
                task.switch2_task_failed(dagrun_id)
        return true_results
