#from process import Process
from .executor import Executor
import multiprocessing
from multiprocessing import Pool, TimeoutError

# https://www.ikaze.cn/article/27
import multiprocess
from pathos.multiprocessing import ProcessPool


import os
class A:
    def dummy_exec(self):
        os.system('echo dummpy')
        return 0


class LocalExecutor(Executor):

    def __init__(self, num_task_concurrents=None):
        super(LocalExecutor, self).__init__()
        if num_task_concurrents is None:
            self.num_task_concurrents = multiprocessing.cpu_count()
        elif num_task_concurrents >= 1:
            self.num_task_concurrents = num_task_concurrents
        else:
            raise Exception('Need int >0 workers')
        # print('Num workers:', self.num_task_concurrents)
        # self.pool = Pool(self.num_task_concurrents)
        self.pool = ProcessPool(self.num_task_concurrents)
        self.pool_task_info = {}

    def update_and_try_execute(self, dagrun_id, tasks_allowed_enqueue):
        # self.update_pending_tasks(dagrun_id, tasks_allowed_enqueue)
        # print('Num tasks_allowed_enqueue that need run: %d' % len(tasks_allowed_enqueue))

        for task in tasks_allowed_enqueue:
            _id = (dagrun_id, task.task_id)
            # 目标假定所有放入pool中的task 都处于运行态
            # 为了通信安全，使用redis，然后在task中改变自己的状态
            task.switch2_task_queued(dagrun_id)
            task.switch2_task_running(dagrun_id)
            # print(dir(task))
            # result = self.pool.apply_async(task.execute, args=())
            result = self.pool.apipe(task.execute)
            # result = self.pool.apply_async(a.dummy_exec, args=())
            # print(type(result))
            self.pool_task_info[_id] = (task, result)

        # check all task ...
        finished_results = {}
        # print('check all', len(self.pool_task_info))
        for _id, info in self.pool_task_info.items():
            try:
                task, result = info
                finished_results[_id] = result.get(timeout=0.1)
                if not task.is_task_done(dagrun_id):
                    # print(task.get_task_state(dagrun_id))
                    task.switch2_task_done(dagrun_id)
            # except TimeoutError:
            except multiprocess.context.TimeoutError:
                # print('timeout')
                pass
        return finished_results

    def finalize(self):
        pass
