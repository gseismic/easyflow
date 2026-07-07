#from process import Process
from .executor import Executor
from multiprocessing import Pool, TimeoutError

# https://www.ikaze.cn/article/27
from pathos.multiprocessing import ProcessPool


import os
class A:
    def dummy_exec(self):
        os.system('echo dummpy')
        return 0


class LocalExecutor(Executor):

    def __init__(self):
        super(LocalExecutor, self).__init__()
        self.num_task_concurrents = 5
        # self.pool = Pool(self.num_task_concurrents)
        self.pool = ProcessPool(self.num_task_concurrents)
        self.pool_task_info = {}

    def update_and_try_execute(self, dagrun_id, tasks):
        # self.update_pending_tasks(dagrun_id, tasks)
        print('Num tasks that need run: %d' % len(tasks))

        for task in tasks:
            _id = (dagrun_id, task.task_id)
            # 目标假定所有放入pool中的task 都处于运行态
            task.switch2_task_running(dagrun_id)
            print(dir(task))
            # result = self.pool.apply_async(task.execute, args=())
            result = self.pool.apipe(task.execute)
            # result = self.pool.apply_async(a.dummy_exec, args=())
            print(type(result))
            self.pool_task_info[_id] = (task, result)

        finished_results = {}
        for _id, info in self.pool_task_info.items():
            try:
                print(_id)
                task, result = info
                print(dir(task))
                print(type(result))
                rv = result.get(timeout=0.02)
                print(rv)
                # continue
                finished_results[_id] = result.get(timeout=0.02)
                if not task.is_task_done(dagrun_id):
                    print(task.get_task_state(dagrun_id))
                    task.switch2_task_done(dagrun_id)
            except TimeoutError:
                print('timeout')
                pass
        return finished_results

    def close(self):
        pass
