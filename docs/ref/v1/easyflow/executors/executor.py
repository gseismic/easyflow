from collections import defaultdict


class Executor(object):

    def __init__(self):
        # 进入执行器中的任务状态
        # 注意，进了执行器，就不能拿出来了
        self._pending_tasks = defaultdict(set) # 需要被执行的
        self._canceled_task = defaultdict(set) # 取消的，只有pending态的允许取消
        self._running_task = defaultdict(set) # 正在
        self._done_task = defaultdict(set) # 完成的
        self._failure_task = defaultdict(set) # 失败的

    def update_and_try_execute(self, dagrun_id, tasks):
        raise NotImplementedError()

    def update_pending_tasks(self, dagrunt_id, tasks):
        if isinstance(tasks, (set, list, tuple)):
            self._pending_tasks[dagrunt_id].update(set(tasks))
        else:
            self._pending_tasks[dagrunt_id].add(tasks)

    def get_pending_tasks(self, dagrunt_id):
        return self._pending_tasks[dagrunt_id]

    def sorted_tasks(self):
        pass

    def finalize(self):
        print('executor finalize ..')
        pass
