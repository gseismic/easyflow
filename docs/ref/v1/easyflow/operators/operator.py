# -*- coding: UTF-8 -*-
import logging
# from collections import OrderedDict as odict
from collections import defaultdict
from enum import IntEnum


class TaskState(IntEnum):
    #Uninitialized = -1
    #Submitted = 0
    Pending = 2
    Queued = 3
    Running = 5
    #Retrying = 4
    Done = 6
    Failed = -8


class Operator(object):

    def __init__(self, task_id, dag):
        self._task_id = task_id
        self._dag = dag
        self._upstreams = set()
        self._downstreams = set()
        self._dag.add_task(self)
        self._status_dict = defaultdict(dict)

    def run(self, dagrun_id):
        # assert(self.get_task_state(dagrun_id) in [TaskState.Pending])
        # self.switch2_task_running()
        self.execute()

    def execute(self):
        raise NotImplementedError()

    def switch2_task_state(self, dagrun_id, new_state):
        self._status_dict[dagrun_id] = new_state

    def task_need_enqueue(self, dagrun_id):
        # 目前不允许重试
        return self._status_dict.get(dagrun_id) in [TaskState.Pending]
        # return self._status_dict.get(dagrun_id) not in [TaskState.Done, TaskState.Running]

    def get_task_state(self, dagrun_id):
        return self._status_dict.get(dagrun_id)

    def is_task_done(self, dagrun_id):
        return self._status_dict.get(dagrun_id) == TaskState.Done

    def is_task_pending(self, dagrun_id):
        return self._status_dict.get(dagrun_id) == TaskState.Pending

    def is_task_queued(self, dagrun_id):
        return self._status_dict.get(dagrun_id) == TaskState.Queued

    def is_task_failed(self, dagrun_id):
        return self._status_dict.get(dagrun_id) == TaskState.Failed

    def switch2_task_pending(self, dagrun_id):
        assert(self.get_task_state(dagrun_id) in [None])
        self.switch2_task_state(dagrun_id, TaskState.Pending)

    def switch2_task_queued(self, dagrun_id):
        assert(self.get_task_state(dagrun_id) in [TaskState.Pending])
        self.switch2_task_state(dagrun_id, TaskState.Queued)

    def switch2_task_running(self, dagrun_id):
        assert(self.get_task_state(dagrun_id) in [TaskState.Queued])
        self.switch2_task_state(dagrun_id, TaskState.Running)

    def switch2_task_done(self, dagrun_id):
        assert(self.get_task_state(dagrun_id) in [TaskState.Running])
        self.switch2_task_state(dagrun_id, TaskState.Done)

    def switch2_task_failed(self, dagrun_id):
        assert(self.get_task_state(dagrun_id) in [TaskState.Running])
        self.switch2_task_state(dagrun_id, TaskState.Failed)

    def add_upstream(self, task):
        logging.debug('add upstream ..: %s -> %s' % (task.task_id, self.task_id))
        self.upstreams.add(task)
        task.downstream.add(self)

    def add_downstream(self, task):
        logging.debug('add downstream ..: %s -> %s' % (task.task_id, self.task_id))
        self.downstreams.add(task)
        task.upstreams.add(self)

    def __lshift__(self, other):
        self.add_upstream(other)
        assert self.dag_id == other.dag_id

    def __rshift__(self, other):
        self.add_downstream(other)
        assert self.dag_id == other.dag_id

    @property
    def task_id(self):
        return self._task_id

    @property
    def dag(self):
        return self._dag

    @property
    def dag_id(self):
        return self._dag.dag_id

    @property
    def upstreams(self):
        return self._upstreams

    @property
    def downstreams(self):
        return self._downstreams

    def __repr__(self):
        return 'Task(task_id=%s)' % self.task_id

    def detail(self):
        return 'task_id=%s: with upstreams={%s}, downstreams={%s}' % (
            self.task_id, 
            str([t.task_id for t in self.upstreams]),
            str([t.task_id for t in self.downstreams])
        )
