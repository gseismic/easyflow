class Flow(object):

    def __init__(self):
        pass


class SequentialExecutor(object):

    def __init__(self):
        from collections import OrderedDict
        self.queued_tasks = OrderedDict()

    def queue_command(self, task, command, priority: int = 1):
        self.queued_tasks[task.key] = (command, priority, task)
