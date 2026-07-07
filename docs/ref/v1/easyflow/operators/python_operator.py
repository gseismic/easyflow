# -*- coding: UTF-8 -*-
from .operator import Operator


class PythonOperator(Operator):

    def __init__(self, task_id, dag, python_callable, op_args=[], op_kwargs={}):
        super(PythonOperator, self).__init__(task_id, dag)
        self.python_callable = python_callable
        self.op_args = op_args
        self.op_kwargs = op_kwargs

    def execute(self):
        try:
            if self.op_args and self.op_kwargs:
                return self.python_callable(*self.op_args, **self.op_kwargs)
            elif self.op_kwargs:
                return self.python_callable(**self.op_kwargs)
            elif self.op_args:
                return self.python_callable(*self.op_args)
            else:
                return self.python_callable()
        finally:
            pass
