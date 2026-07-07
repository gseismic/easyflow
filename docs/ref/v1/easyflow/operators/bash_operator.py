# -*- coding: UTF-8 -*-
import os
import subprocess
import logging
from .operator import Operator


class BashOperator(Operator):

    def __init__(self, task_id, dag, bash_command):
        super(BashOperator, self).__init__(task_id, dag)
        self.bash_command = bash_command

    def execute(self):
        try:
            # print('='*20)
            # print('Cmd:', self.bash_command)
            # https://docs.python.org/3/library/subprocess.html#popen-constructor
            # os.system(self.bash_command)
            # p = os.popen(self.bash_command)
            # https://blog.csdn.net/weixin_38120374/article/details/80607369
            ex = subprocess.Popen(self.bash_command, stdout=subprocess.PIPE,
                                  shell=True, stderr=subprocess.PIPE)
            out, err  = ex.communicate()
            print(out)
            print(err)
            status = ex.wait()
            # print("cmd in:", self.bash_command)
            # print("cmd out: ", out.decode())
            if ex.returncode != 0:
                raise Exception('returncode Nonzeros: %d' % ex.returncode)

            return out.decode()
            # print('Cmd:', self.bash_command, '...done')
            # subprocess.Popen(self.bash_command, shell=True)
        finally:
            pass
