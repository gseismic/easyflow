'''
 11     default_args = {
 12         'owner': 'airflow',  # 拥有者名称
 13         'start_date': days_ago(1),  # 第一次开始执行的时间，为 UTC 时间(注意不要设置
 14         'retries': 3,  # 失败重试次数
 15         'retry_delay': timedelta(seconds=5)  # 失败重试间隔
 16     }
 17     return default_args
'''
import time
import uuid
import logging
import networkx as nx
from networkx import DiGraph
import matplotlib.pyplot as plt
from networkx.algorithms.dag import is_directed_acyclic_graph
from .executors import SequentialExecutor, Executor


logger = logging.getLogger(__name__)
ascii_text = '''
 _____                      _____  _
| ____|  __ _  ___  _   _  |  ___|| |  ___  __      __
|  _|   / _` |/ __|| | | | | |_   | | / _ \ \ \ /\ / /
| |___ | (_| |\__ \| |_| | |  _|  | || (_) | \ V  V /
|_____| \__,_||___/ \__, | |_|    |_| \___/   \_/\_/
                    |___/
'''

from .logger import config_logging
config_logging()

class DAG(object):

    def __init__(self, 
                 dag_id, 
                 executor=None,
                 description='',
                 options={}, schedule_interval=None):
        if executor is None:
            self._executor = SequentialExecutor()
        elif isinstance(executor, Executor):
            self._executor = executor
        else:
            raise Exception('BadExecutor')
        self._description = description
        self.init(dag_id, options, schedule_interval)
        self._tasks = set()
        self._graph = DiGraph()

    def init(self, dag_id, options, schedule_interval):
        self._dag_id = dag_id
        self._options = options
        self._schedule_interval = schedule_interval

    def add_task(self, task):
        self._tasks.add(task)

    def build_dag(self):
        for task in self._tasks:
            for dtask in task.downstreams:
                self._graph.add_edge(task, dtask)
            for utask in task.upstreams:
                self._graph.add_edge(utask, task)
    
    def is_dag(self):
        return is_directed_acyclic_graph(self._graph)

    def update_nodes_info(self, G, dagrun_id):
        '''Tasks allowed to run
        cases(any):
            - 当前节点task_need_enqueue 且 没有祖先
            - 当前节点task_need_enqueue 且 没有任何一个祖先task_need_enqueue
        # task: 
        # 没有上游(或ancestor) 或 上游祖先没有need_run
        #       node1  -> node11 -> node111
        #              \-> node12 -> node121
        #                          /^
        #       node5  -> node51 -/
        '''
        nodes_allowed_enqueue = set() # 
        nodes_need_enqueue = set() # 需要放入队列的
        for node in G.nodes:
            if not node.task_need_enqueue(dagrun_id):
                continue
            else:
                nodes_need_enqueue.add(node)

            # 包含所有祖先 nx.ancestors(G, node))，不只是上游
            anc_list = nx.ancestors(G, node)
            any_ancestor_need_run = False
            for anc in anc_list:
                if anc.task_need_enqueue(dagrun_id):
                    any_ancestor_need_run = True
                    break

            # print('an_need', any_ancestor_need_run)
            if any_ancestor_need_run:
                continue

            nodes_allowed_enqueue.add(node)
        return nodes_allowed_enqueue, nodes_need_enqueue

    def get_dagrun_id(self):
        return str(uuid.uuid4())

    def run(self):
        try:
            self._run()
        except (KeyboardInterrupt, SystemExit):
            print('** Stoped By User **')

    def _run(self):
        print(ascii_text)
        logging.info('** Running **')
        dagrun_id = self.get_dagrun_id() # uuid of this run of this dag
        # init task
        for task in self._tasks:
            task.switch2_task_pending(dagrun_id)
            # print(task.task_id, task.get_task_state(dagrun_id))

        logging.info('[dag_id=%s] Running [dagrun_id=%s] ...' % (self.dag_id, dagrun_id))
        self.build_dag()
        if not self.is_dag():
            # print(self._graph.graph)
            raise Exception('NotDAG')
        
        while 1:
            nodes_allowed_enqueue, nodes_need_enqueue = self.update_nodes_info(self._graph, dagrun_id)
            # print('='*50)
            # print('nodes_allowed_enqueue', nodes_allowed_enqueue)
            # print('nodes_need_enqueue', nodes_need_enqueue)
            # print('Running:', [t for t in self._tasks if not t.is_task_done(dagrun_id)])
               
            if nodes_need_enqueue:
                # self._executor.execute_many(dagrun_id, nodes_allowed_enqueue)
                finished_results = self._executor.update_and_try_execute(dagrun_id, nodes_allowed_enqueue)
            elif self.all_task_finished(dagrun_id):
                logging.info('** All Task Done **')
                break
            else:
                self._executor.update_and_try_execute(dagrun_id, [])
                # print('wait ...')
                time.sleep(0.1)

        self._executor.finalize()

    def all_task_finished(self, dagrun_id):
        n_done, n_failed = 0, 0
        for task in self._tasks:
            if task.is_task_done(dagrun_id):
                n_done += 1
            elif task.is_task_failed(dagrun_id):
                n_failed += 1
        print('n_done/total: %d/%d' % (n_done, len(self._tasks)))
        print('n_failed/total: %d/%d' % (n_failed, len(self._tasks)))
        return n_done + n_failed == len(self._tasks)

    def plot(self):
        nx.draw(self._graph, with_labels=True, edge_color='b', node_color='g', node_size=1000)
        plt.show()

    @property
    def dag_id(self):
        return self._dag_id

    def __repr__(self):
        s = 'Dag(dag_id=%s)\n' % self._dag_id
        s += '\tdescription: %s\n' % self._description
        s += '\ttasks: [num=%d]\n' % len(self._tasks)
        for t in self._tasks:
            s += '\t\t%s\n' % t.detail()
        return s.strip()
