import importlib
from easyflow import DAG
from easyflow.operators import PythonOperator, BashOperator


def task1(dag):
    task = BashOperator(
        task_id='test1',
        bash_command='echo "hello world"', 
        dag=dag)
    return task


def task2(dag):
    def python_fun():
        print('helo!!!!')

    task = PythonOperator(
        task_id='test1',
        python_callable=python_fun,
        dag=dag)
    return task


@task.python
def task2(dag):
    print('helo!!!!')


with DAG(__name__) as d:
    task1 = MyTask2('task1', d)
    task2 = MyTask2('task1', d)
    task1 >> task2


if task1.run():
    task2.run()
else:
    patch_task.run()
    task2.run()


class Task:
    pass


def run(self):
    pass
