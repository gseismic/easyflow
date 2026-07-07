# import logging
from easyflow import DAG
from easyflow.executors import LocalExecutor
from easyflow.operators import PythonOperator, BashOperator


dag = DAG(
    'simple', 
    description='simple task', executor=LocalExecutor()
)

t111 = BashOperator(
    task_id='task111',
    bash_command='echo "task111"', 
    dag=dag
)

t22 = BashOperator(
    task_id='task22',
    bash_command='sleep 3 && echo "task22"', 
    dag=dag
)

t333 = BashOperator(
    task_id='task333',
    bash_command='echo "task333"', 
    dag=dag
)

def func(x):
    print('Hello!', x)
    raise Exception('Bad')


t5 = PythonOperator(
    task_id='task5',
    dag=dag,
    python_callable=func,
    #op_args=[1,2,3]
    op_args=[[1,2,3]]
    #op_kwargs={'x': [1,2,3]}
)


t111 >> t22
t333 >> t22
t22 >> t5


print(dag)
print('Start ..')
dag.run()
