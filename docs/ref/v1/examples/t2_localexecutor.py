# import logging
from easyflow import DAG
from easyflow.operators import PythonOperator, BashOperator
from easyflow.executors import LocalExecutor


dag = DAG(
    'simple', 
    description='simple task', executor=LocalExecutor()
)

t111 = BashOperator(
    task_id='task111',
    #bash_command='sleep 5 && echo "task111"', 
    #bash_command='echo "task111"', 
    bash_command='sleep 1.5',
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

t5 = BashOperator(
    task_id='task5',
    bash_command='echo "task5"', 
    dag=dag
)


t111 >> t22
t333 >> t22
t22 >> t5

print(dag)
print('Start ..')
dag.run()

