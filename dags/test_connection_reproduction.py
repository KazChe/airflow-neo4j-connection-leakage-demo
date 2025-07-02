"""
DAGs - you like dags? - you like neo4j? - you like airflow? - you like to reproduce the customer's neo4j connection issues?
It creates a DAG with multiple parallel tasks to simulate bolt connection leakage.
It also provides an improved connection pattern to fix the issue.
"""

import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase, AsyncGraphDatabase
import asyncio
import time

# this logger's output does not show in airflow logs. killedme
logger = logging.getLogger(__name__)

# Customer's original problematic code
class ProblematicDatabaseConnection:
    """This reproduces the customer's original connection pattern"""
    
    _db_conn_async_alias = {}
    _db_conn_alias = {}
    
    @classmethod
    async def get_async_driver(cls, alias: str = "root"):
        """Recreates driver every time - the problematic pattern"""
        auth = (
            os.getenv("NEO4J_USERNAME"),
            os.getenv("NEO4J_PASSWORD")
        )
        
        #creates a NEW driver every time (the issue!)
        cls._db_conn_async_alias[alias] = AsyncGraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=auth,
            max_connection_lifetime=120,
            max_transaction_retry_time=10
        )
        
        await cls._db_conn_async_alias[alias].verify_connectivity()
        return cls._db_conn_async_alias[alias]
    
    @classmethod
    def get_connection(cls, alias: str = "root"):
        """Recreates driver every time - the problematic pattern"""
        print(f"🍌 DEBUG: ProblematicDatabaseConnection.get_connection called for alias '{alias}'")
        print(f"🍌 DEBUG: current drivers in memory: {list(cls._db_conn_alias.keys())}")
        
        auth = (
            os.getenv("NEO4J_USERNAME"),
            os.getenv("NEO4J_PASSWORD")
        )
        print(f"🍌 DEBUG: about to create NEW driver for alias '{alias}' (THIS IS THE PROBLEM!)🍌🍌🍌")
        
        # This creates a NEW driver every time (the bug!)
        cls._db_conn_alias[alias] = GraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=auth,
            max_connection_lifetime=120,
            max_transaction_retry_time=10
        )
        print(f"🍌 Problematic: created new driver for alias '{alias}'")
        print(f"🍌 DEBUG: total drivers in memory: {len(cls._db_conn_alias)}")
        print(f"DEBUG: Seeing 1 driver for each task. This is the problem. 🍌🍌🍌")
        
        cls._db_conn_alias[alias].verify_connectivity()
        print(f"..and it is verfied as healthy")
        return cls._db_conn_alias[alias]

# Improved connection management <---------:banana:
class ImprovedDatabaseConnection:
    """This shows the improved connection pattern"""
    
    _db_conn_async_alias = {}
    _db_conn_alias = {}
    
    @classmethod
    async def get_async_driver(cls, alias: str = "root"):
        """Reuses existing driver - the fix"""
        # Check if driver already exists and is healthy
        if alias in cls._db_conn_async_alias:
            try:
                await cls._db_conn_async_alias[alias].verify_connectivity()
                print(f"Reusing existing async driver for {alias}")
                return cls._db_conn_async_alias[alias]
            except Exception as e:
                print(f"Existing driver unhealthy, recreating: {e}")
                await cls._db_conn_async_alias[alias].close()
                del cls._db_conn_async_alias[alias]
        
        # Create new driver only if needed
        auth = (
            os.getenv("NEO4J_USERNAME"),
            os.getenv("NEO4J_PASSWORD")
        )
        
        cls.__db_conn_async_alias[alias] = AsyncGraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=auth,
            max_connection_pool_size=50,
            max_connection_lifetime=300,
            connection_acquisition_timeout=60,
            max_transaction_retry_time=15,
            keep_alive=True,
            connection_timeout=30,
        )
        
        await cls._db_conn_async_alias[alias].verify_connectivity()
        print(f"Created NEW async driver for {alias}")
        return cls._db_conn_async_alias[alias]
    
    @classmethod
    def get_connection(cls, alias: str = "root"):
        """Reuses existing driver - the fix"""
        print(f"DEBUG: ImprovedDatabaseConnection.get_connection called for alias '{alias}'")
        print(f"DEBUG: current drivers in memory: {list(cls._db_conn_alias.keys())}")
        # Check if driver already exists and is healthy
        if alias in cls._db_conn_alias:
            try:
                cls._db_conn_alias[alias].verify_connectivity()
                print(f"🍌 Improved: reusing existing sync driver for {alias}")
                return cls._db_conn_alias[alias]
            except Exception as e:
                print(f"🍌 Improved: existing driver con not establish connection, creating a new one: {e}")
                cls._db_conn_alias[alias].close()
                del cls._db_conn_alias[alias]
        
        # Create new driver only if needed
        auth = (
            os.getenv("NEO4J_USERNAME"),
            os.getenv("NEO4J_PASSWORD")
        )
        
        cls._db_conn_alias[alias] = GraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=auth,
            max_connection_pool_size=50,
            max_connection_lifetime=300,
            connection_acquisition_timeout=60,
            max_transaction_retry_time=15,
            keep_alive=True,
            connection_timeout=30,
        )
        
        cls._db_conn_alias[alias].verify_connectivity()
        print(f"🍌 Improved: created NEW sync driver for {alias}")
        return cls._db_conn_alias[alias]

# our task functions to test both patterns
def problematic_neo4j_task(task_number: int, **context):
    """Task using the problematic connection pattern"""
    task_id = context['task_instance'].task_id
    
    try:
        # This creates a NEW driver every time! (the issue!)
        driver = ProblematicDatabaseConnection.get_connection()
        
        with driver.session() as session:
            result = session.run("RETURN $task_num as task_number, $task_id as task_id", 
                               task_num=task_number, task_id=task_id)
            record = result.single()
            print(f"Problematic task {task_id}: {record['task_number']}")
            
        # simulate some work, to make the task take longer
        time.sleep(2)
        
        return f"Completed problematic task {task_number}"
        
    except Exception as e:
        print(f"Problematic task {task_id} failed: {e}")
        raise

def improved_neo4j_task(task_number: int, **context):
    """Task using the improved connection pattern"""
    task_id = context['task_instance'].task_id
    
    try:
        # This reuses existing driver! (the fix!)
        driver = ImprovedDatabaseConnection.get_connection()
        
        with driver.session() as session:
            # using session here but in production we would use a transaction!!
            result = session.run("RETURN $task_num as task_number, $task_id as task_id", 
                               task_num=task_number, task_id=task_id)
            record = result.single()
            print(f"Improved task {task_id}: {record['task_number']}")
            
        # simulate some work to make the task take longer
        time.sleep(2)
        
        return f"Completed improved task {task_number}"
        
    except Exception as e:
        print(f"Improved task {task_id} failed: {e}")
        raise   

# DAG definitions
default_args = {
    'owner': 'neo4j-test',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 1: Reprduce the problematic pattern
problematic_dag = DAG(
    'neo4j_problematic_pattern',
    default_args=default_args,
    description='Reproduce customer connection issues',
    # schedule_interval=timedelta(minutes=5),
    schedule_interval=None,  #trigger it in airflow UI manually
    max_active_runs=1,
    catchup=False,
)

# create multiple parallel tasks to simulate Airflow workload
for i in range(100):  # 100 parallel tasks
    task = PythonOperator(
        task_id=f'problematic_task_{i}',
        python_callable=problematic_neo4j_task,
        op_kwargs={'task_number': i},
        dag=problematic_dag,
    )

# DAG 2: Test the improved pattern
improved_dag = DAG(
    'neo4j_improved_pattern',
    default_args=default_args,
    description='Test improved connection management',
    # schedule_interval=timedelta(minutes=5),
    schedule_interval=None,  # manually trigger it in airflow UI
    max_active_runs=1,
    catchup=False,
)

# create multiple parallel tasks with improved pattern
for i in range(100):  # 100 parallel tasks
    task = PythonOperator(
        task_id=f'improved_task_{i}',
        python_callable=improved_neo4j_task,
        op_kwargs={'task_number': i},
        dag=improved_dag,
    )

# DAG 3: Stress test - many concurrent tasks //TODO: to be tested yet
stress_dag = DAG(
    'neo4j_stress_test',
    default_args=default_args,
    description='Stress test to trigger connection spikes',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    catchup=False,
)

# create many tasks to simulate the customer's workload //TODO: to be tested yet
for i in range(200):  # 200 parallel tasks
    stress_task = PythonOperator(
        task_id=f'stress_task_{i}',
        python_callable=problematic_neo4j_task,
        op_kwargs={'task_number': i},
        dag=stress_dag,
    )