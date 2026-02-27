"""
Pipeline Orchestrator - Simulates Airflow-style DAG orchestration
Demonstrates understanding of workflow dependencies and scheduling
"""

from datetime import datetime, timedelta
import time
import random
from typing import Dict, Any, List, Callable
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Task:
    """
    Represents a single task in a pipeline (similar to Airflow Operator)
    """
    
    def __init__(self, task_id: str, function: Callable, retries: int = 3, retry_delay: int = 5):
        self.task_id = task_id
        self.function = function
        self.retries = retries
        self.retry_delay = retry_delay
        self.upstream_tasks = []
        self.downstream_tasks = []
        self.status = 'pending'  # pending, running, success, failed, skipped
        self.start_time = None
        self.end_time = None
        self.result = None
        self.error = None
    
    def add_upstream(self, task: 'Task'):
        """Add upstream dependency"""
        self.upstream_tasks.append(task)
        task.downstream_tasks.append(self)
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the task with retry logic
        """
        self.status = 'running'
        self.start_time = datetime.now()
        
        logger.info(f"🚀 Starting task: {self.task_id}")
        
        for attempt in range(self.retries):
            try:
                # Execute the task function
                result = self.function(context)
                
                self.result = result
                self.status = 'success'
                self.end_time = datetime.now()
                
                duration = (self.end_time - self.start_time).total_seconds()
                logger.info(f"✅ Task {self.task_id} completed in {duration:.2f}s")
                
                return result
                
            except Exception as e:
                self.error = str(e)
                if attempt < self.retries - 1:
                    logger.warning(f"⚠️ Task {self.task_id} failed (attempt {attempt+1}/{self.retries}): {e}")
                    time.sleep(self.retry_delay)
                else:
                    self.status = 'failed'
                    self.end_time = datetime.now()
                    logger.error(f"❌ Task {self.task_id} failed after {self.retries} attempts: {e}")
                    raise
        
        return None

class DAG:
    """
    Directed Acyclic Graph - represents a workflow (similar to Airflow DAG)
    """
    
    def __init__(self, dag_id: str, schedule: str = None, start_date: datetime = None):
        self.dag_id = dag_id
        self.schedule = schedule  # cron expression or None for manual
        self.start_date = start_date or datetime.now()
        self.tasks = {}
        self.execution_history = []
    
    def add_task(self, task: Task):
        """Add a task to the DAG"""
        self.tasks[task.task_id] = task
    
    def get_task(self, task_id: str) -> Task:
        """Get task by ID"""
        return self.tasks.get(task_id)
    
    def set_dependencies(self, upstream_id: str, downstream_id: str):
        """Set task dependencies"""
        upstream = self.get_task(upstream_id)
        downstream = self.get_task(downstream_id)
        
        if upstream and downstream:
            downstream.add_upstream(upstream)
            logger.info(f"Set dependency: {upstream_id} → {downstream_id}")
        else:
            raise ValueError(f"Task not found: {upstream_id if not upstream else downstream_id}")
    
    def get_root_tasks(self) -> List[Task]:
        """Get tasks with no upstream dependencies"""
        return [task for task in self.tasks.values() if not task.upstream_tasks]
    
    def get_leaf_tasks(self) -> List[Task]:
        """Get tasks with no downstream dependencies"""
        return [task for task in self.tasks.values() if not task.downstream_tasks]
    
    def validate_dag(self) -> bool:
        """
        Validate that the DAG has no cycles
        Simple DFS cycle detection
        """
        visited = set()
        recursion_stack = set()
        
        def has_cycle(task: Task, path: List[str] = None) -> bool:
            if path is None:
                path = []
            
            if task.task_id in recursion_stack:
                cycle_path = path + [task.task_id]
                logger.error(f"⚠️ Cycle detected: {' → '.join(cycle_path)}")
                return True
            
            if task.task_id in visited:
                return False
            
            visited.add(task.task_id)
            recursion_stack.add(task.task_id)
            path.append(task.task_id)
            
            for downstream in task.downstream_tasks:
                if has_cycle(downstream, path.copy()):
                    return True
            
            recursion_stack.remove(task.task_id)
            return False
        
        # Check each root task
        for task in self.get_root_tasks():
            if has_cycle(task):
                return False
        
        logger.info(f"✅ DAG '{self.dag_id}' validation passed - no cycles detected")
        return True
    
    def run(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute the DAG (topological order)
        """
        if not self.validate_dag():
            raise ValueError("DAG validation failed - cannot execute")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Starting DAG: {self.dag_id}")
        logger.info(f"{'='*60}")
        
        context = context or {}
        context['dag_id'] = self.dag_id
        context['execution_date'] = datetime.now()
        context['results'] = {}
        
        start_time = datetime.now()
        
        # Get execution order (topological sort)
        execution_order = []
        visited = set()
        
        def visit(task: Task):
            if task.task_id in visited:
                return
            for upstream in task.upstream_tasks:
                visit(upstream)
            visited.add(task.task_id)
            execution_order.append(task)
        
        for task in self.get_root_tasks():
            visit(task)
        
        # Execute tasks in order
        for task in execution_order:
            # Check if all upstream tasks succeeded
            upstream_status = [t.status for t in task.upstream_tasks]
            if upstream_status and all(s == 'success' for s in upstream_status):
                try:
                    result = task.run(context)
                    context['results'][task.task_id] = result
                except Exception as e:
                    context['results'][task.task_id] = {'error': str(e)}
                    # Stop execution on task failure (could add branch logic)
                    logger.error(f"❌ DAG stopped due to task failure: {task.task_id}")
                    break
            elif upstream_status:
                task.status = 'skipped'
                logger.warning(f"⏭️ Skipping {task.task_id} - upstream tasks not successful")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Summarize execution
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 DAG '{self.dag_id}' Execution Summary")
        logger.info(f"{'='*60}")
        logger.info(f"Duration: {duration:.2f}s")
        
        for task in execution_order:
            status_icon = {
                'success': '✅',
                'failed': '❌',
                'skipped': '⏭️',
                'running': '🔄',
                'pending': '⏳'
            }.get(task.status, '❓')
            
            duration = ""
            if task.start_time and task.end_time:
                task_duration = (task.end_time - task.start_time).total_seconds()
                duration = f" ({task_duration:.2f}s)"
            
            logger.info(f"{status_icon} {task.task_id}: {task.status}{duration}")
        
        execution_record = {
            'dag_id': self.dag_id,
            'execution_date': context['execution_date'].isoformat(),
            'duration': duration,
            'task_status': {task.task_id: task.status for task in execution_order}
        }
        
        self.execution_history.append(execution_record)
        
        return context

class DataPipelineFactory:
    """
    Factory for creating common data pipeline patterns
    """
    
    @staticmethod
    def create_etl_dag(dag_id: str, source: str, destination: str) -> DAG:
        """
        Create a standard ETL pipeline DAG
        """
        dag = DAG(dag_id, schedule="0 2 * * *")  # Run at 2 AM daily
        
        # Define ETL tasks
        def extract(context):
            logger.info(f"📤 EXTRACT: Reading from {source}")
            time.sleep(2)  # Simulate work
            data = {
                'source': source,
                'records': random.randint(1000, 5000),
                'timestamp': datetime.now().isoformat()
            }
            context['extracted_data'] = data
            return data
        
        def transform(context):
            logger.info(f"🔄 TRANSFORM: Processing {context['extracted_data']['records']} records")
            time.sleep(3)  # Simulate work
            transformed = {
                **context['extracted_data'],
                'transformed': True,
                'quality_score': random.uniform(0.95, 0.99)
            }
            context['transformed_data'] = transformed
            return transformed
        
        def validate(context):
            logger.info(f"✅ VALIDATE: Checking data quality")
            time.sleep(1)
            data = context['transformed_data']
            if data['quality_score'] < 0.95:
                raise ValueError(f"Quality score too low: {data['quality_score']}")
            context['validated_data'] = data
            return data
        
        def load(context):
            logger.info(f"📥 LOAD: Writing to {destination}")
            time.sleep(2)
            result = {
                **context['validated_data'],
                'destination': destination,
                'loaded_at': datetime.now().isoformat()
            }
            return result
        
        # Create tasks
        extract_task = Task('extract', extract)
        transform_task = Task('transform', transform)
        validate_task = Task('validate', validate)
        load_task = Task('load', load)
        
        # Add to DAG
        dag.add_task(extract_task)
        dag.add_task(transform_task)
        dag.add_task(validate_task)
        dag.add_task(load_task)
        
        # Set dependencies
        dag.set_dependencies('extract', 'transform')
        dag.set_dependencies('transform', 'validate')
        dag.set_dependencies('validate', 'load')
        
        return dag
    
    @staticmethod
    def create_branching_dag(dag_id: str) -> DAG:
        """
        Create a DAG with conditional branching
        """
        dag = DAG(dag_id)
        
        def start(context):
            logger.info("🚀 Starting branching pipeline")
            context['data_quality'] = random.choice(['good', 'bad', 'needs_review'])
            return {'quality': context['data_quality']}
        
        def process_good(context):
            logger.info("✅ Processing good quality data")
            return {'path': 'good', 'processed': True}
        
        def process_bad(context):
            logger.info("⚠️ Fixing bad quality data")
            return {'path': 'bad', 'fixed': True}
        
        def review_needed(context):
            logger.info("🔍 Data needs manual review")
            return {'path': 'review', 'status': 'pending_review'}
        
        def join(context):
            logger.info("🔄 Joining all branches")
            return {'all_branches_complete': True}
        
        # Create tasks
        start_task = Task('start', start)
        good_task = Task('process_good', process_good)
        bad_task = Task('process_bad', process_bad)
        review_task = Task('review_needed', review_needed)
        join_task = Task('join', join)
        
        # Add to DAG
        dag.add_task(start_task)
        dag.add_task(good_task)
        dag.add_task(bad_task)
        dag.add_task(review_task)
        dag.add_task(join_task)
        
        # Set dependencies (branching logic handled in task functions)
        dag.set_dependencies('start', 'process_good')
        dag.set_dependencies('start', 'process_bad')
        dag.set_dependencies('start', 'review_needed')
        dag.set_dependencies('process_good', 'join')
        dag.set_dependencies('process_bad', 'join')
        dag.set_dependencies('review_needed', 'join')
        
        return dag

if __name__ == "__main__":
    """
    Demonstrate pipeline orchestration concepts
    """
    
    print("\n" + "="*60)
    print("🚀 Pipeline Orchestrator Demonstration")
    print("="*60)
    
    # Example 1: Simple ETL Pipeline
    print("\n📊 Example 1: ETL Pipeline DAG")
    print("-" * 40)
    
    etl_dag = DataPipelineFactory.create_etl_dag(
        'daily_sales_etl',
        'sales_database',
        'data_warehouse'
    )
    
    context = etl_dag.run()
    
    print(f"\n📈 ETL Results:")
    print(f"  • Records processed: {context['results']['extract']['records']}")
    print(f"  • Quality score: {context['results']['validate']['quality_score']:.3f}")
    print(f"  • Loaded to: {context['results']['load']['destination']}")
    
    # Example 2: Branching Pipeline
    print("\n\n🔄 Example 2: Branching Pipeline DAG")
    print("-" * 40)
    
    branch_dag = DataPipelineFactory.create_branching_dag('conditional_processing')
    context = branch_dag.run()
    
    print(f"\n📊 Branching Results:")
    for task_id, result in context['results'].items():
        if isinstance(result, dict):
            print(f"  • {task_id}: {result}")
    
    # Example 3: Show DAG structure
    print("\n\n📋 Example 3: DAG Structure Visualization")
    print("-" * 40)
    
    def print_dag_structure(dag: DAG):
        print(f"\nDAG: {dag.dag_id}")
        print("Schedule:", dag.schedule or "Manual")
        
        # Print dependency tree
        for task in dag.get_root_tasks():
            print_task_tree(task, "")
    
    def print_task_tree(task: Task, prefix: str):
        print(f"{prefix}└─ {task.task_id}")
        for i, downstream in enumerate(task.downstream_tasks):
            is_last = (i == len(task.downstream_tasks) - 1)
            new_prefix = prefix + ("    " if is_last else "│   ")
            print_task_tree(downstream, new_prefix)
    
    print_dag_structure(etl_dag)
    
    print("\n" + "="*60)
    print("✅ Orchestration demonstration complete")
    print("="*60)
    print("\n📝 Key Concepts Demonstrated:")
    print("  • DAG (Directed Acyclic Graph) structure")
    print("  • Task dependencies and execution order")
    print("  • Retry logic and error handling")
    print("  • Conditional branching")
    print("  • Context passing between tasks")
    print("  • Execution logging and monitoring")
