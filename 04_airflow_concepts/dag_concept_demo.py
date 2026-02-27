"""
Airflow DAG Concepts Demo
Shows understanding of Apache Airflow DAG structure and operators
This is a conceptual demonstration - would run in Airflow environment
"""

from datetime import datetime, timedelta
import textwrap

class AirflowConceptDemo:
    """
    Educational demonstration of Airflow concepts
    Shows the structure and logic without requiring Airflow installation
    """
    
    @staticmethod
    def show_dag_structure():
        """Display a typical Airflow DAG structure"""
        
        dag_example = textwrap.dedent("""
        # =====================================================
        # TYPICAL AIRFLOW DAG STRUCTURE
        # =====================================================
        
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from airflow.operators.bash import BashOperator
        from datetime import datetime, timedelta
        
        # Default arguments applied to all tasks
        default_args = {
            'owner': 'tomleja',
            'depends_on_past': False,
            'email': ['tomleja@gmail.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        }
        
        # Define the DAG
        dag = DAG(
            'sales_data_pipeline',
            default_args=default_args,
            description='ETL pipeline for sales data',
            schedule_interval='0 2 * * *',  # Run at 2 AM daily
            catchup=False,  # Don't run for past dates
            tags=['etl', 'sales'],
        )
        
        # Define tasks
        def extract_data(**context):
            # Extract logic here
            return {'status': 'extracted'}
        
        def transform_data(**context):
            # Transform logic here
            return {'status': 'transformed'}
        
        def load_data(**context):
            # Load logic here
            return {'status': 'loaded'}
        
        extract_task = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
            dag=dag,
        )
        
        transform_task = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            dag=dag,
        )
        
        load_task = PythonOperator(
            task_id='load_data',
            python_callable=load_data,
            dag=dag,
        )
        
        # Set task dependencies
        extract_task >> transform_task >> load_task
        
        # Alternative syntax:
        # extract_task.set_downstream(transform_task)
        # transform_task.set_downstream(load_task)
        """)
        
        print(dag_example)
    
    @staticmethod
    def show_operator_types():
        """Show different Airflow operator types"""
        
        operators = {
            'PythonOperator': 'Execute Python functions',
            'BashOperator': 'Execute bash commands',
            'SqlOperator': 'Execute SQL queries',
            'EmailOperator': 'Send emails',
            'HttpOperator': 'Make HTTP requests',
            'Sensor': 'Wait for condition (file, time, API response)',
            'BranchPythonOperator': 'Conditional branching',
            'DummyOperator': 'Placeholder for grouping',
            'SubDagOperator': 'Nested DAGs',
            'TriggerDagRunOperator': 'Trigger other DAGs',
        }
        
        print("\n📋 Airflow Operator Types:")
        for op, desc in operators.items():
            print(f"  • {op}: {desc}")
    
    @staticmethod
    def show_schedule_intervals():
        """Show schedule interval examples"""
        
        schedules = {
            'None': "Manual trigger only",
            '@once': "Run once",
            '@hourly': "First minute of every hour",
            '@daily': "Midnight every day",
            '@weekly': "Sunday at midnight",
            '@monthly': "First day of month at midnight",
            '@yearly': "First day of year at midnight",
            '0 2 * * *': "2 AM daily (cron)",
            '*/15 * * * *': "Every 15 minutes (cron)",
            '0 0 * * 0': "Midnight every Sunday (cron)",
            '0 9-17 * * 1-5': "Every hour 9 AM-5 PM weekdays (cron)",
        }
        
        print("\n⏰ Schedule Interval Examples:")
        for schedule, desc in schedules.items():
            print(f"  • {schedule:15} : {desc}")
    
    @staticmethod
    def show_xcom_example():
        """Show XCom (cross-communication) example"""
        
        example = textwrap.dedent("""
        # =====================================================
        # XCOM - TASK COMMUNICATION
        # =====================================================
        
        def task_1(**context):
            # Push value to XCom
            value = {'key': 'data_from_task_1'}
            context['ti'].xcom_push(key='my_data', value=value)
            return value  # Return also auto-pushes to XCom
        
        def task_2(**context):
            # Pull value from XCom
            ti = context['ti']
            data = ti.xcom_pull(task_ids='task_1', key='my_data')
            # Or pull from return value:
            # data = ti.xcom_pull(task_ids='task_1')
            
            print(f"Received from task 1: {data}")
            return {'processed': data}
        
        # In DAG definition:
        # task_1 >> task_2
        """)
        
        print(example)
    
    @staticmethod
    def show_branching_example():
        """Show conditional branching example"""
        
        example = textwrap.dedent("""
        # =====================================================
        # CONDITIONAL BRANCHING
        # =====================================================
        
        from airflow.operators.python import BranchPythonOperator
        
        def choose_branch(**context):
            # Logic to decide which path to take
            data_quality = check_data_quality()
            
            if data_quality > 0.95:
                return 'process_good_data'
            elif data_quality > 0.80:
                return 'clean_data_task'
            else:
                return 'send_alert_email'
        
        branch_task = BranchPythonOperator(
            task_id='branch_decision',
            python_callable=choose_branch,
            dag=dag,
        )
        
        # Tasks on different branches
        good_task = PythonOperator(task_id='process_good_data', ...)
        clean_task = PythonOperator(task_id='clean_data_task', ...)
        alert_task = PythonOperator(task_id='send_alert_email', ...)
        
        # Join branch back
        join_task = DummyOperator(task_id='join_branches', ...)
        
        # Set dependencies
        branch_task >> [good_task, clean_task, alert_task]
        [good_task, clean_task, alert_task] >> join_task
        """)
        
        print(example)
    
    @staticmethod
    def show_taskflow_example():
        """Show TaskFlow API (new in Airflow 2.0)"""
        
        example = textwrap.dedent("""
        # =====================================================
        # TASKFLOW API (Airflow 2.0+)
        # =====================================================
        
        from airflow.decorators import dag, task
        from datetime import datetime
        
        @dag(
            schedule='@daily',
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=['example'],
        )
        def etl_pipeline():
            
            @task
            def extract():
                # Extract logic
                return {"data": [1, 2, 3, 4, 5]}
            
            @task
            def transform(data):
                # Transform logic
                return [x * 2 for x in data]
            
            @task
            def load(transformed_data):
                # Load logic
                print(f"Loading: {transformed_data}")
                return {"status": "loaded"}
            
            # Define pipeline - dependencies are automatic
            data = extract()
            transformed = transform(data)
            load(transformed)
        
        # Instantiate the DAG
        dag = etl_pipeline()
        """)
        
        print(example)
    
    @staticmethod
    def show_error_handling():
        """Show error handling patterns"""
        
        example = textwrap.dedent("""
        # =====================================================
        # ERROR HANDLING PATTERNS
        # =====================================================
        
        # 1. Retry configuration (in default_args)
        default_args = {
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'retry_exponential': True,  # Exponential backoff
            'max_retry_delay': timedelta(hours=1),
        }
        
        # 2. Email alerts on failure
        default_args = {
            'email': ['team@company.com'],
            'email_on_failure': True,
            'email_on_retry': True,
        }
        
        # 3. Task-level retry override
        task = PythonOperator(
            task_id='critical_task',
            python_callable=my_function,
            retries=5,
            retry_delay=timedelta(minutes=2),
            dag=dag,
        )
        
        # 4. SLA monitoring
        task = PythonOperator(
            task_id='time_sensitive_task',
            python_callable=my_function,
            sla=timedelta(minutes=30),  # Must complete within 30 minutes
            dag=dag,
        )
        
        # 5. Trigger rules (what happens after failure)
        from airflow.utils.trigger_rule import TriggerRule
        
        # Continue even if upstream fails
        task = PythonOperator(
            task_id='always_run_task',
            python_callable=my_function,
            trigger_rule=TriggerRule.ALL_DONE,  # Run when upstream is done (success or fail)
            dag=dag,
        )
        
        # Only run if all upstream succeeded
        task = PythonOperator(
            task_id='dependent_task',
            python_callable=my_function,
            trigger_rule=TriggerRule.ALL_SUCCESS,  # Default
            dag=dag,
        )
        
        # Run if at least one upstream succeeded
        task = PythonOperator(
            task_id='one_success_task',
            python_callable=my_function,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            dag=dag,
        )
        """)
        
        print(example)
    
    @staticmethod
    def show_best_practices():
        """Show Airflow best practices"""
        
        practices = [
            "✅ Make tasks idempotent - can run multiple times safely",
            "✅ Keep tasks atomic - do one thing well",
            "✅ Use XCom for small metadata, not large data transfers",
            "✅ Set appropriate retries for transient failures",
            "✅ Use catchup=False for new DAGs unless backfilling needed",
            "✅ Set clear task_id names (verb_noun format: extract_data)",
            "✅ Add docstrings to DAGs and tasks",
            "✅ Use Variables and Connections for configuration",
            "✅ Test tasks independently before DAG deployment",
            "✅ Monitor task duration to establish baselines",
            "✅ Set SLA for critical tasks",
            "✅ Use tags for organization",
            "✅ Version control your DAGs",
            "✅ Start with simple DAGs, add complexity gradually",
        ]
        
        print("\n✨ Airflow Best Practices:")
        for practice in sorted(practices):
            print(f"  {practice}")

if __name__ == "__main__":
    """
    Educational demonstration of Airflow concepts
    """
    
    print("\n" + "="*60)
    print("✈️  AIRFLOW CONCEPTS DEMONSTRATION")
    print("="*60)
    print("\nThis module shows Airflow concepts without requiring installation")
    print("Understanding these patterns prepares you for Airflow development")
    
    # Show DAG structure
    print("\n" + "-"*60)
    print("1. DAG STRUCTURE")
    print("-"*60)
    AirflowConceptDemo.show_dag_structure()
    
    # Show operator types
    print("\n" + "-"*60)
    print("2. OPERATOR TYPES")
    print("-"*60)
    AirflowConceptDemo.show_operator_types()
    
    # Show schedule intervals
    print("\n" + "-"*60)
    print("3. SCHEDULE INTERVALS")
    print("-"*60)
    AirflowConceptDemo.show_schedule_intervals()
    
    # Show XCom
    print("\n" + "-"*60)
    print("4. XCOM (TASK COMMUNICATION)")
    print("-"*60)
    AirflowConceptDemo.show_xcom_example()
    
    # Show branching
    print("\n" + "-"*60)
    print("5. CONDITIONAL BRANCHING")
    print("-"*60)
    AirflowConceptDemo.show_branching_example()
    
    # Show TaskFlow
    print("\n" + "-"*60)
    print("6. TASKFLOW API (AIRFLOW 2.0+)")
    print("-"*60)
    AirflowConceptDemo.show_taskflow_example()
    
    # Show error handling
    print("\n" + "-"*60)
    print("7. ERROR HANDLING PATTERNS")
    print("-"*60)
    AirflowConceptDemo.show_error_handling()
    
    # Show best practices
    print("\n" + "-"*60)
    print("8. BEST PRACTICES")
    print("-"*60)
    AirflowConceptDemo.show_best_practices()
    
    print("\n" + "="*60)
    print("✅ AIRFLOW CONCEPTS DEMONSTRATION COMPLETE")
    print("="*60)
    print("\n📌 Next Steps:")
    print("  1. Install Airflow locally for hands-on practice")
    print("  2. Convert your ETL scripts to Airflow DAGs")
    print("  3. Practice with different operator types")
    print("  4. Experiment with branching and error handling")
