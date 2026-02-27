
"""
Pipeline Orchestrator - Coordinates the entire PowerApps ETL pipeline
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime
import logging
import json
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PowerAppsPipelineOrchestrator:
    """
    Orchestrates the complete PowerApps ETL pipeline
    """
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = base_dir
        self.pipeline_log = []
        self.start_time = None
        self.end_time = None
        
    def log_step(self, step: str, status: str, details: str = ""):
        """Log pipeline step"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = {
            'timestamp': timestamp,
            'step': step,
            'status': status,
            'details': details
        }
        self.pipeline_log.append(log_entry)
        
        status_icon = {
            'STARTED': '🚀',
            'COMPLETED': '✅',
            'FAILED': '❌',
            'SKIPPED': '⏭️'
        }.get(status, '📌')
        
        logger.info(f"{status_icon} {step}: {status} - {details}")
    
    def run_step(self, step_name: str, command: list, cwd: str = None) -> bool:
        """Run a pipeline step"""
        self.log_step(step_name, 'STARTED', f"Running: {' '.join(command)}")
        
        try:
            result = subprocess.run(
                command,
                cwd=cwd or self.base_dir,
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                self.log_step(step_name, 'COMPLETED', f"Output: {result.stdout[:200]}...")
                return True
            else:
                self.log_step(step_name, 'FAILED', f"Error: {result.stderr[:200]}")
                return False
                
        except Exception as e:
            self.log_step(step_name, 'FAILED', f"Exception: {str(e)}")
            return False
    
    def run_full_pipeline(self, days: int = 7) -> bool:
        """
        Run the complete ETL pipeline
        """
        self.start_time = datetime.now()
        
        print("\n" + "="*70)
        print("🚀 POWERAPPS ETL PIPELINE ORCHESTRATOR")
        print("="*70)
        
        # Step 1: Generate sample data
        print("\n📤 STEP 1: Generating PowerApps Export Data")
        success = self.run_step(
            "Generate Sample Data",
            [sys.executable, "export_simulator.py", "--days", str(days)]
        )
        
        if not success:
            self.log_step("Pipeline", 'FAILED', "Failed at data generation")
            return False
        
        # Step 2: Transform data
        print("\n🔄 STEP 2: Transforming Data")
        success = self.run_step(
            "Transform Data",
            [sys.executable, "transform_processor.py"]
        )
        
        if not success:
            self.log_step("Pipeline", 'FAILED', "Failed at transformation")
            return False
        
        # Step 3: Load to database
        print("\n📥 STEP 3: Loading to Database")
        success = self.run_step(
            "Load to Database",
            [sys.executable, "load_to_database.py", "--summary"]
        )
        
        if not success:
            self.log_step("Pipeline", 'FAILED', "Failed at database load")
            return False
        
        # Step 4: Generate final report
        print("\n📊 STEP 4: Generating Pipeline Report")
        self.generate_pipeline_report()
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        print("\n" + "="*70)
        print(f"✅ PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds")
        print("="*70)
        
        self.log_step("Pipeline", 'COMPLETED', f"Total duration: {duration:.2f}s")
        
        return True
    
    def generate_pipeline_report(self):
        """Generate comprehensive pipeline report"""
        
        report = {
            'pipeline_execution': {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None
            },
            'steps': self.pipeline_log
        }
        
        # Add summary statistics
        steps_completed = sum(1 for step in self.pipeline_log if step['status'] == 'COMPLETED')
        steps_failed = sum(1 for step in self.pipeline_log if step['status'] == 'FAILED')
        
        report['summary'] = {
            'total_steps': len(self.pipeline_log),
            'steps_completed': steps_completed,
            'steps_failed': steps_failed,
            'success_rate': (steps_completed / len(self.pipeline_log) * 100) if self.pipeline_log else 0
        }
        
        # Check if database exists and get record counts
        if os.path.exists('data_warehouse.db'):
            try:
                import sqlite3
                conn = sqlite3.connect('data_warehouse.db')
                cursor = conn.cursor()
                
                # Get record counts
                cursor.execute("SELECT COUNT(*) FROM opportunities")
                opp_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM customer_feedback")
                feedback_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM inventory")
                inv_count = cursor.fetchone()[0]
                
                report['database'] = {
                    'opportunities': opp_count,
                    'customer_feedback': feedback_count,
                    'inventory': inv_count,
                    'total_records': opp_count + feedback_count + inv_count
                }
                
                conn.close()
            except Exception as e:
                report['database'] = {'error': str(e)}
        
        # Save report
        report_filename = f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Pipeline report saved to {report_filename}")
        
        # Print summary
        print("\n📋 PIPELINE SUMMARY")
        print("-" * 40)
        print(f"Total Steps: {report['summary']['total_steps']}")
        print(f"Completed: {report['summary']['steps_completed']}")
        print(f"Failed: {report['summary']['steps_failed']}")
        print(f"Success Rate: {report['summary']['success_rate']:.1f}%")
        
        if 'database' in report:
            print(f"\n💾 Database Records:")
            for table, count in report['database'].items():
                if table != 'error':
                    print(f"  • {table}: {count:,}")

def main():
    parser = argparse.ArgumentParser(description='Orchestrate PowerApps ETL pipeline')
    parser.add_argument('--days', type=int, default=7, help='Days of sample data to generate')
    parser.add_argument('--skip-gen', action='store_true', help='Skip data generation')
    
    args = parser.parse_args()
    
    orchestrator = PowerAppsPipelineOrchestrator()
    
    if args.skip_gen:
        # Run from step 2
        print("\n⚠️ Skipping data generation, using existing files")
        orchestrator.run_step("Transform Data", [sys.executable, "transform_processor.py"])
        orchestrator.run_step("Load to Database", [sys.executable, "load_to_database.py", "--summary"])
        orchestrator.generate_pipeline_report()
    else:
        orchestrator.run_full_pipeline(args.days)

if __name__ == "__main__":
    main()
