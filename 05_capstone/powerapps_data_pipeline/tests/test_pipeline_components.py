"""
Unit Tests for PowerApps ETL Pipeline Components
Demonstrates test-driven development and quality assurance practices
"""

import unittest
import tempfile
import os
import sys
import json
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import shutil

# Add parent directory to path so we can import pipeline modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import pipeline modules
from export_simulator import PowerAppsExportSimulator
from transform_processor import PowerAppsDataTransformer
from load_to_database import PowerAppsDataLoader
from pipeline_orchestrator import PowerAppsPipelineOrchestrator

class TestExportSimulator(unittest.TestCase):
    """Test suite for PowerApps Export Simulator"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.simulator = PowerAppsExportSimulator(output_dir=self.test_dir)
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
    
    def test_generate_sales_opportunity(self):
        """Test that sales opportunity generation produces valid data"""
        test_date = datetime.now()
        opportunity = self.simulator.generate_sales_opportunity(test_date)
        
        # Check required fields
        required_fields = ['opportunity_id', 'name', 'customer', 'product', 
                          'amount', 'stage', 'region', 'created_date']
        for field in required_fields:
            self.assertIn(field, opportunity, f"Missing required field: {field}")
        
        # Check data types
        self.assertIsInstance(opportunity['amount'], float)
        self.assertIsInstance(opportunity['opportunity_id'], str)
        self.assertIn(opportunity['stage'], self.simulator.sales_stages)
        
        # Check date format
        self.assertEqual(opportunity['created_date'], test_date.isoformat())
    
    def test_generate_customer_feedback(self):
        """Test that customer feedback generation produces valid data"""
        test_date = datetime.now()
        feedback = self.simulator.generate_customer_feedback(test_date)
        
        # Check rating range
        self.assertIn(feedback['rating'], [1, 2, 3, 4, 5])
        
        # Check sentiment mapping (will be done in transformer)
        self.assertIn(feedback['feedback_type'], self.simulator.feedback_types)
    
    def test_generate_inventory_item(self):
        """Test that inventory item generation produces valid data"""
        test_date = datetime.now()
        item = self.simulator.generate_inventory_item(test_date)
        
        # Check quantity and status consistency
        if item['quantity'] == 0:
            self.assertEqual(item['status'], 'Out of Stock')
        elif item['quantity'] < 50:
            self.assertEqual(item['status'], 'Low Stock')
        else:
            self.assertEqual(item['status'], 'In Stock')
        
        # Check price consistency
        self.assertGreater(item['unit_price'], item['unit_cost'])
    
    def test_generate_daily_export(self):
        """Test that daily export contains all entity types"""
        test_date = datetime.now()
        export = self.simulator.generate_daily_export(test_date)
        
        # Check structure
        self.assertIn('data', export)
        self.assertIn('opportunities', export['data'])
        self.assertIn('feedback', export['data'])
        self.assertIn('inventory', export['data'])
        
        # Check record counts
        self.assertGreater(len(export['data']['opportunities']), 0)
        self.assertGreater(len(export['data']['feedback']), 0)
        self.assertGreater(len(export['data']['inventory']), 0)
    
    def test_export_file_creation(self):
        """Test that export files are created correctly"""
        self.simulator.generate_historical_exports(days=3)
        
        files = os.listdir(self.test_dir)
        self.assertEqual(len(files), 3)  # 3 days of data
        
        for filename in files:
            self.assertTrue(filename.startswith('powerapps_export_'))
            self.assertTrue(filename.endswith('.json'))
            
            # Verify file can be read
            with open(os.path.join(self.test_dir, filename), 'r') as f:
                data = json.load(f)
                self.assertIn('export_date', data)
                self.assertIn('data', data)

class TestDataTransformer(unittest.TestCase):
    """Test suite for PowerApps Data Transformer"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, 'input')
        self.output_dir = os.path.join(self.test_dir, 'output')
        os.makedirs(self.input_dir)
        
        self.transformer = PowerAppsDataTransformer(
            input_dir=self.input_dir,
            output_dir=self.output_dir
        )
        
        # Create sample test data
        self.create_test_data()
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
    
    def create_test_data(self):
        """Create sample data for testing"""
        test_data = {
            'export_date': '2024-01-15',
            'data': {
                'opportunities': [
                    {
                        'opportunity_id': 'TEST001',
                        'name': 'Test Opp',
                        'customer': 'Test Corp',
                        'product': 'Laptop',
                        'amount': 50000.0,
                        'probability': 80,
                        'stage': 'Negotiation',
                        'region': 'North America',
                        'sales_rep': 'test@email.com',
                        'created_date': '2024-01-15',
                        'close_date': '2024-03-15',
                        'actual_revenue': 0,
                        'notes': 'Test note'
                    }
                ],
                'feedback': [
                    {
                        'feedback_id': 'FDBK001',
                        'customer': 'Test Corp',
                        'feedback_type': 'Product',
                        'rating': 4,
                        'comment': 'Great product',
                        'submitted_date': '2024-01-15',
                        'responded': True,
                        'response_days': 2,
                        'source': 'Web'
                    }
                ],
                'inventory': [
                    {
                        'item_id': 'INV001',
                        'sku': 'SKU001',
                        'product': 'Laptop',
                        'category': 'Hardware',
                        'quantity': 100,
                        'status': 'In Stock',
                        'location': 'Warehouse A',
                        'reorder_point': 20,
                        'unit_cost': 800.0,
                        'unit_price': 1200.0,
                        'last_updated': '2024-01-15',
                        'supplier': 'Supplier 1',
                        'lead_time_days': 5
                    }
                ]
            }
        }
        
        self.test_file = os.path.join(self.input_dir, 'test_export.json')
        with open(self.test_file, 'w') as f:
            json.dump(test_data, f)
    
    def test_load_export_file(self):
        """Test loading export file"""
        data = self.transformer.load_export_file(self.test_file)
        self.assertIn('export_date', data)
        self.assertIn('data', data)
    
    def test_transform_opportunities(self):
        """Test opportunities transformation"""
        # Load test data
        data = self.transformer.load_export_file(self.test_file)
        df = pd.DataFrame(data['data']['opportunities'])
        
        # Transform
        transformed = self.transformer.transform_opportunities(df)
        
        # Check derived fields
        self.assertIn('weighted_amount', transformed.columns)
        self.assertIn('deal_size', transformed.columns)
        self.assertIn('days_to_close', transformed.columns)
        
        # Check calculations
        self.assertEqual(transformed.iloc[0]['weighted_amount'], 40000.0)  # 50000 * 0.8
        self.assertEqual(transformed.iloc[0]['deal_size'], 'Medium')  # 50000 is Medium
        
        # Check date conversion
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed['created_date']))
    
    def test_transform_feedback(self):
        """Test feedback transformation"""
        data = self.transformer.load_export_file(self.test_file)
        df = pd.DataFrame(data['data']['feedback'])
        
        transformed = self.transformer.transform_feedback(df)
        
        # Check sentiment mapping
        self.assertIn('sentiment', transformed.columns)
        self.assertEqual(transformed.iloc[0]['sentiment'], 'Positive')  # Rating 4 = Positive
        
        # Check response tracking
        self.assertTrue(transformed.iloc[0]['responded_within_2days'])
    
    def test_transform_inventory(self):
        """Test inventory transformation"""
        data = self.transformer.load_export_file(self.test_file)
        df = pd.DataFrame(data['data']['inventory'])
        
        transformed = self.transformer.transform_inventory(df)
        
        # Check calculated fields
        self.assertIn('inventory_value', transformed.columns)
        self.assertIn('margin', transformed.columns)
        self.assertIn('margin_percent', transformed.columns)
        
        # Check calculations
        self.assertEqual(transformed.iloc[0]['inventory_value'], 80000.0)  # 100 * 800
        self.assertEqual(transformed.iloc[0]['margin'], 400.0)  # 1200 - 800
        self.assertAlmostEqual(transformed.iloc[0]['margin_percent'], 33.33, places=2)
        
        # Check reorder flag
        self.assertFalse(transformed.iloc[0]['needs_reorder'])  # 100 > 20
    
    def test_generate_quality_report(self):
        """Test quality report generation"""
        data = self.transformer.load_export_file(self.test_file)
        original_df = pd.DataFrame(data['data']['opportunities'])
        transformed_df = self.transformer.transform_opportunities(original_df)
        
        report = self.transformer.generate_quality_report(
            'opportunities', original_df, transformed_df, '2024-01-15'
        )
        
        self.assertIn('quality_score', report)
        self.assertIn('columns_added', report)
        self.assertIn('null_counts_before', report)
        
        # Quality score should be high for clean test data
        self.assertGreater(report['quality_score'], 90)

class TestDataLoader(unittest.TestCase):
    """Test suite for PowerApps Data Loader"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test.db')
        self.processed_dir = os.path.join(self.test_dir, 'processed')
        os.makedirs(self.processed_dir)
        
        self.loader = PowerAppsDataLoader(
            db_path=self.db_path,
            processed_dir=self.processed_dir
        )
        
        # Create test transformed data
        self.create_test_parquet()
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir)
    
    def create_test_parquet(self):
        """Create sample parquet files for testing"""
        # Opportunities test data
        opp_df = pd.DataFrame({
            'opportunity_id': ['OPP001', 'OPP002'],
            'name': ['Test Opp 1', 'Test Opp 2'],
            'customer': ['Customer A', 'Customer B'],
            'product': ['Laptop', 'Server'],
            'amount': [50000.0, 150000.0],
            'probability': [80, 60],
            'stage': ['Negotiation', 'Proposal'],
            'region': ['North America', 'EMEA'],
            'sales_rep': ['rep1@test.com', 'rep2@test.com'],
            'created_date': ['2024-01-15', '2024-01-16'],
            'close_date': ['2024-03-15', '2024-04-16'],
            'actual_revenue': [0, 0],
            'notes': ['', ''],
            'weighted_amount': [40000.0, 90000.0],
            'days_to_close': [60, 91],
            'deal_size': ['Medium', 'Large'],
            'high_value': [0, 1],
            'created_month': ['2024-01', '2024-01'],
            'created_year': [2024, 2024]
        })
        opp_df.to_parquet(os.path.join(self.processed_dir, 'transformed_opportunities_20240115.parquet'))
        
        # Feedback test data
        fb_df = pd.DataFrame({
            'feedback_id': ['FB001', 'FB002'],
            'customer': ['Customer A', 'Customer B'],
            'feedback_type': ['Product', 'Service'],
            'rating': [4, 5],
            'comment': ['Good', 'Excellent'],
            'submitted_date': ['2024-01-15', '2024-01-16'],
            'responded': [1, 1],
            'response_days': [2, 1],
            'source': ['Web', 'Email'],
            'sentiment': ['Positive', 'Positive'],
            'has_comment': [1, 1],
            'responded_within_2days': [1, 1],
            'submitted_month': ['2024-01', '2024-01']
        })
        fb_df.to_parquet(os.path.join(self.processed_dir, 'transformed_feedback_20240115.parquet'))
    
    def test_connect_and_create_tables(self):
        """Test database connection and table creation"""
        conn = self.loader.connect()
        self.loader.create_tables()
        
        # Check that tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['opportunities', 'customer_feedback', 'inventory', 'load_history', 'sales_summary']
        for table in expected_tables:
            self.assertIn(table, tables)
        
        conn.close()
    
    def test_load_opportunities(self):
        """Test loading opportunities data"""
        self.loader.connect()
        self.loader.create_tables()
        
        # Load test data
        df = pd.read_parquet(os.path.join(self.processed_dir, 'transformed_opportunities_20240115.parquet'))
        records_loaded = self.loader.load_opportunities(df, 'test_file.parquet')
        
        self.assertEqual(records_loaded, 2)
        
        # Verify data was loaded
        cursor = self.loader.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM opportunities")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
    
    def test_load_feedback(self):
        """Test loading feedback data"""
        self.loader.connect()
        self.loader.create_tables()
        
        df = pd.read_parquet(os.path.join(self.processed_dir, 'transformed_feedback_20240115.parquet'))
        records_loaded = self.loader.load_feedback(df, 'test_file.parquet')
        
        self.assertEqual(records_loaded, 2)
        
        cursor = self.loader.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customer_feedback")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
    
    def test_load_history_tracking(self):
        """Test that load history is tracked"""
        self.loader.connect()
        self.loader.create_tables()
        
        # Load some data
        df = pd.read_parquet(os.path.join(self.processed_dir, 'transformed_opportunities_20240115.parquet'))
        self.loader.load_opportunities(df, 'test_file.parquet')
        
        # Check history
        cursor = self.loader.conn.cursor()
        cursor.execute("SELECT * FROM load_history")
        history = cursor.fetchall()
        
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0][3], 2)  # records_loaded
        
    def test_generate_sales_summary(self):
        """Test sales summary generation"""
        self.loader.connect()
        self.loader.create_tables()
        
        # Load test data
        df = pd.read_parquet(os.path.join(self.processed_dir, 'transformed_opportunities_20240115.parquet'))
        self.loader.load_opportunities(df, 'test_file.parquet')
        
        # Generate summary
        self.loader.generate_sales_summary()
        
        # Check summary table
        cursor = self.loader.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sales_summary")
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0)

class TestPipelineOrchestrator(unittest.TestCase):
    """Test suite for Pipeline Orchestrator"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.test_dir)
        
        # Create minimal pipeline files for testing
        self.create_mock_pipeline_files()
        
        self.orchestrator = PowerAppsPipelineOrchestrator(base_dir=self.test_dir)
    
    def tearDown(self):
        """Clean up test fixtures"""
        os.chdir(self.original_dir)
        shutil.rmtree(self.test_dir)
    
    def create_mock_pipeline_files(self):
        """Create mock pipeline Python files"""
        # Mock export_simulator.py
        with open('export_simulator.py', 'w') as f:
            f.write("""
import argparse
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=7)
    args = parser.parse_args()
    print(f"Generated {args.days} days of data")
if __name__ == "__main__":
    main()
""")
        
        # Mock transform_processor.py
        with open('transform_processor.py', 'w') as f:
            f.write("""
import argparse
def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    print("Transformed data successfully")
if __name__ == "__main__":
    main()
""")
        
        # Mock load_to_database.py
        with open('load_to_database.py', 'w') as f:
            f.write("""
import argparse
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--summary', action='store_true')
    args = parser.parse_args()
    print("Loaded data to database")
if __name__ == "__main__":
    main()
""")
    
    def test_log_step(self):
        """Test step logging"""
        self.orchestrator.log_step('Test Step', 'COMPLETED', 'Test details')
        
        self.assertEqual(len(self.orchestrator.pipeline_log), 1)
        self.assertEqual(self.orchestrator.pipeline_log[0]['step'], 'Test Step')
        self.assertEqual(self.orchestrator.pipeline_log[0]['status'], 'COMPLETED')
    
    def test_run_step_success(self):
        """Test running a successful step"""
        success = self.orchestrator.run_step(
            'Test Step',
            [sys.executable, '-c', 'print("test")']
        )
        
        self.assertTrue(success)
        self.assertEqual(len(self.orchestrator.pipeline_log), 2)  # STARTED + COMPLETED
    
    def test_run_step_failure(self):
        """Test running a failing step"""
        success = self.orchestrator.run_step(
            'Test Step',
            [sys.executable, '-c', 'raise Exception("Test error")']
        )
        
        self.assertFalse(success)
        self.assertEqual(len(self.orchestrator.pipeline_log), 2)  # STARTED + FAILED
    
    def test_generate_pipeline_report(self):
        """Test pipeline report generation"""
        # Add some log entries
        self.orchestrator.log_step('Step 1', 'COMPLETED', 'Details 1')
        self.orchestrator.log_step('Step 2', 'COMPLETED', 'Details 2')
        self.orchestrator.start_time = datetime.now()
        self.orchestrator.end_time = datetime.now()
        
        self.orchestrator.generate_pipeline_report()
        
        # Check that report file was created
        report_files = [f for f in os.listdir('.') if f.startswith('pipeline_report_')]
        self.assertEqual(len(report_files), 1)

class TestDataIntegrity(unittest.TestCase):
    """Integration tests for data integrity across pipeline"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.test_dir)
        
        # Set up directories
        os.makedirs('sample_exports')
        os.makedirs('processed_data')
    
    def tearDown(self):
        """Clean up integration test environment"""
        os.chdir(self.original_dir)
        shutil.rmtree(self.test_dir)
    
    def create_real_pipeline_files(self):
        """Copy real pipeline files to test directory"""
        # This is a simplified version - in reality, you'd copy the actual files
        # For now, we'll use the mock files from previous test
        pass
    
    def test_end_to_end_data_flow(self):
        """Test that data flows correctly through the pipeline"""
        # This would be a full integration test
        # For now, we'll test individual components with real data
        
        # 1. Generate sample data
        simulator = PowerAppsExportSimulator(output_dir='sample_exports')
        simulator.generate_historical_exports(days=2)
        
        # Check that files were created
        export_files = os.listdir('sample_exports')
        self.assertEqual(len(export_files), 2)
        
        # 2. Transform data
        transformer = PowerAppsDataTransformer(
            input_dir='sample_exports',
            output_dir='processed_data'
        )
        results = transformer.process_all()
        
        self.assertEqual(len(results), 2)  # Processed 2 files
        
        # Check that parquet files were created
        parquet_files = os.listdir('processed_data')
        self.assertGreater(len(parquet_files), 0)
        
        # 3. Load to database
        loader = PowerAppsDataLoader(
            db_path='test_integration.db',
            processed_dir='processed_data'
        )
        loader.connect()
        loader.create_tables()
        
        # Load all files
        parquet_files = [f for f in os.listdir('processed_data') if f.endswith('.parquet')]
        for file in parquet_files:
            df = pd.read_parquet(os.path.join('processed_data', file))
            if 'opportunities' in file:
                loader.load_opportunities(df, file)
            elif 'feedback' in file:
                loader.load_feedback(df, file)
        
        # Verify data was loaded
        cursor = loader.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM opportunities")
        opp_count = cursor.fetchone()[0]
        self.assertGreater(opp_count, 0)
        
        cursor.execute("SELECT COUNT(*) FROM customer_feedback")
        fb_count = cursor.fetchone()[0]
        self.assertGreater(fb_count, 0)
        
        loader.conn.close()

def run_tests():
    """Run all tests with verbose output"""
    print("\n" + "="*60)
    print("🧪 Running PowerApps Pipeline Test Suite")
    print("="*60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestExportSimulator))
    suite.addTests(loader.loadTestsFromTestCase(TestDataTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestDataLoader))
    suite.addTests(loader.loadTestsFromTestCase(TestPipelineOrchestrator))
    suite.addTests(loader.loadTestsFromTestCase(TestDataIntegrity))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*60)
    print(f"✅ Tests Run: {result.testsRun}")
    print(f"✅ Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"⚠️ Errors: {len(result.errors)}")
    print("="*60)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    # Run tests when script is executed directly
    success = run_tests()
    
    # Create a test report file
    report = {
        'timestamp': datetime.now().isoformat(),
        'success': success,
        'test_modules': [
            'TestExportSimulator',
            'TestDataTransformer', 
            'TestDataLoader',
            'TestPipelineOrchestrator',
            'TestDataIntegrity'
        ]
    }
    
    with open('test_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📊 Test report saved to test_report.json")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)
