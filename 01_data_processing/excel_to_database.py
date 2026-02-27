"""
Excel to Database ETL Pipeline
Reads Excel files, transforms data, loads into SQLite database
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
import re

class ExcelToDatabase:
    """ETL pipeline for Excel to SQLite migration"""
    
    def __init__(self, db_name='data_warehouse.db'):
        self.db_name = db_name
        self.conn = None
        self.log = []
    
    def connect_db(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_name)
        self.log_event(f"Connected to database: {self.db_name}")
        return self.conn
    
    def log_event(self, message, level='INFO'):
        """Log pipeline events"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {level}: {message}"
        self.log.append(log_entry)
        print(log_entry)
    
    def extract_excel(self, file_path):
        """
        EXTRACT phase: Read Excel file
        Handles multiple sheets, different formats
        """
        self.log_event(f"Extracting data from: {file_path}")
        
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            sheets_data = {}
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                sheets_data[sheet_name] = df
                self.log_event(f"  - Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
            
            return sheets_data
            
        except Exception as e:
            self.log_event(f"Extraction failed: {e}", level='ERROR')
            raise
    
    def transform_data(self, df, sheet_name):
        """
        TRANSFORM phase: Clean and standardize data
        Applies common data quality rules
        """
        self.log_event(f"Transforming sheet: {sheet_name}")
        
        # Make a copy to avoid warnings
        df_clean = df.copy()
        
        # 1. Standardize column names (lowercase, underscores)
        df_clean.columns = [
            re.sub(r'[^a-zA-Z0-9]', '_', col.lower()).strip('_')
            for col in df_clean.columns
        ]
        
        # 2. Remove completely empty rows
        before_rows = len(df_clean)
        df_clean = df_clean.dropna(how='all')
        if before_rows > len(df_clean):
            self.log_event(f"  - Removed {before_rows - len(df_clean)} empty rows")
        
        # 3. Remove duplicate rows
        before_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        if before_rows > len(df_clean):
            self.log_event(f"  - Removed {before_rows - len(df_clean)} duplicate rows")
        
        # 4. Handle missing values in key columns
        for col in df_clean.columns:
            if df_clean[col].dtype in ['int64', 'float64']:
                # For numeric columns, fill with 0 or median
                if df_clean[col].isnull().sum() > 0:
                    median_val = df_clean[col].median()
                    df_clean[col].fillna(median_val, inplace=True)
                    self.log_event(f"  - Filled {df_clean[col].isnull().sum()} missing in '{col}' with median")
            else:
                # For text columns, fill with 'Unknown'
                if df_clean[col].isnull().sum() > 0:
                    df_clean[col].fillna('Unknown', inplace=True)
        
        # 5. Add metadata columns
        df_clean['_source_file'] = os.path.basename(file_path)
        df_clean['_source_sheet'] = sheet_name
        df_clean['_loaded_at'] = datetime.now().isoformat()
        
        self.log_event(f"  - Final shape: {len(df_clean)} rows, {len(df_clean.columns)} columns")
        
        return df_clean
    
    def load_to_db(self, df, table_name, if_exists='append'):
        """
        LOAD phase: Write transformed data to database
        Creates table if not exists
        """
        self.log_event(f"Loading to table: {table_name}")
        
        try:
            # Write to database
            df.to_sql(
                table_name, 
                self.conn, 
                if_exists=if_exists, 
                index=False,
                method='multi'  # Faster for large inserts
            )
            
            # Verify load
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            self.log_event(f"  - Table '{table_name}' now has {count} total rows")
            
        except Exception as e:
            self.log_event(f"Load failed: {e}", level='ERROR')
            raise
    
    def run_pipeline(self, excel_path, table_name=None):
        """
        Execute complete ETL pipeline
        """
        self.log_event("="*50)
        self.log_event("🚀 Starting Excel to Database Pipeline")
        self.log_event("="*50)
        
        try:
            # Connect to database
            self.connect_db()
            
            # Extract
            sheets_data = self.extract_excel(excel_path)
            
            # Transform and Load for each sheet
            for sheet_name, df in sheets_data.items():
                # Use sheet name as table name if not specified
                target_table = table_name or re.sub(r'[^a-zA-Z0-9]', '_', sheet_name.lower())
                
                # Transform
                df_transformed = self.transform_data(df, sheet_name)
                
                # Load
                self.load_to_db(df_transformed, target_table)
            
            self.log_event("="*50)
            self.log_event("✅ Pipeline completed successfully")
            self.log_event("="*50)
            
            return True
            
        except Exception as e:
            self.log_event(f"❌ Pipeline failed: {e}", level='ERROR')
            return False
        finally:
            if self.conn:
                self.conn.close()
                self.log_event("Database connection closed")
    
    def query_data(self, sql_query):
        """Utility method to query the database"""
        if not self.conn:
            self.connect_db()
        
        result = pd.read_sql_query(sql_query, self.conn)
        return result

def create_sample_excel():
    """Create sample Excel file with multiple sheets for testing"""
    # Sheet 1: Employee data
    employees = pd.DataFrame({
        'Employee ID': [1001, 1002, 1003, 1004, 1005],
        'Full Name': ['Alice Johnson', 'Bob Smith', 'Carol White', 'David Brown', None],
        'Department': ['IT', 'HR', 'IT', 'Sales', 'Sales'],
        'Salary': [75000, 65000, 82000, None, 71000],
        'Hire Date': ['2020-01-15', '2019-03-20', '2021-06-01', '2020-11-10', '2022-01-05']
    })
    
    # Sheet 2: Sales data
    sales = pd.DataFrame({
        'Order ID': [5001, 5002, 5003, 5004, 5005],
        'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Laptop'],
        'Quantity': [2, 5, 1, 3, None],
        'Unit Price': [1200.00, 25.50, 350.00, 75.00, 1200.00],
        'Order Date': ['2023-01-10', '2023-01-10', '2023-01-11', None, '2023-01-12']
    })
    
    # Write to Excel with multiple sheets
    with pd.ExcelWriter('sample_company_data.xlsx', engine='openpyxl') as writer:
        employees.to_excel(writer, sheet_name='Employees', index=False)
        sales.to_excel(writer, sheet_name='Sales', index=False)
    
    print("✅ Created sample_company_data.xlsx with Employees and Sales sheets")
    return 'sample_company_data.xlsx'

if __name__ == "__main__":
    # Create sample data
    sample_file = create_sample_excel()
    
    # Run pipeline
    pipeline = ExcelToDatabase()
    pipeline.run_pipeline(sample_file)
    
    # Query example
    print("\n" + "="*50)
    print("🔍 Sample Query Results")
    print("="*50)
    
    # Show employees by department
    result = pipeline.query_data("""
        SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    """)
    print("\n📊 Employees by Department:")
    print(result.to_string(index=False))
    
    # Show sales summary
    result = pipeline.query_data("""
        SELECT product, SUM(quantity) as total_sold, AVG(unit_price) as avg_price
        FROM sales
        GROUP BY product
    """)
    print("\n📊 Sales Summary:")
    print(result.to_string(index=False))
