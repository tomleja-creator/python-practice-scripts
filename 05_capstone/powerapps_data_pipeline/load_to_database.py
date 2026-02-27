"""
Database Loader for Transformed PowerApps Data
Loads processed data into SQLite/PostgreSQL
"""

import pandas as pd
import sqlite3
import os
import argparse
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional
import json
import glob

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PowerAppsDataLoader:
    """
    Loads transformed PowerApps data into database
    """
    
    def __init__(self, db_path: str = "data_warehouse.db", processed_dir: str = "processed_data"):
        self.db_path = db_path
        self.processed_dir = processed_dir
        self.conn = None
        self.load_history = []
        
    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {self.db_path}")
        
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        return self.conn
    
    def create_tables(self):
        """Create database tables if they don't exist"""
        
        # Opportunities table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                opportunity_id TEXT PRIMARY KEY,
                name TEXT,
                customer TEXT,
                product TEXT,
                amount REAL,
                probability INTEGER,
                stage TEXT,
                region TEXT,
                sales_rep TEXT,
                created_date TIMESTAMP,
                close_date TIMESTAMP,
                actual_revenue REAL,
                notes TEXT,
                weighted_amount REAL,
                days_to_close INTEGER,
                deal_size TEXT,
                high_value BOOLEAN,
                created_month TEXT,
                created_year INTEGER,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Feedback table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_feedback (
                feedback_id TEXT PRIMARY KEY,
                customer TEXT,
                feedback_type TEXT,
                rating INTEGER,
                comment TEXT,
                submitted_date TIMESTAMP,
                responded BOOLEAN,
                response_days INTEGER,
                source TEXT,
                sentiment TEXT,
                has_comment BOOLEAN,
                responded_within_2days BOOLEAN,
                submitted_month TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Inventory table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                item_id TEXT PRIMARY KEY,
                sku TEXT,
                product TEXT,
                category TEXT,
                quantity INTEGER,
                status TEXT,
                location TEXT,
                reorder_point INTEGER,
                unit_cost REAL,
                unit_price REAL,
                last_updated TIMESTAMP,
                supplier TEXT,
                lead_time_days INTEGER,
                inventory_value REAL,
                potential_revenue REAL,
                margin REAL,
                margin_percent REAL,
                needs_reorder BOOLEAN,
                health_score INTEGER,
                turnover_category TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Load history tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS load_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_file TEXT,
                entity TEXT,
                records_loaded INTEGER,
                status TEXT
            )
        """)
        
        # Aggregated sales table for reporting
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sales_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_date TEXT,
                region TEXT,
                total_opportunities INTEGER,
                total_amount REAL,
                weighted_amount REAL,
                won_amount REAL,
                lost_amount REAL,
                win_rate REAL,
                avg_deal_size REAL,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.commit()
        logger.info("Tables created/verified")
    
    def load_opportunities(self, df: pd.DataFrame, source_file: str) -> int:
        """Load opportunities data"""
        
        # Ensure date columns are strings for SQLite
        for col in ['created_date', 'close_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert boolean to integer for SQLite
        df['high_value'] = df['high_value'].astype(int)
        
        # Insert records
        records_loaded = 0
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO opportunities (
                        opportunity_id, name, customer, product, amount, probability,
                        stage, region, sales_rep, created_date, close_date, actual_revenue,
                        notes, weighted_amount, days_to_close, deal_size, high_value,
                        created_month, created_year
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('opportunity_id'), row.get('name'), row.get('customer'),
                    row.get('product'), row.get('amount'), row.get('probability'),
                    row.get('stage'), row.get('region'), row.get('sales_rep'),
                    row.get('created_date'), row.get('close_date'), row.get('actual_revenue'),
                    row.get('notes'), row.get('weighted_amount'), row.get('days_to_close'),
                    row.get('deal_size'), row.get('high_value'),
                    str(row.get('created_month')), row.get('created_year')
                ))
                records_loaded += 1
            except Exception as e:
                logger.error(f"Error loading opportunity {row.get('opportunity_id')}: {e}")
        
        self.conn.commit()
        
        # Track load history
        cursor.execute("""
            INSERT INTO load_history (source_file, entity, records_loaded, status)
            VALUES (?, ?, ?, ?)
        """, (source_file, 'opportunities', records_loaded, 'success'))
        self.conn.commit()
        
        logger.info(f"Loaded {records_loaded} opportunities")
        return records_loaded
    
    def load_feedback(self, df: pd.DataFrame, source_file: str) -> int:
        """Load feedback data"""
        
        df['submitted_date'] = pd.to_datetime(df['submitted_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['responded'] = df['responded'].astype(int)
        df['has_comment'] = df['has_comment'].astype(int)
        df['responded_within_2days'] = df['responded_within_2days'].astype(int)
        
        records_loaded = 0
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO customer_feedback (
                        feedback_id, customer, feedback_type, rating, comment,
                        submitted_date, responded, response_days, source,
                        sentiment, has_comment, responded_within_2days, submitted_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('feedback_id'), row.get('customer'), row.get('feedback_type'),
                    row.get('rating'), row.get('comment'), row.get('submitted_date'),
                    row.get('responded'), row.get('response_days'), row.get('source'),
                    row.get('sentiment'), row.get('has_comment'),
                    row.get('responded_within_2days'), str(row.get('submitted_month'))
                ))
                records_loaded += 1
            except Exception as e:
                logger.error(f"Error loading feedback {row.get('feedback_id')}: {e}")
        
        self.conn.commit()
        
        cursor.execute("""
            INSERT INTO load_history (source_file, entity, records_loaded, status)
            VALUES (?, ?, ?, ?)
        """, (source_file, 'feedback', records_loaded, 'success'))
        self.conn.commit()
        
        logger.info(f"Loaded {records_loaded} feedback records")
        return records_loaded
    
    def load_inventory(self, df: pd.DataFrame, source_file: str) -> int:
        """Load inventory data"""
        
        df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['needs_reorder'] = df['needs_reorder'].astype(int)
        
        records_loaded = 0
        cursor = self.conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO inventory (
                        item_id, sku, product, category, quantity, status,
                        location, reorder_point, unit_cost, unit_price,
                        last_updated, supplier, lead_time_days, inventory_value,
                        potential_revenue, margin, margin_percent, needs_reorder,
                        health_score, turnover_category
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('item_id'), row.get('sku'), row.get('product'),
                    row.get('category'), row.get('quantity'), row.get('status'),
                    row.get('location'), row.get('reorder_point'), row.get('unit_cost'),
                    row.get('unit_price'), row.get('last_updated'), row.get('supplier'),
                    row.get('lead_time_days'), row.get('inventory_value'),
                    row.get('potential_revenue'), row.get('margin'),
                    row.get('margin_percent'), row.get('needs_reorder'),
                    row.get('health_score'), row.get('turnover_category')
                ))
                records_loaded += 1
            except Exception as e:
                logger.error(f"Error loading inventory {row.get('item_id')}: {e}")
        
        self.conn.commit()
        
        cursor.execute("""
            INSERT INTO load_history (source_file, entity, records_loaded, status)
            VALUES (?, ?, ?, ?)
        """, (source_file, 'inventory', records_loaded, 'success'))
        self.conn.commit()
        
        logger.info(f"Loaded {records_loaded} inventory records")
        return records_loaded
    
    def generate_sales_summary(self):
        """Generate aggregated sales summary for reporting"""
        
        cursor = self.conn.cursor()
        
        # Calculate summary by region
        cursor.execute("""
            SELECT 
                strftime('%Y-%m', created_date) as report_date,
                region,
                COUNT(*) as total_opportunities,
                SUM(amount) as total_amount,
                SUM(weighted_amount) as weighted_amount,
                SUM(CASE WHEN stage = 'Closed Won' THEN actual_revenue ELSE 0 END) as won_amount,
                SUM(CASE WHEN stage = 'Closed Lost' THEN amount ELSE 0 END) as lost_amount,
                AVG(CASE WHEN stage IN ('Closed Won', 'Closed Lost') 
                    THEN 1.0 ELSE NULL END) as win_rate,
                AVG(amount) as avg_deal_size
            FROM opportunities
            GROUP BY report_date, region
        """)
        
        summaries = cursor.fetchall()
        
        for summary in summaries:
            cursor.execute("""
                INSERT INTO sales_summary (
                    report_date, region, total_opportunities, total_amount,
                    weighted_amount, won_amount, lost_amount, win_rate, avg_deal_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, summary)
        
        self.conn.commit()
        logger.info(f"Generated {len(summaries)} sales summary records")
    
    def load_all_processed_files(self):
        """Load all transformed parquet files"""
        
        parquet_files = glob.glob(os.path.join(self.processed_dir, "transformed_*.parquet"))
        
        logger.info(f"Found {len(parquet_files)} transformed files to load")
        
        for filepath in parquet_files:
            filename = os.path.basename(filepath)
            logger.info(f"Loading: {filename}")
            
            # Parse entity type from filename
            parts = filename.replace('transformed_', '').replace('.parquet', '').split('_')
            entity = parts[0]
            date_str = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
            
            # Load parquet file
            df = pd.read_parquet(filepath)
            
            # Load based on entity type
            if entity == 'opportunities':
                self.load_opportunities(df, filename)
            elif entity == 'feedback':
                self.load_feedback(df, filename)
            elif entity == 'inventory':
                self.load_inventory(df, filename)
            else:
                logger.warning(f"Unknown entity type: {entity}")
    
    def get_load_summary(self) -> pd.DataFrame:
        """Get summary of all loads"""
        query = """
            SELECT 
                date(load_timestamp) as load_date,
                entity,
                COUNT(*) as batches,
                SUM(records_loaded) as total_records
            FROM load_history
            GROUP BY load_date, entity
            ORDER BY load_date DESC
        """
        return pd.read_sql_query(query, self.conn)

def main():
    parser = argparse.ArgumentParser(description='Load transformed data to database')
    parser.add_argument('--db', type=str, default='data_warehouse.db', help='Database path')
    parser.add_argument('--processed', type=str, default='processed_data', help='Processed data directory')
    parser.add_argument('--summary', action='store_true', help='Show load summary')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("📥 PowerApps Data Loader")
    print("="*60)
    
    loader = PowerAppsDataLoader(args.db, args.processed)
    loader.connect()
    loader.create_tables()
    loader.load_all_processed_files()
    loader.generate_sales_summary()
    
    if args.summary:
        print("\n📊 Load Summary:")
        summary = loader.get_load_summary()
        print(summary.to_string(index=False))
    
    print("\n" + "="*60)
    print("✅ Data loading complete")
    print("="*60)

if __name__ == "__main__":
    main()
