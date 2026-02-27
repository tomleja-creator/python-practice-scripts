
"""
Data Transformation Processor
Cleans, validates, and enriches PowerApps export data
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
import argparse
from typing import Dict, List, Any, Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PowerAppsDataTransformer:
    """
    Transforms raw PowerApps exports into clean, analysis-ready data
    """
    
    def __init__(self, input_dir: str = "sample_exports", output_dir: str = "processed_data"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.quality_reports = []
        
    def load_export_file(self, filepath: str) -> Dict:
        """Load a single export file"""
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def transform_opportunities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and enrich opportunities data"""
        
        # Make a copy to avoid warnings
        df_clean = df.copy()
        
        # 1. Remove any completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        # 2. Convert date columns
        df_clean['created_date'] = pd.to_datetime(df_clean['created_date'])
        df_clean['close_date'] = pd.to_datetime(df_clean['close_date'])
        df_clean['last_modified'] = pd.to_datetime(df_clean['last_modified'])
        
        # 3. Calculate derived fields
        df_clean['days_to_close'] = (df_clean['close_date'] - df_clean['created_date']).dt.days
        
        # 4. Calculate weighted amount (probability * amount)
        df_clean['weighted_amount'] = df_clean['amount'] * (df_clean['probability'] / 100)
        
        # 5. Create month/year for grouping
        df_clean['created_month'] = df_clean['created_date'].dt.to_period('M')
        df_clean['created_year'] = df_clean['created_date'].dt.year
        df_clean['created_month_name'] = df_clean['created_date'].dt.strftime('%B')
        
        # 6. Categorize deal size
        conditions = [
            df_clean['amount'] < 50000,
            df_clean['amount'] < 100000,
            df_clean['amount'] < 250000,
            df_clean['amount'] >= 250000
        ]
        choices = ['Small', 'Medium', 'Large', 'Enterprise']
        df_clean['deal_size'] = np.select(conditions, choices, default='Unknown')
        
        # 7. Flag high-value opportunities
        df_clean['high_value'] = df_clean['amount'] > 100000
        
        # 8. Clean text fields
        df_clean['notes'] = df_clean['notes'].fillna('').astype(str).str.strip()
        df_clean['customer'] = df_clean['customer'].str.strip()
        df_clean['product'] = df_clean['product'].str.strip()
        
        return df_clean
    
    def transform_feedback(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and enrich feedback data"""
        
        df_clean = df.copy()
        
        # 1. Convert dates
        df_clean['submitted_date'] = pd.to_datetime(df_clean['submitted_date'])
        
        # 2. Handle missing comments
        df_clean['has_comment'] = df_clean['comment'].notna() & (df_clean['comment'] != '')
        df_clean['comment'] = df_clean['comment'].fillna('').astype(str).str.strip()
        
        # 3. Categorize sentiment based on rating
        conditions = [
            df_clean['rating'] <= 2,
            df_clean['rating'] == 3,
            df_clean['rating'] >= 4
        ]
        choices = ['Negative', 'Neutral', 'Positive']
        df_clean['sentiment'] = np.select(conditions, choices, default='Unknown')
        
        # 4. Calculate response metrics
        df_clean['response_days'] = pd.to_numeric(df_clean['response_days'], errors='coerce')
        df_clean['responded_within_2days'] = df_clean['response_days'] <= 2
        
        # 5. Month/year for grouping
        df_clean['submitted_month'] = df_clean['submitted_date'].dt.to_period('M')
        
        return df_clean
    
    def transform_inventory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and enrich inventory data"""
        
        df_clean = df.copy()
        
        # 1. Convert dates
        df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated'])
        
        # 2. Calculate inventory value
        df_clean['inventory_value'] = df_clean['quantity'] * df_clean['unit_cost']
        df_clean['potential_revenue'] = df_clean['quantity'] * df_clean['unit_price']
        df_clean['margin'] = df_clean['unit_price'] - df_clean['unit_cost']
        df_clean['margin_percent'] = (df_clean['margin'] / df_clean['unit_price'] * 100).round(2)
        
        # 3. Flag items needing reorder
        df_clean['needs_reorder'] = df_clean['quantity'] <= df_clean['reorder_point']
        
        # 4. Inventory health score (0-100)
        df_clean['health_score'] = np.where(
            df_clean['status'] == 'In Stock',
            100,
            np.where(
                df_clean['status'] == 'Low Stock',
                50,
                np.where(
                    df_clean['status'] == 'On Order',
                25,
                0
                )
            )
        )
        
        # 5. Calculate turnover category
        df_clean['turnover_category'] = np.where(
            df_clean['quantity'] > df_clean['reorder_point'] * 3,
            'High',
            np.where(
                df_clean['quantity'] > df_clean['reorder_point'],
                'Medium',
                'Low'
            )
        )
        
        return df_clean
    
    def generate_quality_report(self, entity_name: str, original_df: pd.DataFrame, 
                              transformed_df: pd.DataFrame, export_date: str) -> Dict:
        """Generate data quality report for transformation"""
        
        report = {
            'entity': entity_name,
            'export_date': export_date,
            'timestamp': datetime.now().isoformat(),
            'original_row_count': len(original_df),
            'transformed_row_count': len(transformed_df),
            'columns_added': list(set(transformed_df.columns) - set(original_df.columns)),
            'null_counts_before': original_df.isnull().sum().to_dict(),
            'null_counts_after': transformed_df.isnull().sum().to_dict(),
            'data_quality_score': 100
        }
        
        # Calculate quality score
        if len(original_df) > 0:
            # Check for missing data
            missing_pct = transformed_df.isnull().sum().sum() / (len(transformed_df) * len(transformed_df.columns))
            report['data_quality_score'] -= missing_pct * 50
            
            # Check for duplicates
            duplicate_pct = transformed_df.duplicated().sum() / len(transformed_df)
            report['data_quality_score'] -= duplicate_pct * 30
        
        report['data_quality_score'] = max(0, round(report['data_quality_score'], 2))
        
        return report
    
    def process_file(self, filepath: str) -> Dict[str, Any]:
        """Process a single export file"""
        
        logger.info(f"Processing: {os.path.basename(filepath)}")
        
        # Load data
        export_data = self.load_export_file(filepath)
        export_date = export_data['export_date']
        
        results = {
            'export_date': export_date,
            'source_file': os.path.basename(filepath),
            'transformed': {}
        }
        
        # Process each entity type
        for entity, data in export_data['data'].items():
            if not data:
                logger.warning(f"  No data for {entity}")
                continue
            
            original_df = pd.DataFrame(data)
            logger.info(f"  {entity}: {len(original_df)} records")
            
            # Apply appropriate transformation
            if entity == 'opportunities':
                transformed_df = self.transform_opportunities(original_df)
            elif entity == 'feedback':
                transformed_df = self.transform_feedback(original_df)
            elif entity == 'inventory':
                transformed_df = self.transform_inventory(original_df)
            else:
                transformed_df = original_df
            
            # Save transformed data
            output_filename = f"transformed_{entity}_{export_date}.parquet"
            output_path = os.path.join(self.output_dir, output_filename)
            transformed_df.to_parquet(output_path, index=False)
            
            # Generate quality report
            report = self.generate_quality_report(entity, original_df, transformed_df, export_date)
            self.quality_reports.append(report)
            
            results['transformed'][entity] = {
                'records': len(transformed_df),
                'output_file': output_filename,
                'quality_score': report['data_quality_score']
            }
            
            logger.info(f"  ✓ Saved {len(transformed_df)} transformed records")
        
        return results
    
    def process_all(self) -> List[Dict]:
        """Process all export files in input directory"""
        
        results = []
        files = sorted([f for f in os.listdir(self.input_dir) if f.endswith('.json')])
        
        logger.info(f"Found {len(files)} files to process")
        
        for filename in files:
            filepath = os.path.join(self.input_dir, filename)
            result = self.process_file(filepath)
            results.append(result)
        
        # Save combined quality report
        report_path = os.path.join(self.output_dir, f"quality_report_{datetime.now().strftime('%Y%m%d')}.json")
        with open(report_path, 'w') as f:
            json.dump({
                'summary': {
                    'files_processed': len(results),
                    'total_quality_reports': len(self.quality_reports),
                    'average_quality_score': np.mean([r['data_quality_score'] for r in self.quality_reports])
                },
                'reports': self.quality_reports
            }, f, indent=2, default=str)
        
        logger.info(f"Saved quality report to {report_path}")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Transform PowerApps export data')
    parser.add_argument('--input', type=str, default='sample_exports', help='Input directory')
    parser.add_argument('--output', type=str, default='processed_data', help='Output directory')
    parser.add_argument('--file', type=str, help='Process single file (optional)')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🔄 PowerApps Data Transformer")
    print("="*60)
    
    transformer = PowerAppsDataTransformer(args.input, args.output)
    
    if args.file:
        results = transformer.process_file(os.path.join(args.input, args.file))
        print(f"\n✅ Processed: {args.file}")
    else:
        results = transformer.process_all()
        print(f"\n✅ Processed {len(results)} files")
    
    print("\n📊 Quality Summary:")
    for report in transformer.quality_reports[-5:]:  # Show last 5
        print(f"  • {report['entity']} ({report['export_date']}): Score {report['data_quality_score']}")

if __name__ == "__main__":
    main()
