"""
ETL Pipeline Demo - Shows understanding of Extract, Transform, Load concepts
Prepares for Airflow orchestration
"""

import json
import csv
from datetime import datetime

class SimpleETLPipeline:
    """Demonstrates ETL pattern that Airflow would orchestrate"""
    
    def __init__(self, source_file, target_file):
        self.source = source_file
        self.target = target_file
        self.log = []
    
    def extract(self):
        """EXTRACT phase - Read from source"""
        print("📤 EXTRACT: Reading data...")
        data = []
        try:
            if self.source.endswith('.csv'):
                with open(self.source, 'r') as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
            elif self.source.endswith('.json'):
                with open(self.source, 'r') as f:
                    data = json.load(f)
            else:
                raise ValueError(f"Unsupported file type: {self.source}")
            
            self.log.append(f"Extracted {len(data)} records at {datetime.now()}")
            print(f"   ✅ Extracted {len(data)} records")
            return data
        except Exception as e:
            self.log.append(f"❌ Extraction failed: {e}")
            raise
    
    def transform(self, data):
        """TRANSFORM phase - Clean and enrich"""
        print("🔄 TRANSFORM: Processing data...")
        transformed = []
        
        for record in data:
            # Example transformations
            # 1. Standardize field names
            cleaned = {k.lower().strip(): v for k, v in record.items()}
            
            # 2. Handle missing values
            for key in cleaned:
                if cleaned[key] in ('', 'null', 'None'):
                    cleaned[key] = None
            
            # 3. Add metadata
            cleaned['processed_date'] = datetime.now().isoformat()
            cleaned['data_source'] = self.source
            
            transformed.append(cleaned)
        
        self.log.append(f"Transformed {len(transformed)} records at {datetime.now()}")
        print(f"   ✅ Transformed {len(transformed)} records")
        return transformed
    
    def load(self, data):
        """LOAD phase - Write to target"""
        print("📥 LOAD: Writing data...")
        
        try:
            with open(self.target, 'w') as f:
                if self.target.endswith('.json'):
                    json.dump(data, f, indent=2)
                else:
                    # Default to CSV
                    if data:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
            
            self.log.append(f"Loaded {len(data)} records to {self.target} at {datetime.now()}")
            print(f"   ✅ Loaded {len(data)} records to {self.target}")
        except Exception as e:
            self.log.append(f"❌ Load failed: {e}")
            raise
    
    def run_pipeline(self):
        """Run the complete ETL pipeline"""
        print("\n" + "="*50)
        print("🚀 Running ETL Pipeline")
        print("="*50)
        
        try:
            data = self.extract()
            transformed = self.transform(data)
            self.load(transformed)
            
            print("\n✅ Pipeline completed successfully!")
            print(f"📋 Log entries: {len(self.log)}")
            return True
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            return False

def create_test_data():
    """Create test data for the pipeline"""
    test_json = [
        {"Name": "Product A", "Price": "29.99", "Category": "Electronics"},
        {"Name": "Product B", "Price": "", "Category": "Office"},  # Empty price
        {"Name": "Product C", "Price": "49.50", "Category": "null"},  # Null category
    ]
    
    with open('test_data.json', 'w') as f:
        json.dump(test_json, f, indent=2)
    print("✅ Created test_data.json")

if __name__ == "__main__":
    # Create test data if needed
    import os
    if not os.path.exists('test_data.json'):
        create_test_data()
    
    # Run the pipeline
    pipeline = SimpleETLPipeline('test_data.json', 'output_data.json')
    pipeline.run_pipeline()
    
    # Show logs
    print("\n📋 Pipeline Log:")
    for entry in pipeline.log:
        print(f"   • {entry}")
