## 📄 File 2: export_simulator.py

```python
"""
PowerApps Export Simulator
Simulates exporting data from PowerApps via REST API
Matches real PowerApps data structures
"""

import json
import random
from datetime import datetime, timedelta
import os
import argparse
from typing import List, Dict, Any
import uuid

class PowerAppsExportSimulator:
    """
    Simulates PowerApps data exports
    Creates realistic sample data matching PowerApps Common Data Service structure
    """
    
    def __init__(self, output_dir: str = "sample_exports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Sample data domains
        self.sales_stages = ['Prospecting', 'Qualification', 'Needs Analysis', 
                            'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']
        self.products = ['Laptop Pro', 'Desktop Elite', 'Server X1', 'Storage Array', 
                        'Network Switch', 'Software License', 'Consulting Hours']
        self.regions = ['North America', 'EMEA', 'Asia Pacific', 'Latin America']
        self.customers = [
            'Acme Corp', 'Globex Inc', 'Initech', 'Umbrella Corp', 
            'Stark Industries', 'Wayne Enterprises', 'Oscorp', 'Cyberdyne Systems'
        ]
        
    def generate_sales_opportunity(self, date: datetime) -> Dict[str, Any]:
        """Generate a single sales opportunity record (like PowerApps Sales table)"""
        
        # Randomly determine if won/lost for closed opportunities
        stage = random.choice(self.sales_stages)
        amount = round(random.uniform(10000, 500000), 2)
        
        if stage == 'Closed Won':
            probability = 100
            actual_revenue = amount
        elif stage == 'Closed Lost':
            probability = 0
            actual_revenue = 0
        else:
            probability = random.randint(10, 90)
            actual_revenue = 0
        
        return {
            'opportunity_id': str(uuid.uuid4())[:8],
            'name': f"Opportunity {random.randint(1000, 9999)}",
            'customer': random.choice(self.customers),
            'product': random.choice(self.products),
            'amount': amount,
            'probability': probability,
            'stage': stage,
            'region': random.choice(self.regions),
            'sales_rep': f"rep{random.randint(1, 20)}@company.com",
            'created_date': date.isoformat(),
            'close_date': (date + timedelta(days=random.randint(30, 180))).isoformat(),
            'actual_revenue': actual_revenue,
            'notes': f"Sample opportunity for {random.choice(self.products)}",
            'last_modified': datetime.now().isoformat()
        }
    
    def generate_customer_feedback(self, date: datetime) -> Dict[str, Any]:
        """Generate customer feedback records (like PowerApps Feedback form)"""
        
        ratings = [1, 2, 3, 4, 5]
        feedback_types = ['Product', 'Service', 'Support', 'General']
        
        return {
            'feedback_id': str(uuid.uuid4())[:8],
            'customer': random.choice(self.customers),
            'feedback_type': random.choice(feedback_types),
            'rating': random.choices(ratings, weights=[0.05, 0.1, 0.2, 0.3, 0.35])[0],
            'comment': random.choice([
                "Great product, very satisfied",
                "Support response time could be better",
                "Excellent service, will recommend",
                "Need more documentation",
                "Perfect solution for our needs",
                "",
                None
            ]),
            'submitted_date': date.isoformat(),
            'responded': random.choice([True, False]),
            'response_days': random.randint(0, 5) if random.choice([True, False]) else None,
            'source': random.choice(['Web', 'Mobile', 'Email'])
        }
    
    def generate_inventory_item(self, date: datetime) -> Dict[str, Any]:
        """Generate inventory records (like PowerApps Inventory app)"""
        
        locations = ['Warehouse A', 'Warehouse B', 'Distribution Center', 'Retail Store']
        statuses = ['In Stock', 'Low Stock', 'Out of Stock', 'On Order']
        
        quantity = random.randint(0, 500)
        if quantity == 0:
            status = 'Out of Stock'
        elif quantity < 50:
            status = 'Low Stock'
        else:
            status = 'In Stock'
        
        return {
            'item_id': str(uuid.uuid4())[:8],
            'sku': f"SKU-{random.randint(10000, 99999)}",
            'product': random.choice(self.products),
            'category': random.choice(['Hardware', 'Software', 'Accessories']),
            'quantity': quantity,
            'status': status,
            'location': random.choice(locations),
            'reorder_point': random.randint(25, 100),
            'unit_cost': round(random.uniform(10, 2000), 2),
            'unit_price': round(random.uniform(20, 4000), 2),
            'last_updated': date.isoformat(),
            'supplier': f"Supplier {random.randint(1, 10)}",
            'lead_time_days': random.randint(3, 30)
        }
    
    def generate_daily_export(self, date: datetime) -> Dict[str, Any]:
        """Generate a complete export for one day with all record types"""
        
        # Generate varying numbers of records per day
        num_opportunities = random.randint(50, 200)
        num_feedback = random.randint(20, 100)
        num_inventory = random.randint(100, 300)
        
        export = {
            'export_date': date.isoformat(),
            'export_timestamp': datetime.now().isoformat(),
            'source_system': 'PowerApps',
            'environment': 'Production',
            'record_counts': {
                'opportunities': num_opportunities,
                'feedback': num_feedback,
                'inventory': num_inventory
            },
            'data': {
                'opportunities': [self.generate_sales_opportunity(date) for _ in range(num_opportunities)],
                'feedback': [self.generate_customer_feedback(date) for _ in range(num_feedback)],
                'inventory': [self.generate_inventory_item(date) for _ in range(num_inventory)]
            }
        }
        
        return export
    
    def generate_historical_exports(self, days: int = 30):
        """Generate exports for multiple days"""
        
        end_date = datetime.now()
        
        for i in range(days):
            export_date = end_date - timedelta(days=i)
            export_data = self.generate_daily_export(export_date)
            
            filename = f"powerapps_export_{export_date.strftime('%Y%m%d')}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            print(f"✅ Generated: {filename} - {export_data['record_counts']}")
    
    def simulate_api_export(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Simulate API call to PowerApps export endpoint
        Returns data for date range
        """
        all_exports = []
        current_date = start_date
        
        while current_date <= end_date:
            export = self.generate_daily_export(current_date)
            all_exports.append(export)
            current_date += timedelta(days=1)
        
        return all_exports

def main():
    parser = argparse.ArgumentParser(description='Generate sample PowerApps export data')
    parser.add_argument('--days', type=int, default=7, help='Number of days of data to generate')
    parser.add_argument('--output', type=str, default='sample_exports', help='Output directory')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("📤 PowerApps Export Simulator")
    print("="*60)
    
    simulator = PowerAppsExportSimulator(args.output)
    simulator.generate_historical_exports(args.days)
    
    print("\n" + "="*60)
    print(f"✅ Generated {args.days} days of sample data in '{args.output}/'")
    print("="*60)

if __name__ == "__main__":
    main()
