"""
Simple CSV data processor - Demonstrates Python data handling skills
Created for portfolio: github.com/tomleja/python-practice-scripts
"""

import csv
from collections import Counter
import json

def process_sales_data(input_file, output_file):
    """
    Reads sales data from CSV, analyzes it, and exports results
    Mimics real ETL processes from my Power Platform experience
    """
    sales_data = []
    total_revenue = 0
    categories = []
    
    try:
        with open(input_file, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Convert string numbers to floats for calculation
                row['amount'] = float(row.get('amount', 0))
                sales_data.append(row)
                total_revenue += row['amount']
                categories.append(row.get('category', 'Unknown'))
        
        # Analyze the data
        category_counts = Counter(categories)
        avg_sale = total_revenue / len(sales_data) if sales_data else 0
        
        # Prepare results
        results = {
            'total_transactions': len(sales_data),
            'total_revenue': round(total_revenue, 2),
            'average_sale': round(avg_sale, 2),
            'top_categories': dict(category_counts.most_common(3)),
            'data_sample': sales_data[:3]  # First 3 records as sample
        }
        
        # Export to JSON (common data engineering task)
        with open(output_file, 'w') as outfile:
            json.dump(results, outfile, indent=2)
        
        print(f"✅ Successfully processed {len(sales_data)} records")
        print(f"📊 Results saved to {output_file}")
        return results
        
    except FileNotFoundError:
        print(f"❌ Error: Could not find {input_file}")
        print("💡 Tip: Create a sample CSV file first (see create_sample_data.py)")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def create_sample_data():
    """Creates a sample CSV file for testing"""
    sample_data = [
        {'date': '2024-01-15', 'category': 'Electronics', 'amount': 1250.00, 'product': 'Laptop'},
        {'date': '2024-01-16', 'category': 'Office Supplies', 'amount': 89.50, 'product': 'Printer Paper'},
        {'date': '2024-01-16', 'category': 'Electronics', 'amount': 799.99, 'product': 'Monitor'},
        {'date': '2024-01-17', 'category': 'Software', 'amount': 299.00, 'product': 'License'},
        {'date': '2024-01-18', 'category': 'Electronics', 'amount': 199.50, 'product': 'Mouse'},
    ]
    
    with open('sample_sales_data.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['date', 'category', 'amount', 'product'])
        writer.writeheader()
        writer.writerows(sample_data)
    
    print("✅ Created sample_sales_data.csv with 5 test records")

if __name__ == "__main__":
    print("📁 Python Data Processing Script")
    print("-" * 30)
    
    # Check if sample data exists, create if not
    import os
    if not os.path.exists('sample_sales_data.csv'):
        create_sample_data()
    
    # Process the data
    results = process_sales_data('sample_sales_data.csv', 'sales_analysis.json')
    
    if results:
        print("\n📈 Analysis Results:")
        print(f"   Total Revenue: ${results['total_revenue']}")
        print(f"   Average Sale: ${results['average_sale']}")
        print(f"   Top Category: {list(results['top_categories'].keys())[0]}")
