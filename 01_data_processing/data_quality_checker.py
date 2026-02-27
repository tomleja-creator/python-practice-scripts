"""
Data Quality Checker - Validates CSV/Excel data quality
Flags missing values, duplicates, outliers, and data type issues
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

class DataQualityChecker:
    """Comprehensive data quality validation tool"""
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
        self.report = {
            'filename': os.path.basename(file_path),
            'timestamp': datetime.now().isoformat(),
            'overall_quality_score': 0,
            'issues_found': []
        }
    
    def load_data(self):
        """Load data based on file extension"""
        try:
            if self.file_path.endswith('.csv'):
                self.df = pd.read_csv(self.file_path)
            elif self.file_path.endswith(('.xls', '.xlsx')):
                self.df = pd.read_excel(self.file_path)
            elif self.file_path.endswith('.json'):
                self.df = pd.read_json(self.file_path)
            else:
                raise ValueError(f"Unsupported file type: {self.file_path}")
            
            self.report['total_rows'] = len(self.df)
            self.report['total_columns'] = len(self.df.columns)
            self.report['column_names'] = list(self.df.columns)
            return True
            
        except Exception as e:
            self.report['load_error'] = str(e)
            return False
    
    def check_missing_values(self):
        """Identify columns with missing data"""
        missing = self.df.isnull().sum()
        missing_pct = (missing / len(self.df)) * 100
        
        missing_info = {}
        for col in self.df.columns:
            if missing[col] > 0:
                missing_info[col] = {
                    'count': int(missing[col]),
                    'percentage': round(float(missing_pct[col]), 2)
                }
                if missing_pct[col] > 20:
                    self.report['issues_found'].append(
                        f"High missing rate in '{col}': {missing_pct[col]:.1f}%"
                    )
        
        self.report['missing_values'] = missing_info
        return missing_info
    
    def check_duplicates(self):
        """Find duplicate rows"""
        duplicate_rows = self.df.duplicated().sum()
        duplicate_pct = (duplicate_rows / len(self.df)) * 100
        
        self.report['duplicate_rows'] = int(duplicate_rows)
        self.report['duplicate_percentage'] = round(duplicate_pct, 2)
        
        if duplicate_rows > 0:
            self.report['issues_found'].append(
                f"Found {duplicate_rows} duplicate rows ({duplicate_pct:.1f}%)"
            )
    
    def check_data_types(self):
        """Verify data types are consistent"""
        type_issues = []
        type_info = {}
        
        for col in self.df.columns:
            actual_type = str(self.df[col].dtype)
            unique_types = self.df[col].apply(type).unique()
            
            type_info[col] = {
                'inferred_type': actual_type,
                'unique_types': [str(t) for t in unique_types]
            }
            
            # Flag mixed types
            if len(unique_types) > 2:  # More than 2 distinct types
                type_issues.append(f"Mixed types in '{col}': {unique_types}")
        
        self.report['data_types'] = type_info
        
        if type_issues:
            self.report['issues_found'].extend(type_issues)
    
    def check_numeric_columns(self):
        """Analyze numeric columns for outliers"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        numeric_summary = {}
        
        for col in numeric_cols:
            # Calculate statistics
            mean = self.df[col].mean()
            std = self.df[col].std()
            q1 = self.df[col].quantile(0.25)
            q3 = self.df[col].quantile(0.75)
            iqr = q3 - q1
            
            # Identify outliers using IQR method
            outliers_iqr = self.df[
                (self.df[col] < (q1 - 1.5 * iqr)) | 
                (self.df[col] > (q3 + 1.5 * iqr))
            ]
            
            # Identify outliers using z-score method
            z_scores = np.abs((self.df[col] - mean) / std) if std > 0 else 0
            outliers_zscore = self.df[z_scores > 3]
            
            numeric_summary[col] = {
                'min': float(self.df[col].min()),
                'max': float(self.df[col].max()),
                'mean': float(mean),
                'std': float(std) if not pd.isna(std) else 0,
                'outliers_iqr': len(outliers_iqr),
                'outliers_zscore': len(outliers_zscore),
                'missing': int(self.df[col].isnull().sum())
            }
            
            # Flag if outliers detected
            if len(outliers_iqr) > 0:
                self.report['issues_found'].append(
                    f"Found {len(outliers_iqr)} outliers in numeric column '{col}'"
                )
        
        self.report['numeric_summary'] = numeric_summary
    
    def check_categorical_columns(self):
        """Analyze categorical columns"""
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        categorical_summary = {}
        
        for col in categorical_cols:
            value_counts = self.df[col].value_counts()
            unique_count = len(value_counts)
            
            categorical_summary[col] = {
                'unique_values': unique_count,
                'top_values': value_counts.head(3).to_dict(),
                'missing': int(self.df[col].isnull().sum())
            }
            
            # Flag if too many unique values (potential ID column)
            if unique_count > len(self.df) * 0.9:
                self.report['issues_found'].append(
                    f"Column '{col}' has {unique_count} unique values - might be an ID field"
                )
        
        self.report['categorical_summary'] = categorical_summary
    
    def calculate_quality_score(self):
        """Calculate overall data quality score (0-100)"""
        score = 100
        deductions = []
        
        # Deduct for missing data
        missing_count = sum(v.get('count', 0) for v in self.report.get('missing_values', {}).values())
        if missing_count > 0:
            missing_penalty = min(20, (missing_count / (self.report['total_rows'] * self.report['total_columns'])) * 100)
            score -= missing_penalty
            deductions.append(f"Missing data: -{missing_penalty:.1f}")
        
        # Deduct for duplicates
        duplicate_pct = self.report.get('duplicate_percentage', 0)
        if duplicate_pct > 0:
            duplicate_penalty = min(15, duplicate_pct)
            score -= duplicate_penalty
            deductions.append(f"Duplicates: -{duplicate_penalty:.1f}")
        
        # Deduct for outliers (if any)
        outlier_count = sum(
            v.get('outliers_iqr', 0) 
            for v in self.report.get('numeric_summary', {}).values()
        )
        if outlier_count > 0:
            outlier_penalty = min(10, outlier_count / 10)
            score -= outlier_penalty
            deductions.append(f"Outliers: -{outlier_penalty:.1f}")
        
        self.report['quality_score'] = max(0, round(score, 1))
        self.report['score_deductions'] = deductions
        return self.report['quality_score']
    
    def generate_report(self, output_format='json'):
        """Generate quality report"""
        self.report['issues_summary'] = f"Found {len(self.report['issues_found'])} potential issues"
        self.calculate_quality_score()
        
        if output_format == 'json':
            report_file = f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(self.report, f, indent=2, default=str)
            print(f"📊 Report saved to: {report_file}")
            return self.report
        else:
            # Print summary to console
            print("\n" + "="*50)
            print("📋 DATA QUALITY REPORT")
            print("="*50)
            print(f"File: {self.report['filename']}")
            print(f"Rows: {self.report['total_rows']}, Columns: {self.report['total_columns']}")
            print(f"Quality Score: {self.report['quality_score']}/100")
            print(f"Issues Found: {len(self.report['issues_found'])}")
            
            if self.report['issues_found']:
                print("\n⚠️ Issues:")
                for issue in self.report['issues_found'][:5]:
                    print(f"  • {issue}")
            
            return self.report

def create_sample_data():
    """Create sample data with quality issues for testing"""
    import random
    
    # Create data with intentional issues
    data = {
        'name': [f'User_{i}' for i in range(100)],
        'age': [random.randint(18, 65) for _ in range(95)] + [None]*5,
        'salary': [random.randint(30000, 120000) for _ in range(98)] + [1000000, 2500000],
        'department': ['IT', 'HR', 'Sales', 'Marketing', None] * 20,
        'join_date': pd.date_range('2020-01-01', periods=100, freq='D')
    }
    
    df = pd.DataFrame(data)
    
    # Add duplicates
    df = pd.concat([df, df.iloc[[0, 1, 2]]])  # Add 3 duplicates
    
    # Save to CSV
    df.to_csv('sample_employee_data.csv', index=False)
    print("✅ Created sample_employee_data.csv with intentional quality issues")

if __name__ == "__main__":
    # Create sample data if it doesn't exist
    if not os.path.exists('sample_employee_data.csv'):
        create_sample_data()
    
    # Run quality check
    checker = DataQualityChecker('sample_employee_data.csv')
    
    if checker.load_data():
        checker.check_missing_values()
        checker.check_duplicates()
        checker.check_data_types()
        checker.check_numeric_columns()
        checker.check_categorical_columns()
        
        # Generate both JSON report and console output
        report = checker.generate_report(output_format='console')
        
        # Also save JSON
        checker.generate_report(output_format='json')
    else:
        print(f"❌ Failed to load data: {checker.report.get('load_error')}")
