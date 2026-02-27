
"""
Data Quality Checker - Validates CSV/Excel data quality
Flags missing values, duplicates, outliers
"""
import pandas as pd
import numpy as np
import json

def check_data_quality(file_path, output_report=True):
    """
    Comprehensive data quality check
    Mimics real data engineering validation tasks
    """
    # Load data based on file type
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type")
    
    quality_report = {
        'filename': file_path,
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': df.dtypes.astype(str).to_dict(),
        'numeric_summary': {}
    }
    
    # Numeric column analysis
    for col in df.select_dtypes(include=[np.number]).columns:
        quality_report['numeric_summary'][col] = {
            'min': float(df[col].min()),
            'max': float(df[col].max()),
            'mean': float(df[col].mean()),
            'std': float(df[col].std()),
            'outliers': int((np.abs(df[col] - df[col].mean()) > 3*df[col].std()).sum())
        }
    
    if output_report:
        report_name = f"quality_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_name, 'w') as f:
            json.dump(quality_report, f, indent=2)
        print(f"✅ Report saved: {report_name}")
    
    return quality_report

# Example usage
if __name__ == "__main__":
    # Test with sample data
    import io
    sample = "id,name,age,salary\n1,Alice,30,70000\n2,Bob,,85000\n3,Charlie,35,\n4,Alice,30,70000"
    with open('sample_data.csv', 'w') as f:
        f.write(sample)
    
    report = check_data_quality('sample_data.csv')
    print("\n📊 Quality Summary:")
    print(f"   Missing values: {sum(report['missing_values'].values())}")
    print(f"   Duplicates: {report['duplicate_rows']}")
