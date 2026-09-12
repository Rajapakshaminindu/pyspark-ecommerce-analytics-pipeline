import pandas as pd
import sys

def main():
    parquet_path = "output/analytics_parquet"
    print(f"Reading Parquet data from: {parquet_path} ...")
    
    df = pd.read_parquet(parquet_path)
    
    print("\n" + "="*80)
    print(f"DATASET SUMMARY: Total Rows = {len(df):,}, Total Columns = {len(df.columns)}")
    print("="*80)
    print("\nColumns & Types:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col:<25} ({dtype})")
        
    print("\n" + "="*80)
    print("ROW COUNT PER PARTITION (category):")
    print("="*80)
    print(df['category'].value_counts().to_string())
    
    print("\n" + "="*80)
    print("SAMPLE PREVIEW (First 5 Rows):")
    print("="*80)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df.head(5))

    # Also save a sample CSV so user can double-click and open it in editor if desired
    sample_csv_path = "output/preview_sample.csv"
    df.head(50).to_csv(sample_csv_path, index=False)
    print(f"\n Exported a 50-row preview to: {sample_csv_path} (can be opened in any text editor/Excel)")

if __name__ == "__main__":
    main()
