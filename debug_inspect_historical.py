import pandas as pd

def inspect_excel(path, desc):
    print(f"\n{'='*20}\nInspecting {desc}\n{'='*20}")
    try:
        xl = pd.ExcelFile(path)
        print(f"Sheet names: {xl.sheet_names[:5]}...") # Show first 5 sheets
        
        # Read first sheet
        df = pd.read_excel(xl, sheet_name=0, header=None, nrows=10)
        print("First 10 rows of Sheet 1:")
        print(df)
        
    except Exception as e:
        print(f"Error reading {desc}: {e}")

inspect_excel("/Users/nishiharahiroto/.gemini/tmp/koushasai2018r.xlsx", "2018 Data (Revised)")
inspect_excel("/Users/nishiharahiroto/.gemini/tmp/srb.xls", "Historical Data (srb.xls)")
