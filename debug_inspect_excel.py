import pandas as pd

file_path = "/Users/nishiharahiroto/.gemini/tmp/koushasai2024.xlsx"
sheet_name = "(Ｂ)一般売買高"

try:
    # Read only the header rows (first 5 rows)
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=5)
    print(f"\nHeader rows of sheet '{sheet_name}':")
    # Set display options to show all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)
    
    # Also print column values for row 2 (index 2) which seems to contain bond types
    print("\nColumn headers (Row 2 + Row 3 combined usually):")
    headers = []
    for col in range(df.shape[1]):
        val2 = str(df.iloc[2, col]).replace('\n', ' ')
        val3 = str(df.iloc[3, col]).replace('\n', ' ')
        headers.append(f"Col {col}: {val2} | {val3}")
    
    for h in headers:
        print(h)

except Exception as e:
    print(f"Error reading sheet '{sheet_name}': {e}")
