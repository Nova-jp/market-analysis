from data.utils.database_manager import DatabaseManager
import pandas as pd

def check_bond_codes():
    db = DatabaseManager()
    query = """
    SELECT bond_code, bond_name 
    FROM bond_data 
    WHERE trade_date = (SELECT MAX(trade_date) FROM bond_data)
    """
    rows = db.execute_query(query)
    
    suffix_map = {
        '0042': '2-Year',
        '0045': '5-Year',
        '0057': '5-Year (GX)',
        '0067': '10-Year?',
        '0058': '10-Year (GX)?',
        '0069': '20-Year?',
        '0068': '30-Year',
        '0054': '40-Year',
        '0027': '5-Year (GX WI)?'
    }

    print(f"{'Code':<15} | {'Suffix':<6} | {'Name'}")
    print("-" * 50)
    
    seen_suffixes = set()
    
    for row in rows:
        code = str(row[0]).strip()
        name = row[1]
        if len(code) >= 4:
            suffix = code[-4:]
            
            if suffix in ['0069', '0067', '0058', '0054', '0068', '0045', '0042', '0027', '0057', '0074']:
                if suffix not in seen_suffixes or suffix in ['0069', '0067']: # Print more for these
                    print(f"{code:<15} | {suffix:<6} | {name}")
                    seen_suffixes.add(suffix)

if __name__ == "__main__":
    check_bond_codes()