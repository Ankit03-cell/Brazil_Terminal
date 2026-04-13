import pandas as pd
from sqlalchemy import create_engine
import os

def load_data_to_sql(excel_filename):
    print(f"Opening file: {excel_filename}...")
    
    # 1. Read Excel starting from Row 2 (the one with dates)
    df = pd.read_excel(excel_filename, sheet_name="BG", header=1)

    if df.empty:
        print("⚠️ The Excel sheet appears to be empty.")
        return 0

    # 2. Reshape the data
    # Set the first column (Contract Names) as the index
    df = df.set_index(df.columns[0])
    
    # The fix: call .stack() without arguments, then .reset_index()
    # Then we drop NAs manually in the next step
    df_stacked = df.stack().reset_index()

    # 3. Rename columns to match our database
    df_stacked.columns = ['contract_name', 'date', 'price']

    # 4. Clean and Convert
    # Use 'coerce' to handle those pesky #REF! errors
    df_stacked['date'] = pd.to_datetime(df_stacked['date'], errors='coerce')
    df_stacked['price'] = pd.to_numeric(df_stacked['price'], errors='coerce')
    
    # Remove any invalid rows (including the NAs that stack didn't drop)
    df_final = df_stacked.dropna(subset=['date', 'price'])

    # 5. Save to SQL
    engine = create_engine('sqlite:///trading_data.db')
    df_final.to_sql('market_prices', con=engine, if_exists='replace', index=False)
    
    print(f"--- Ingestion Report ---")
    print(f"Successfully saved {len(df_final)} price points to SQL.")
    
    return len(df_final)

if __name__ == "__main__":
    filename = "GenericFBrazil.xlsx" 
    if os.path.exists(filename):
        count = load_data_to_sql(filename)
        if count > 0:
            print(f" Success! Your data is now indexed and ready in SQL.")
        else:
            print(f" Warning: 0 rows were loaded. Check your Excel formatting.")
    else:
        print(f"Error: {filename} not found in this folder.")