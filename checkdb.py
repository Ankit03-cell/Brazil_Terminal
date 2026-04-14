import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///trading_data.db')
df = pd.read_sql("SELECT * FROM market_prices LIMIT 1000", engine)
print(df)