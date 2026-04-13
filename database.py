from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# This is what our SQL table looks like
class MarketPrice(Base):
    __tablename__ = 'market_prices'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    contract_name = Column(String, nullable=False)
    price = Column(Float)

# This creates the actual file on your computer
engine = create_engine('sqlite:///trading_data.db')
Base.metadata.create_all(engine)

print("Database created as 'trading_data.db'")