import pandas as pd
from sqlalchemy import create_engine

def load_to_repository(df: pd.DataFrame, table_name: str, engine):
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
