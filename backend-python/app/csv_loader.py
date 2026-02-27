import pandas as pd

def load_csv_to_db(file, table_name, engine):
    df = pd.read_csv(file.file)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df.to_sql(table_name, engine, if_exists="replace", index=False)
