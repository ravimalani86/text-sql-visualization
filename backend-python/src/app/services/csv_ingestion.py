from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def load_csv_to_db(file, table_name: str, engine: Engine) -> None:
    df = pd.read_csv(file.file)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df.to_sql(table_name, engine, if_exists="replace", index=False)

