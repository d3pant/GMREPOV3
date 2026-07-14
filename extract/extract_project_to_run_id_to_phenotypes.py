from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

OUT_PATH = Path(__file__).parent.parent / "data/extracted/project_to_run_id_to_phenotypes.parquet"

engine = create_engine("mysql+pymysql://root:gmrepo@127.0.0.1:3307/gmrepo")

df = pd.read_sql("SELECT * FROM project_to_run_id_to_phenotypes", engine)
df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(df)} rows to {OUT_PATH}")
