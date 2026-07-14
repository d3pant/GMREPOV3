from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

OUT_PATH = Path(__file__).parent.parent / "data/extracted/sample_to_run_info.parquet"

engine = create_engine("mysql+pymysql://root:gmrepo@127.0.0.1:3307/gmrepo")

df = pd.read_sql("SELECT * FROM sample_to_run_info", engine)
df.to_parquet(OUT_PATH, index=False)

print(f"wrote {len(df)} rows to {OUT_PATH}")
