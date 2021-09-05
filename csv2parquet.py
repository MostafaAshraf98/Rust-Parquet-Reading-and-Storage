import sys
import pandas as pd

if len(sys.argv) != 3:
    raise Exception("Please supply input and output files")

in_file = sys.argv[1]
out_file = sys.argv[2]

df = pd.read_csv(in_file)
df.to_parquet(out_file)