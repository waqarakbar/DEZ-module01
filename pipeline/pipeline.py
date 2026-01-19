import sys
import pandas as pd

month = sys.argv[1]
# print(f"the month is {month}")

df = pd.DataFrame({"Days": [1, 2, 3, 4, 5, 6, 7], "Passengers": [25, 26, 27, 28, 29, 30, 31]})
df['Month'] = month
print(df.head())

df.to_parquet(f"output_{month}.parquet");