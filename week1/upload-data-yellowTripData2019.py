import pandas as pd
from sqlalchemy import create_engine
from time import time

# create new db connection
engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')
engine.connect()

# read csv files and split to chunksize 100k
# df_iter = pd.read_csv('green_tripdata_2019-01.csv',iterator=True, chunksize=100000)
# df = next(df_iter)
df = pd.read_csv('green_tripdata_2019-01.csv')

# convert column become datetime
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

# insert data to db
# while True:
#     t_start = time()
#     df = next(df_iter)
#     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
#     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
#     df.to_sql(name='green_taxi_data_2019',con=engine,if_exists='replace')
#     t_end = time()
#     print('inserted another chunk ..., took %.3f second' % (t_end - t_start))
t_start = time()
df.to_sql(name='green_taxi_data_2019',con=engine,if_exists='replace')
t_end = time()
print('inserted data ..., took %.3f second' % (t_end - t_start))