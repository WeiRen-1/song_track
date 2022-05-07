import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('demo').getOrCreate()

### read the user_id data
df_user = spark.read.csv('data/userid-profile.tsv',sep='\t', header=True)

### read user_song track data
df_track = spark.read.csv('data/userid-timestamp-artid-artname-traid-traname.tsv',sep='\t',header=None)

### check the count and schema
print(f"The number of users: {df_user.distinct().count()}")
print(f"There are {df_track.count()} rows in the track.")

### find top 50 longest sessions, where song interval < 20 minutes

# 1. convert timestamp from string to timestamp
df_track= df_track.select('_c0', to_timestamp(df_track._c1).alias('_c1'), '_c2','_c3','_c4','_c5').sort("_c0", "_c1")

# 2. check the time_interval between sequencial track, groupby user_id
df_track = df_track.withColumn(
        "time_interval_sec", df_track._c1.cast("bigint") - lag(df_track._c1, 1).over(Window.partitionBy("_c0").orderBy(col("_c1"))).cast("bigint")  )

# 3. define "session" where time_interval_sec < 20min (1200 seconds)
#        sessions 0: within the same session of previous song
#        sessions 1: start a new session, > 20 mins
df_track = df_track.withColumn("sessions", when( (df_track['time_interval_sec']>1200) | (df_track['time_interval_sec']==None) , 1).otherwise(0))

# 3.1 define "session_id": sumulative sum of "session"
#        e.g., 0,1,2,3,...n
df_track = df_track.withColumn('session_id', sum(df_track.sessions).over(Window.partitionBy('_c0').orderBy().rowsBetween(-sys.maxsize, 0)))

# 4. Count the frequency of each session_id, groupby by user_id, find top 50 longest sessions
#    the "freqency" presents number of songs in each session_id
df_track_count = df_track.groupBy('_c0','session_id').count().orderBy('count',ascending=False)
df_track_count50 = df_track_count.limit(50)

# 5. filter df_track, such that it only contain rows that are in top 50 longest session: df_track_count50
df_join = df_track.join(df_track_count50, ['_c0', 'session_id'], 'leftsemi')

# 6. find the top 10 songs that in the filtered (top 50 longest session) df_track
df_join_count = df_join.groupBy('_c4','_c5').count().orderBy('count',ascending=False)
print("top 10 songs in top 50 longest session: ")
df_join_count.limit(10).show()


# save the top 10 songs into csv file
#df_join_count.limit(10).toPandas().to_csv('top10_songs.csv')