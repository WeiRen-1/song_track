# How to run:
```bash
make build
```
```bash
make run
```

# Note:
1. It requires Docker to be installed in local machine.

2. To be able to run successfully, data file "userid-timestamp-artid-artname-traid-traname.tsv" needs to be manually copied under "data/". The data size is too large to be uploaded to gitHub, even use Git Large File Storage.

3. The notebook "solution.ipynb" contains the same code as "main.py", while it shows intermediate results.

4. The top 10 songs in top 50 loggest sessions are in "top10_songs.csv".


# Improve
1. Data quality should be considered more carefully.

2. longest sessions are counted by the song numbers played in the session, not by the really time difference: (last_song_time - first_song_time) in one session.
