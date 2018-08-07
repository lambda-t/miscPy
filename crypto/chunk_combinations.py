import os
import math

def chunk(df,num_workers):
    nworkers = 200
    chunksize = math.floor(len(df) / nworkers)
    chunks = [(chunksize * i, (chunksize * i) + chunksize) for i in range(nworkers)]
    chunks.append((chunksize * nworkers, len(df)))

    for i in chunks:
        data = df[i[0]:i[1]]
        #spin docker container with cache
        #load chunks in cache
        #load logic py
        #run logic py

