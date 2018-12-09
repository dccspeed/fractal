#!/usr/local/bin/env python

import multiprocessing as mp,os

import sys

ifile = sys.argv[1]
ofile = sys.argv[2]
vertex2Id = {}
adjlists = {}


def process1(line):
    if line[0] == '#': continue
    toks = line.strip().split()
    a = int(toks[0])
    b = int(toks[1])
    if not a in vertex2Id:
        vid = len(vertex2Id)
        vertex2Id[a] = vid
    if not b in vertex2Id:
        vid = len(vertex2Id)
        vertex2Id[b] = vid

def process2(line):
    if line[0] == '#': continue
    toks = line.strip().split()
    a = vertex2Id[int(toks[0])]
    b = vertex2Id[int(toks[1])]
    l = adjlists.get(a, set())
    l.add(b)
    adjlists[a] = l
    l = adjlists.get(b, set())
    l.add(a)
    adjlists[b] = l

def process_wrapper(chunkStart, chunkSize, process):
    with open(ifile) as f:
        f.seek(chunkStart)
        lines = f.read(chunkSize).splitlines()
        for line in lines:
            process(line)

def chunkify(fname,size=1024*1024):
    fileEnd = os.path.getsize(fname)
    with open(fname,'r') as f:
        chunkEnd = f.tell()
        while True:
            chunkStart = chunkEnd
            f.seek(size,1)
            f.readline()
            chunkEnd = f.tell()
            yield chunkStart, chunkEnd - chunkStart
            if chunkEnd > fileEnd:
                break

#init objects
pool = mp.Pool(8)
jobs = []

with open(ifile,'r') as f:
    for line in f:
        jobs.append( pool.apply_async(process,line) )

for chunkStart,chunkSize in chunkify(ifile):

#wait for all jobs to finish
for job in jobs:
    job.get()

#create jobs
for chunkStart,chunkSize in chunkify(ifile):
    jobs.append( pool.apply_async(process_wrapper,(chunkStart,chunkSize,process1)) )

#wait for all jobs to finish
for job in jobs:
    job.get()



#clean up
pool.close()
