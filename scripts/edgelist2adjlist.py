#!/usr/local/bin/env python

import sys

ifile = sys.argv[1]
ofile = sys.argv[2]
vertex2Id = {}
adjlists = {}

with open(ifile, 'r') as f:
    for line in f:
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

    print ("nvertices: %s" % (len(vertex2Id)))

nvertices = len(vertex2Id)

with open(ifile, 'r') as f:
    for line in f:
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
    
    print ("nadjlists: %s" % (len(adjlists)))

with open(ofile, 'w') as f:
    for vid in xrange(nvertices):
        f.write ("%s 1 %s\n" % (vid," ".join([str(x) for x in adjlists[vid]])))
