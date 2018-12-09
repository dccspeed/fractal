import sys

ifile = sys.argv[1]

with open(ifile, 'r') as f:
    nembeddings = []
    nbytes = []
    cummulative = []
    nvertices = 1
    for line in f:
        nembs = int(line.strip())
        nembeddings.append(nembs)
        nbytes.append(nembs * nvertices * 4)
        if nvertices == 1:
            cummulative.append(0)
        else:
            cummulative.append(cummulative[nvertices - 2] +
                    nembeddings[nvertices - 2])

        nvertices += 1

    relcost = []
    for i in xrange(len(nembeddings)):
        relcost.append(cummulative[i] / float(nembeddings[i]))

    #for i in xrange(len(nembeddings)):
    #    print ("%s %s %s %s %s" % (i, nembeddings[i], cummulative[i],
    #        relcost[i], nbytes[i]))
    
    for i in xrange(len(nembeddings)):
        print ("%s laststep %s %s" % (i+1,nembeddings[i],nembeddings[i] /
            float(nembeddings[i] + cummulative[i])))
    
    for i in xrange(len(cummulative)):
        print ("%s prevsteps %s %s" % (i+1,cummulative[i],cummulative[i] /
            float(nembeddings[i] + cummulative[i])))

