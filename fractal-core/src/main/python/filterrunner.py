import dill
import codecs
import sys
from fractal import Subgraph

# deserialize filter (assume hex string)
filterstr = sys.argv[1]
filter = dill.loads(codecs.decode(filterstr.encode(), "hex"))
f = open('/tmp/error.txt', 'w')
sys.stderr = f
print("filter function:", filter, file=f)

nsubgraphsvalid = 0
nsubgraphsinvalid = 0

while True:
    subgraphstr = sys.stdin.readline().strip()
    subgraph = Subgraph(subgraphstr)

    #if subgraphstr == "CLOSE":
    #    break
    #try:
    #    subgraph = Subgraph(subgraphstr)
    #except Exception as e:
    #    print("error:", e, subgraphstr, file=f)
    #    break

    #print("subgraph object:", subgraph, file=f)

    result = filter(subgraph)

    #print("filter result:", result, file=f)

    # run filter
    if result:
        print("true")
        nsubgraphsvalid += 1
    else:
        print("false")
        nsubgraphsinvalid += 1

print("exit num subgraphs:", nsubgraphsvalid, "/",
      (nsubgraphsvalid+nsubgraphsinvalid), file=f)

f.close()
