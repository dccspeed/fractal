import dill
import codecs
import sys
from util import networkx_from_string

# deserialize filter (assume hex string)
filterstr = sys.argv[1]
filter = dill.loads(codecs.decode(filterstr.encode(), "hex"))
while True:
    subgraphstr = sys.stdin.readline().strip()
    #subgraph = Subgraph(subgraphstr)
    subgraph = networkx_from_string(subgraphstr)
    result = filter(subgraph)
    # run filter
    if result:
        sys.stdout.write('1')
    else:
        sys.stdout.write('0')

    sys.stdout.flush()
