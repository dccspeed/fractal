import dill
import codecs

def dumps(obj):
    return codecs.encode(dill.dumps(obj, recurse=True), "hex").decode()

def loads(hexstr):
    return dill.loads(codecs.decode(hexstr.encode(), "hex"))