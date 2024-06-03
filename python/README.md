# PyFractal: A Python Wrapper for Fractal

1. Build and install this python wrapper using a virtual environment:

```commandline
git clone git@github.com:dccspeed/fractal.git fractal && cd fractal
cd python && python3 -m venv venv && source venv/bin/activate && cd ..
cd python && make && make install && cd ..
```

2. Test some provided example, for example, motif counting:

```commandline
python python/examples/motifcounting.py data/citeseer 4
```

