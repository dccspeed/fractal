all: build
install: sparkinstall pyfractalinstall

build: sparkbuild/spark

pyfractalinstall:
	pip install .

sparkbuild/spark:
	mkdir -p sparkbuild
	cd sparkbuild && wget "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz" && tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz && mv spark-3.5.0-bin-hadoop3-scala2.13 spark

sparkinstall: build
	cd sparkbuild &&  pip install spark/python

clean:
	rm -rf sparkbuild

.PHONY: all build install sparkinstall pyfractalinstall