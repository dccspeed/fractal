ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

all: build
install: sparkinstall pyfractalinstall

build: $(ROOT_DIR)/sparkbuild/spark fractalcompile

sparkinstall: $(ROOT_DIR)/sparkbuild/spark
	cd $(ROOT_DIR)/sparkbuild && pip install spark/python
	touch sparkinstall

pyfractalinstall:
	pip install $(ROOT_DIR)

$(ROOT_DIR)/sparkbuild/spark:
	mkdir -p $(ROOT_DIR)/sparkbuild
	cd $(ROOT_DIR)/sparkbuild && wget "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz" && tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz && mv spark-3.5.0-bin-hadoop3-scala2.13 spark

fractalcompile:
	 cd "$(ROOT_DIR)/.." && ./gradlew jar && cp fractal-core/build/libs/*.jar "$(ROOT_DIR)/src/pyfractal/"

clean:
	rm -rf $(ROOT_DIR)/sparkbuild

.PHONY: all clean build install pyfractalinstall fractalcompile