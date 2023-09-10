SBT:=sbt
MODULE:=bayesianNetwork
MODULE_LC=$(shell echo $(MODULE) | tr '[:upper]' '[:lower]')
SCALA_VERSION:=2.13

# Dependent sources.
SRC:=$(shell find $(MODULE) -name '*.scala')

TRGT_JAR:=$(MODULE)/target/scala-$(SCALA_VERSION)/$(MODULE)-assembly-0.0.1.jar

# The satellite dependency jars of the application.
#DEPS:= $(shell find $(JAR_PATH) -name '*.jar')

EXP_FILE:=bayesianNetwork/src/main/python/run_export.py

$(TRGT_JAR): $(SRC) build.sbt
	$(SBT) assembly

deps:
	@echo $(DEPS)

clean:
	$(SBT) clean

build: $(TRGT_JAR)

download: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/download_config"

# 		--jars $(shell echo $(DEPS) | sed 's/ /,/') \

local_feature_extraction: $(TRGT_JAR)
	spark-submit \
		--master local \
		--driver-memory 4g \
		--executor-memory 2g \
		--class com.erbridge.ds.LocalApp \
		$(TRGT_JAR) \
		features-extraction

remote_feature_extraction: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/features"

local_build_network: $(TRGT_JAR)
	spark-submit \
		--master local \
		--driver-memory 4g \
		--executor-memory 2g \
		--class com.erbridge.ds.LocalApp \
		$(TRGT_JAR) \
	  topology-builder

remote_build_network: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/network_topology"

local_network_prediction: $(TRGT_JAR)
	spark-submit \
		--master local \
		--driver-memory 4g \
		--executor-memory 2g \
		--jars $(shell echo $(DEPS) | sed 's/ /,/') \
		--class com.erbridge.ds.LocalApp \
		$(TRGT_JAR) \
	  network-query

remote_network_prediction: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/network_query"

local_oracle: $(TRGT_JAR)
	spark-submit \
		--master local \
		--driver-memory 4g \
		--executor-memory 2g \
		--jars $(shell echo $(DEPS) | sed 's/ /,/') \
		--class com.erbridge.ds.LocalApp \
		$(TRGT_JAR) \
		oracle

remote_oracle: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/oracle"

local_validation: $(TRGT_JAR)
	spark-submit \
		--master local \
		--driver-memory 4g \
		--executor-memory 2g \
		--jars $(shell echo $(DEPS) | sed 's/ /,/') \
		--class com.erbridge.ds.LocalApp \
		$(TRGT_JAR) \
		validation

remote_validation: $(TRGT_JAR)
	$(SBT) $(MODULE)/"sparkSubmit --conf sgprod/validation"

local_export_graph: guard-JSON guard-OUT guard-EDGE_THRESHOLD $(EXP_FILE)
	python $(EXP_FILE) -i $(JSON) -o $(OUT) -t $(EDGE_THRESHOLD) && \
	dot -Tpng $(OUT) -o $(OUT:.dot=.png) && \
	open $(OUT:.dot=.png)

test_network_builder: $(TRGT_JAR)
	$(SBT) "testOnly com.erbridge.ds.network.NetworkBuilderTest"

test_network_query: $(TRGT_JAR)
	$(SBT) "testOnly com.erbridge.ds.network.NetworkQueryTest"

guard-%:
	@ if [ "${${*}}" = "" ]; then \
  	echo "Environment variable $* not set"; \
    	exit 1; \
		fi
