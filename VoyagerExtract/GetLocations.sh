#!/bin/bash

CLASS=edu.cornell.library.integration.GetLocations
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

java -classpath $CLASSPATH $CLASS http://culdata.library.cornell.edu/data/voyager/locations
#java -classpath $CLASSPATH $CLASS http://jaf30-dev.library.cornell.edu/data/voyager/locations
