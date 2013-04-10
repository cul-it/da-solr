#!/bin/bash

CLASS=edu.cornell.library.integration.GetLocations
CLASSES=./build/classes
LIB=./build/lib
OPTS=-Xmx1280m

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

java $OPTS -classpath $CLASSPATH $CLASS http://culdata.library.cornell.edu/data/voyager/locations
