#!/bin/bash
#
# Run Marc2MarcXML
#
CLASS=edu.cornell.library.integration.MarcReader
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

java -classpath $CLASSPATH $CLASS $@
