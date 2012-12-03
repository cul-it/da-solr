#!/bin/bash
CLASS=edu.cornell.library.integration.GetRecentBibDataCount
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

java -classpath $CLASSPATH $CLASS 
