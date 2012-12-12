#!/bin/bash

CLASS=edu.cornell.library.integration.app.LookupBibId
CLASSES=./build/WEB-INF/classes
LIB=./build/WEB-INF/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

java -classpath $CLASSPATH $CLASS 4255465 
