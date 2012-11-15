#!/bin/bash
CLASS=edu.cornell.library.integration.GetRecentBibData
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy destination Dir for bib data
# java -classpath $CLASSPATH $CLASS $@
java -classpath $CLASSPATH $CLASS http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.updates 
