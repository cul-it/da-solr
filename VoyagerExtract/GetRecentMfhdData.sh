#!/bin/bash
CLASS=edu.cornell.library.integration.GetRecentMfhdData
CLASSES=./build/classes
LIB=./build/lib


CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES

# need to specifiy destination Dir for mfhd data
# java -classpath $CLASSPATH $CLASS $@
java -classpath $CLASSPATH $CLASS http://jaf30-dev.library.cornell.edu/data/voyager/mfhd/mfhd.mrc.updates 
