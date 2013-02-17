#!/bin/bash
CLASS=edu.cornell.library.integration.GetRecordCount
CLASSES=./build/classes
LIB=./build/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
echo $CLASSPATH
# java -classpath $CLASSPATH $CLASS $@
SRCDIR=http://culdata.library.cornell.edu/data/voyager/bib/bib.mrc.full.done
java -classpath $CLASSES $CLASS $SRCDIR
