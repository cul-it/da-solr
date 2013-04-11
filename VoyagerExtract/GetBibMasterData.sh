#!/bin/bash
CLASS=edu.cornell.library.integration.GetBibMasterData
CLASSES=./build/classes
LIB=./build/lib
# CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
# CLASSPATH=$CLASSPATH:$CLASSES
CLASSPATH=./build/VoyagerExtract.jar
echo $CLASSPATH
# need to specifiy bibid 
BIBID=$1
$JAVA_HOME/bin/java -classpath $CLASSPATH $CLASS $BIBID 
