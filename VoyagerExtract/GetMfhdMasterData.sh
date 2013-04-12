#!/bin/bash
CLASS=edu.cornell.library.integration.GetMfhdMasterData
CLASSES=./build/classes
LIB=./build/lib
CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$CLASSES
# need to specifiy mfhd id
MFHDID=$1
java -classpath $CLASSPATH $CLASS $MFHDID 
