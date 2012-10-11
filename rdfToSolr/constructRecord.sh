#!/bin/bash

# consider using with something like:
#  cat radioactiveMarcUris.txt | grep binding | sed "s/^.*\(http[^<]*\).*$/\1/g" | ./stdinRecordToDocument.sh

CLASS=edu.cornell.library.integration.recordextract.RecordExtract
COMPILED=./bin
LIB=./libs

RDFURL=http://bdc34-dev.library.cornell.edu:8890/sparql

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$COMPILED

java -classpath $CLASSPATH $CLASS $RDFURL $@
