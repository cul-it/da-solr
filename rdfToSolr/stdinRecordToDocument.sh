#!/bin/bash

CLASS=edu.cornell.library.integration.indexer.StdinRecordToDocument
COMPILED=./bin
LIB=./lib

RDFURL=http://bdc34-dev.library.cornell.edu:8890/sparql
SOLRURL=http://bdc34-dev.library.cornell.edu:8080/solr/core1
RECTODOC_CLASS=edu.cornell.library.integration.indexer.RecordToDocumentMARC

CLASSPATH=$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=$CLASSPATH:$COMPILED

java -classpath $CLASSPATH $CLASS $RDFURL $RECTODOC_CLASS $SOLRURL
