#!/bin/bash

SPARQLURL=http://bdc34-dev.library.cornell.edu:8890/sparql 
BATCHSIZE=10000

# get the total number of bib records
COUNTQUERY='select count(distinct(?uri)) { ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> . }'
TOTALBIBS=$( curl -g --data-urlencode "query=$COUNTQUERY" $SPARQLURL | xpath  -e "/sparql/results/result/binding/literal/text()" )

WORKDIR="./tmpWork"
mkdir $WORKDIR

# make a script to load each of the files
for (( OFFSET=0; OFFSET<$TOTALBIBS ;OFFSET=$OFFSET+$BATCHSIZE ))
do
  BATCHNAME=batch.$OFFSET.sh
  BIBURIQUERY="select ?uri { ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> . } LIMIT $BATCHSIZE OFFSET $OFFSET"
  
  echo "curl -g --data-urlencode \""query=$BIBURIQUERY"\" $SPARQLURL | grep binding | sed \"s/^.*\(http[^<]*\).*$/\1/g\" | ./stdinRecordToDocument.sh  && rm $WORKDIR/$BATCHNAME " > $WORKDIR/$BATCHNAME
done
