#!/bin/bash

# add random number to start of all files
# this is useful to randomize the order that hadoop processes the files  

undoid=$RANDOM
undofile=undo$undoid.sh

ls -1 | while read fname
do
 newname=$RANDOM.$fname
 mv "$fname" "$newname"
done

