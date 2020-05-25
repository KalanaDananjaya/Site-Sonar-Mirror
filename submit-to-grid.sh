#!/bin/bash
set -e

GRID_HOME="$1"
BASE_DIR="$GRID_HOME/site-sonar"
JDL_DIR="$BASE_DIR/JDL"

JDL_LIST=$(alien.py ls $JDL_DIR)

for i in $JDL_LIST
do
    cmd="alien.py submit $JDL_DIR/$i"
    echo $cmd
    $cmd &
done
