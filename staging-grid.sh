#!/bin/bash
set -e

GRID_HOME="$1"
BASE_DIR="$GRID_HOME/site-sonar"
JDL_DIR="$BASE_DIR/JDL"
LOCAL_JDL="JDL"

echo $JDL_DIR

function stage_JDLs() {
	local j
	local cmd

	for j in $(ls $LOCAL_JDL)
	do
	  cmd="alien_cp file://$LOCAL_JDL/$j alien://$JDL_DIR/$(basename $j)@disk:1"
	  echo $cmd
	  $cmd &
	done
}

#alien.py rm -r -f $BASE_DIR
alien.py mkdir -p $JDL_DIR

stage_JDLs
