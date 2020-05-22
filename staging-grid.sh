#!/bin/bash
set -e

GRID_HOME="$1"
BASE_DIR="$GRID_HOME/site-sonar"
JDL_DIR="$BASE_DIR/JDL"
SCRIPT_DIR="$BASE_DIR/scripts"
LOCAL_JDL="JDL"
LOCAL_SCRIPT="scripts"

function stage_JDLs() {
	local j
	local cmd

	for j in $(ls $LOCAL_JDL)
	do
	  cmd="alien_cp file:$LOCAL_JDL/$j alien:$JDL_DIR/$(basename $j)@disk:1"
	  echo $cmd
	  $cmd &
	done
}

function stage_scripts() {
	local j
	local cmd

	for j in $(ls $LOCAL_SCRIPT)
	do
	  cmd="alien_cp file:$LOCAL_SCRIPT/$j alien:$SCRIPT_DIR/$(basename $j)@disk:1"
	  echo $cmd
	  $cmd &
	done
}

alien.py rm -r -f $BASE_DIR
alien.py mkdir -p $JDL_DIR
alien.py mkdir -p $SCRIPT_DIR

stage_JDLs
stage_scripts