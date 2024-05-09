#!/bin/bash

mypath=`realpath "$0"`
mybase=`dirname "$mypath"`
cd $mybase

poetry config virtualenvs.in-project true
poetry install
