#!/bin/bash
cd `dirname $0`
dir=`pwd`
load_path="${dir}/loadApplication.py -module=$1 $2"
echo $load_path
python3 $load_path
echo "0"