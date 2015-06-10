#!/bin/bash
export DEBUG="replicate*"
node ./bin/fullfat.js --fat="http://user:pass@dst_host:5984/dst_base" --skim="http://user:pass@src_host:5984/src_base" --seq-file=./registry.seq --missing-log=./missing.log --tmp=./tmp --inactivity-ms=300000

