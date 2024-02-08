#!/bin/bash

rm -f mypipe
scala producer.scala &
python3 /home/sej/workspace/tools/backup/python/tensorflow_backbone/tfBackbone.py &