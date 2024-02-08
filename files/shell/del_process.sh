#!/bin/bash

ps -elf | grep producer-consumer.py | awk '{print $4}' > temp.txt

cat temp.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

rm -f temp.txt

ps -elf | grep normal_upper_avg_ywtest.py | awk '{print $4}' > temp.txt

cat temp.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

ps -elf | grep standard_avg.py | awk '{print $4}' > temp.txt

cat temp.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

rm -f temp.txt

ps -elf | grep tfBackbone | awk '{print $4}' > temp.txt

cat temp.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

rm -f temp.txt

ps -elf | grep normal_avg.py | awk '{print $4}' > temp.txt

cat temp.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

rm -f temp.txt

ps -elf | grep rapid | awk '{print $4}' > temp_rapid.txt

cat temp_rapid.txt | while read line || [[ -n "$line" ]];
do
	kill -9 $line
done

rm -f temp_rapid.txt

sudo rm -f /home/sej/data/log/*
sudo rm -f /kafka/linear-road-topic-0/*.timeindex
sudo rm -f /kafka/linear-road-topic-0/*.index
sudo rm -f /kafka/linear-road-topic-0/*.log
sudo rm -f /kafka/linear-road-topic-0/*.snapshot
#sudo rm -rf /tmp/blockmgr-*
#sudo rm -rf /tmp/rapids-*
#sudo rm -rf /tmp/temporary-*
sudo apt-get autoremove
sudo apt-get clean


