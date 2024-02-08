sudo /usr/local/zookeeper/bin/zkServer.sh start
# 30초 지난 뒤 Kafka 실행
echo "Waiting for Zookeeper Execution Finished"
sleep 30
nohup sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
# GPU Numa node repair
sudo /home/sej/workspace/tools/backup/numa_repair.sh
