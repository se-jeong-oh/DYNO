import threading, logging, time, random
import os, sys
from multiprocessing import Process
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import csv
from datetime import datetime, timezone

def producer(numTuplesPerInterval, timeInterval):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = "linear-road-topic"
    print("Kafka producer started - Data ingestion to linear_road topic") 

    with open ("/home/sej/workspace/data/output_0.dat", "r") as myfile:
        #data = myfile.readlines()
        #for line in data:
        
        startTime = time.time()
        #print(time.time())
        
        for count, line in enumerate(myfile):
            producer.send(topic, line.encode('utf-8'))
            
            # Send 'numTuplesPerSec' tuples to producer per second instead of random traffic.
            #time.sleep(random.uniform(0, 1)) # random traffic
            if count % numTuplesPerInterval == 0:
                #print(time.time())
                elapsedTime = time.time() - startTime
                #if elapsedTime < 1.0:
                    #time.sleep(1.0 - elapsedTime)
                if elapsedTime < timeInterval:
                    time.sleep(timeInterval - elapsedTime)
                else:
                    print("elpasedTime was bigger than " + str(timeInterval) + " sec!")
                #time.sleep(1)
                startTime = time.time() # Restart the timer.

    producer.close()

def consumer(numTuplesPerInterval, timeInterval):
    topic = "linear-road-topic"
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], 
                                enable_auto_commit=True, auto_offset_reset='earliest',
                                max_poll_records=300000, max_partition_fetch_bytes=10485760)
    print("Kafka consumer started - Data copying from kafka topic to hdfs")

    while True:
        
        msg_dict = consumer.poll(max_records = numTuplesPerInterval)
        if len(msg_dict) == 0: # If there is no data from producer
            continue # Do not create data file
        #print(msg_dict)
        #timestamp = time.time()
        #filename = os.path.join("/home/sue/kafka_files/linear_road/" + str(timestamp) + ".csv") # TODO) Need HDFS path!
        # Use NFS path for running spark cluster.
        #filename = os.path.join("/home/sue/nfs/linear_road/stream_data/" + str(timestamp) + ".csv")
        #fp = open(filename, 'w', newline='')
        #writer = csv.writer(fp)
        
        data = []

        for key, messages in msg_dict.items():
            #print(str(messages[0].value))
            for msg in messages:
                timestamp = datetime.fromtimestamp((msg.timestamp / 1000), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") # 맨 마지막에 %Z 없어도 spark-rapids에서 GPU 함수 사용 가능
                row = str(msg.value)[2:-3] + "," + str(timestamp)
                data.append(list(row.split(',')))
                #writer.writerow(row.split(','))
        try:
        # Write data at once to minimize delay.
            timestamp = time.time()
            # Use NFS path for running spark cluster.
            filename = os.path.join("/home/sej/workspace/csv_data/" + str(timestamp) + ".csv")
            #print(filename)
            fp = open(filename, 'w', newline='')
            writer = csv.writer(fp)
            writer.writerows(data)
            #print(data)
            fp.close()
            time.sleep(timeInterval) # control traffic
        except:
            print("Error Occured!")

    consumer.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Need argument (1): Number of tuples per interval")
        print("Need argument (2): Time Interval (sec)")
        sys.exit(1)
    else:
        numTuplesPerInterval = int(sys.argv[1])
        timeInterval = float(sys.argv[2])

    Process(target=producer, args=(numTuplesPerInterval, timeInterval,)).start()
    Process(target=consumer, args=(numTuplesPerInterval, timeInterval,)).start()
