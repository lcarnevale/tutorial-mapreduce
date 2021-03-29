# Running Word Count MapReduce over Docker Hadoop Containers

```bash
wget -O docker-hadoop-master.zip https://github.com/big-data-europe/docker-hadoop/archive/refs/heads/master.zip
unzip docker-hadoop-master.zip
cd docker-hadoop-master
# wget -O docker-compose.yml https://gist.githubusercontent.com/nathan815/a938b3f7a4d06b2811cf2b1a917800e1/raw/84f1432f4d452e8afdcf9566da45a9713db79e66/docker-compose.yml
```

```bash
docker-compose up -d
```

```bash
docker exec -it namenode bash -c "apt update && apt install python -y"
docker exec -it datanode bash -c "apt update && apt install python -y"
docker exec -it resourcemanager bash -c "apt update && apt install python -y"
docker exec -it nodemanager bash -c "apt update && apt install python -y"
```

```bash
cd ..
docker cp mapper.py namenode:mapper.py
docker cp reducer.py namenode:reducer.py
docker exec namenode mkdir input
docker cp ../data/lotr1.txt namenode:input/lotr1.txt
```

```bash
docker exec -it namenode bash
hadoop fs -mkdir -p input
hdfs dfs -put ./input/* input
hadoop fs -rm -r /user/root/output 
```

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-file mapper.py    -mapper mapper.py \
-file reducer.py   -reducer reducer.py \
-input input -output output
hadoop fs -get /user/root/output/part-00000
sort -k2 -n part-00000
```

```bash
docker-compose down
```