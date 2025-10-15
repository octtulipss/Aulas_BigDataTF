# Produtor
docker exec -it bigdata-kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic tweets

# Consumidor
docker exec -it bigdata-kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tweets --from-beginning

# Delete Menssage
docker exec -it bigdata-kafka-1 kafka-topics.sh --delete --topic tweets --bootstrap-server localhost:9092

# Pyspark CLI
docker exec -it bigdata-jupyter-lab-1 pyspark --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0


