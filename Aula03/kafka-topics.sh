# Criar tópico para tweets
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tweets

# Produtor Python para Twitter AP
from tweepy import Stream
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# Código do listener omitido