docker build -t spark-nlp-twitter .
docker run -it --rm -p 8888:8888 spark-nlp-twitter

docker run -it --rm -v $(pwd)/tweets_dataset.json:/app/tweets_dataset.json spark-nlp-twitter
