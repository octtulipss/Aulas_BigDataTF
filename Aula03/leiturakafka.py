from pyspark.sql import SparkSession

# Classe para guardar os códigos de cores ANSI para o terminal
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m' # Reseta a cor
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

spark = SparkSession.builder \
    .appName("TweetStreamConsumer") \
    # Garante que a UI do Spark não inicie, para um console mais limpo
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "latest") \
    .load()

# Converte o valor binário do Kafka para uma string legível
tweets_df = streaming_df.selectExpr("CAST(value AS STRING) as tweet")

# Função que será chamada para cada micro-lote de dados do stream
def exibir_tweets_com_estilo(batch_df, batch_id):
    """
    Coleta os dados do micro-lote e os exibe no console com formatação.
    """
    # Ação: Coleta os resultados para o Driver.
    # Cuidado: Use isso apenas se o volume de dados por lote for pequeno!
    tweets_do_lote = [row.tweet for row in batch_df.collect()]
    
    if tweets_do_lote:
        print(f"\n--- {Colors.HEADER}{Colors.BOLD}Lote #{batch_id} | {len(tweets_do_lote)} novos tweets recebidos{Colors.ENDC} ---")
        for tweet in tweets_do_lote:
            print(f"{Colors.CYAN}------------------------------------------------------{Colors.ENDC}")
            print(f"{Colors.GREEN}{tweet}{Colors.ENDC}")
            print(f"{Colors.CYAN}------------------------------------------------------{Colors.ENDC}")

# Configura o stream para usar nossa função customizada
query = tweets_df.writeStream \
    .outputMode("append") \
    .foreachBatch(exibir_tweets_com_estilo) \
    .start()

print(f"{Colors.WARNING}Aguardando novos tweets do tópico 'tweets'... (Pressione Ctrl+C para parar){Colors.ENDC}")
query.awaitTermination()
