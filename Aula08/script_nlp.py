from pyspark.sql import SparkSession
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import sparknlp

# Inicializar Spark NLP
spark = sparknlp.start()

# Carregar dataset
tweets_df = spark.read.json("tweets_dataset.json")

# Pipeline NLP
document_assembler = DocumentAssembler().setInputCol("text").setOutputCol("document")

# Embeddings do Universal Sentence Encoder (modelo multilíngue compatível com o SentimentDL)
use_embeddings = (
    UniversalSentenceEncoder.pretrained("tfhub_use", "en")
    .setInputCols(["document"])
    .setOutputCol("sentence_embeddings")
)

# Modelo de sentimento treinado em tweets
sentimentModel = (
    SentimentDLModel.pretrained("sentimentdl_use_twitter", "en")
    .setInputCols(["sentence_embeddings"])
    .setOutputCol("sentiment")
)

pipeline = Pipeline().setStages([
    document_assembler,
    use_embeddings,
    sentimentModel
])

# Executar pipeline
result = pipeline.fit(tweets_df).transform(tweets_df)

# Mostrar resultados
result.select("text", "sentiment.result").show(10, truncate=False)
