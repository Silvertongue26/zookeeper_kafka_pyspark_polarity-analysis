# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import re
import logging
from textblob import TextBlob

import findspark
findspark.init()


# Text cleaning function
def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # Remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove punctuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # Remove numbers
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # Remove hashtags
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove extra simbols
    tweet = re.sub('@\w+', '', str(tweet))
    tweet = re.sub('\n', '', str(tweet))

    return tweet

# TextBlob Subjectivity function
def Subjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity

# TextBlob Polarity function
def Polarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

# Assign sentiment to elements
def Sentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

# Main function
if __name__ == "__main__":
    # Create logging file
    logging.basicConfig(filename='spark_tw.log', encoding='UTF-8', level=logging.INFO)

    # Spark object creation
    spark = SparkSession\
            .builder \
            .appName("Sentiment_Analysis_TW") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

    # Main spark reader in kafka format
    df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", "TW_ANALYSIS")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load()

    # Convert df's column value tu string so we can manipulate it
    df.selectExpr("CAST(value AS STRING)")

    # Schema creation for new df values
    valSchema = StructType([StructField("text", StringType(), True)])

    # Creation and asignation of tweets text to new df
    values = df.select(from_json(df.value.cast("string"), valSchema).alias("tweets"))
    df1 = values.select("tweets.*")

    # User defined function creation from normal functions
    clean_tweets_udf = F.udf(cleanTweet, StringType())
    subjectivity_func_udf = F.udf(Subjectivity, FloatType())
    polarity_func_udf = F.udf(Polarity, FloatType())
    sentiment_func_udf = F.udf(Sentiment, StringType())

    # Tweet processing
    cl_tweets = df1.withColumn('processed_text', clean_tweets_udf(col("text")))
    subjectivity_tw = cl_tweets.withColumn('subjectivity', subjectivity_func_udf(col("processed_text")))
    polarity_tw = subjectivity_tw.withColumn("polarity", polarity_func_udf(col("processed_text")))
    sentiment_tw = polarity_tw.withColumn("sentiment", sentiment_func_udf(col("polarity")))

    # Final tweet logging
    query = sentiment_tw.writeStream.queryName("final_tweets_reg") \
        .outputMode("append").format("console") \
        .option("truncate", False) \
        .start().awaitTermination(60)

    # Parquet file dumping
    parquets = sentiment_tw.repartition(1)
    query2 = parquets.writeStream.queryName("final_tweets_parquet") \
        .outputMode("append").format("parquet") \
        .option("path", "./parc") \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='60 seconds').start()
    query2.awaitTermination(60)

    print("Process finished")
