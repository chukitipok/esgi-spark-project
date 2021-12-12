from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, Row, StringType, StructField, StructType


class GithubReposDataFrame:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.dataset = spark.read.option('header', 'true').csv('./resources/full.csv')
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.stop_words = self.__prepare_stop_words()

    def __prepare_stop_words(self):
        df = self.spark.read.option('header', 'true').text('./resources/stop_words_en.txt')
        stop_words = [row.value for row in df.collect()]

        rdd = self.spark.sparkContext.parallelize([Row('tmp', stop_words)])

        schema = StructType([
            StructField("tmp", StringType(), False),
            StructField("values", ArrayType(StringType(), False), False)
        ])

        return self.spark.createDataFrame(rdd, schema)
    
    def repos_with_most_commits(self, limit: int = 10) -> DataFrame:
        return self.dataset.select('repo')\
            .where('repo is not null')\
            .groupBy('repo')\
            .count()\
            .orderBy('count', ascending =False)\
            .withColumnRenamed('repo', 'Projects')\
            .withColumnRenamed('count', 'Commits number')\
            .limit(limit)

    def best_contributor(self, project_name: str) -> str:
        row = self.dataset.select('author')\
            .where(col('repo') == project_name)\
            .groupBy('author')\
            .count()\
            .orderBy('count', ascending = False)\
            .first()\
            
        return row.author if row is not None else 'No commits made on "' + project_name + '".'

    def best_contributor_on_last_x_months(self, project_name: str, last_months: int) -> str:
        pattern = "E LLL d HH:mm:ss yyyy Z"
        row = self.dataset.select('author', 'date', months_between(current_date(), to_date('date', pattern)).alias('months'))\
            .where((col('repo') == project_name) & (col('months') <= last_months) & (col('months') >= 0))\
            .groupBy('author')\
            .count()\
            .orderBy('count', ascending = False)\
            .first()\
            
        return row.author if row is not None else 'No commits made the last ' + str(last_months) + ' months on "' + project_name + '".'

    def words_most_used_in_commits(self, limit: int = 10) -> DataFrame:        
        return self.dataset.withColumn('tmp', lit('tmp')) \
            .join(self.stop_words, on = ['tmp']) \
            .drop('tmp') \
            .withColumnRenamed('values', 'stop_words') \
            .select(array_except(split('message', ' '), col('stop_words')).alias('messages')) \
            .withColumn('word', explode(col('messages'))) \
            .groupBy('word') \
            .count() \
            .sort('count', ascending = False) \
            .limit(limit)
