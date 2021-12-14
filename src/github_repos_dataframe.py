from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *


class GithubReposDataFrame:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.dataset = spark.read.option('header', 'true').csv('./resources/full.csv')
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.stop_words = None

    def __prepare_stop_words(self):
        words_df = self.spark.read.option('header', 'true').text('./resources/stop_words_en.txt')
        return [row.value for row in words_df.collect()]
    
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
        pattern = 'E LLL d HH:mm:ss yyyy Z'
        row = self.dataset.select('author', 'date', months_between(current_date(), to_date('date', pattern)).alias('months'))\
            .filter((col('repo') == project_name) & (col('months') <= last_months) & (col('months') >= 0))\
            .groupBy('author')\
            .count()\
            .orderBy('count', ascending = False)\
            .first()\
            
        return row.author if row is not None else 'No commits made the last ' + str(last_months) + ' months on "' + project_name + '".'

    def words_most_used_in_commits(self, limit: int = 10) -> DataFrame:
        self.stop_words = self.__prepare_stop_words()
        return self.dataset.where(col('message').isNotNull()) \
            .select(explode(split('message', ' ')).alias('words')) \
            .where(col('words').isin(self.stop_words) == False) \
            .groupBy(lower(col('words')).alias('words')) \
            .count() \
            .orderBy(col('count'), ascending = False) \
            .limit(limit)
