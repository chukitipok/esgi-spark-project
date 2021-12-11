from pyspark.sql.dataframe import DataFrame

class GithubReposDataFrame:

    def __init__(self, spark) -> None:
        self.dataset = spark.read.option('header', 'true').csv('./resources/full.csv')

    def repos_with_most_commits(self, limit = 10) -> DataFrame:
        return self.dataset.select('repo')\
            .where('repo is not null')\
            .groupBy('repo')\
            .count()\
            .orderBy('count', ascending=False)\
            .withColumnRenamed('repo', 'Projects')\
            .withColumnRenamed('count', 'Commits number')\
            .limit(limit)

    def best_contributor(self, project_name) -> str:
        row = self.dataset.select('author')\
            .where(self.dataset.repo == project_name)\
            .groupBy('author')\
            .count()\
            .orderBy('count', ascending=False)\
            .first()\
            
        return row.author if row is not None else 'No commits made on "' + project_name + '".'
