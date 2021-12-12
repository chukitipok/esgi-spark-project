from pyspark.sql import SparkSession
from github_repos_dataframe import GithubReposDataFrame

if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark-project').getOrCreate()

    github_repo = GithubReposDataFrame(spark)

    print()
    print('The 10 projects with the most commits are:')
    github_repo.repos_with_most_commits().show(truncate=False)

    print('The best contributor on "apache/spark" project is: ' + github_repo.best_contributor('apache/spark'))
    print()

    print('The best contributor on "apache/spark" project for the last 6 months is: ' + github_repo.best_contributor_on_last_x_months('apache/spark', 6))
    print()

    spark.stop()
