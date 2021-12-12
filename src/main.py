import os
from pyspark.sql import SparkSession
from github_repos_dataframe import GithubReposDataFrame

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('spark-project').getOrCreate()
    spark.sparkContext.setLogLevel('OFF')
    github_repo = GithubReposDataFrame(spark)
    
    os.system('clear')

    # Question 1:
    print('The 10 projects with the most commits are:')
    github_repo.repos_with_most_commits().show(truncate = False)

    # Question 2:
    print('The best contributor on "apache/spark" project is: ' + github_repo.best_contributor('apache/spark'))
    print()

    # Question 3:
    print('The best contributor on "apache/spark" project for the last 6 months is: ' + github_repo.best_contributor_on_last_x_months('apache/spark', 6))
    print()

    # Question 4:
    print('The 10 words most used in commits message are:')
    github_repo.words_most_used_in_commits().show(truncate = False)

    spark.stop()
