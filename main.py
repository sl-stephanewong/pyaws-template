import sys

from pyspark import SparkContext, SparkConf
from src.tasks.aggregations.books_tasks import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #aws = AWSUtil()
    #cred = aws.get_credentials()
    #c = cred.get_frozen_credentials()
    #print(c)
    #task = SampleTask()
    #task = MovieNameTask()
    #from .src.tasks.aggregations.books_tasks import BookTask
    task = BookTask()
    task.run()
