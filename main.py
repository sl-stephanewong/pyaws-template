# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from src.utils.config import *
from src.tasks.aggregations.sample_task import SampleTask
from src.tasks.aggregations.movie_tasks import MovieNameTask
from src.tasks.aggregations.books_tasks import BookTask

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # aws = AWSUtil()
    # cred = aws.get_credentials()
    # c = cred.get_frozen_credentials()
    #task = SampleTask()
    #task = MovieNameTask()
    task = BookTask()
    task.run()
