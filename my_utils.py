import csv
from datetime import datetime

"""
This module includes a few functions used in computing average rating per genre
"""

def extractVideo(record):
    """ This function converts entries of changed.csv into key,value pair of the following format
    (movieID, rating)
    Args:
        record (str): A row of CSV file, with four columns separated by comma
    Returns:
        The return value is a tuple (movieID, genre)
    """
    try:
        video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country = record.split(",")
        trending_date = datetime.strptime(trending_date, '%y.%d.%m').strftime('%Y%m%d')
        dislikes = int(dislikes)
        likes = int(likes)
        return (video_id, trending_date,category,likes,dislikes,country)
    except:
        return ()


def extractKeyValue(record):
    try:
        video_id, trending_date, category, likes,dislikes, country = record
        return(video_id+','+country, (trending_date,category,likes,dislikes,1))
    except:
        return()


def recordFormat(record):
    try:
        key,trending_date, category,growth = record
        id,country =key.split(",")
        return(id,growth,category,country)
    except:
        return()
