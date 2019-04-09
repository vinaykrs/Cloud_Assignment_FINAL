import csv
from datetime import datetime

"""
This module includes a few functions used in identifying the controversial trending videos
"""

def extractVideo(record):
    """ This function converts entries of changed.csv into key,value pair of the following format
    (video_id, trending_date,category,likes,dislikes,country)
    Args:
        record (str): A row of CSV file, with 12  columns separated by comma
    Returns:
        The return value is a tuple (video_id, trending_date,category,likes,dislikes,country)
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
    """
    This function converts the record in to key value pairs, as below
    (video_id+','+country, (trending_date,category,likes,dislikes,1))
    Appending 1 at the end of each record. This will help to filter the records where there is only one record for each key
    Args:
        record (str): A row with 6  columns separated by comma
    Returns:
        The return value is a tuple (video_id+','+country, (trending_date,category,likes,dislikes,1))
    """
    try:
        video_id, trending_date, category, likes,dislikes, country = record
        return(video_id+','+country, (trending_date,category,likes,dislikes,1))
    except:
        return()


def recordFormat(record):
    """
    This function is used to split the KEY in to 2 values based on the comma seperator

    """
    try:
        key,trending_date, category,growth = record
        id,country =key.split(",")
        return(id,growth,category,country)
    except:
        return()
