#!/usr/bin/python3

import sys

def task1_mapper():
    """ This mapper select video_id, Category and country information and returns category and video_id as key and country as value.
    Input format: video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country
    Output format: category_id,video_id \t country
    """
    line_count = 0
    for line in sys.stdin:
        # Clean input and split it
        lines = line.strip().split(",")
        line_count += 1
        # Check that the line is of the correct format and filtering the HEADER record 
        # If line is malformed, we ignore the line and continue to the next line
        if line_count == 1:
            continue
        else:
            if len(lines) != 12:
                continue
            
            category = lines[3].strip()
            videoid = lines[0].strip()
            country = lines[11].strip()
            k_key = category+','+videoid

            print("{}\t{}".format(k_key, country))

if __name__ == "__main__":
    task1_mapper()
