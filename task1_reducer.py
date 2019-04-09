#!/usr/bin/python3

"""
This program will use two dictionaries to produce the desired result.
One dictionary to capture the Categories and other to capture videos for each category.
Once the category dictionary is build, key values for each category will be used to compute the average   

"""



import csv
import sys
from collections import defaultdict


def task1_reducer():
    """ This reducer takes input from mapper :: key as  category,videoid and value as country
        and produces the average number of countries for videos in each category
        Output: Music: 1.31
                News & Politics: 1.05
                ....
    """
    # Initializing all required variables required to produce the desired putput
    last_category_name = None
    last_video_id = None
    category_name = ""
    video_id = None
    country_name = ""
    cat_dict = defaultdict(dict)
    video_dict = defaultdict(list)
    # Reading the data from input and looping through each record 
    for line in sys.stdin:
        # Clean input and split it
        lines = line.strip().split("\t")
        #Splitting the key as key has two values concatenated with comma
        category_name,video_id = lines[0].split(",")
        country_name = lines[1]
        #Checking if the last category is same as the current category based on that either we insert the video values in to category dict or continue capturing the items into video dict
        # Conditional driven - Constructs Video Id Dictionary and Category Dictionary
        if not last_category_name or last_category_name != category_name:
            cat_dict[last_category_name] = video_dict
            video_dict = defaultdict(list)
            if not video_dict:
                last_category_name = category_name
                last_video_id = video_id
                list_cn = []
                list_cn.append(country_name)
                video_dict[video_id].append(list_cn)
            
            else:
                cat_dict[last_category_name] = video_dict
                video_dict = defaultdict(list)
        elif category_name == last_category_name:
            if not last_video_id or last_video_id != video_id:
                list_cn = []
                last_video_id = video_id
                list_cn.append(country_name)
                video_dict[video_id].append(list_cn)
            elif video_id == last_video_id:
                list_cn = []
                list_cn.append(country_name)
                video_dict[video_id].append(list_cn)
    cat_dict[last_category_name] = video_dict
    #Ensuring there are no NULL keys which might cause issues while calculating Averages
    del(cat_dict[None])
    
    #Reading the category dict and calculating the averages 
    main_dict = defaultdict(list)
    lst = []
    for key,value in cat_dict.items():
        lst = []
        for k,v in value.items():
            st = set()
            for i in range(len(v)):
                st.update(set(v[i]))
            lst.append(len(st))
        agg_avg = round(sum(lst)/len(lst),2)
        main_dict[key]= agg_avg
    for k in main_dict:
        print("{}\t{}".format(k, main_dict[k])) 

if __name__ == "__main__":
    task1_reducer()
