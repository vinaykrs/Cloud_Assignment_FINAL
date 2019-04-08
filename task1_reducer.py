#!/usr/bin/python

import csv
import sys
from collections import defaultdict


def task1_reducer():
    """ This mapper select tags and return the tag-owner information.
    Input format: photo_id \t owner \t tags \t date_taken \t place_id \t accuracy
    Output format: tag \t owner
    """
    last_category_name = None
    last_video_id = None
    category_name = ""
    video_id = None
    country_name = ""
    cat_dict = defaultdict(dict)
    video_dict = defaultdict(list)
    
    for line in sys.stdin:
        # Clean input and split it
        lines = line.strip().split("\t")
        # If line is malformed, we ignore the line and continue to the next line
        #category_name = lines[0]
        #video_id = lines[1]
        #country_name = lines[2]
        category_name,video_id = lines[0].split(",")
        country_name = lines[1]
        if not last_category_name or last_category_name != category_name:
            category_name,video_id = line[0].split(",")
            country_name = line[1]
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
    del(cat_dict[None])
    
    
    main_dict = defaultdict(list)
    lst = []
    for key,value in cat_dict.items():
        lst = []
        for k,v in value.items():
            st = set()
            for i in range(len(v)):
                st.update(set(v[i]))
            #print(key,len(st))
            lst.append(len(st))
        agg_avg = sum(lst)/len(lst)
        main_dict[key]= agg_avg
    #print(main_dict)
    for k in main_dict:
        print("{}\t{}".format(k, main_dict[k]))

if __name__ == "__main__":
    task1_reducer()
