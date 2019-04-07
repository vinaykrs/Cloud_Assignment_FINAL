#!/usr/bin/python

import sys

def task1_mapper():
    """ This mapper select tags and return the tag-owner information.
    Input format: photo_id \t owner \t tags \t date_taken \t place_id \t accuracy
    Output format: tag \t owner
    """
    line_count = 0
    for line in sys.stdin:
        # Clean input and split it
        lines = line.strip().split(",")
        line_count += 1
        # Check that the line is of the correct format
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
            #print("{}\t{}\t{}".format(category,videoid,country))

if __name__ == "__main__":
    task1_mapper()
