#!/usr/bin/python3
import sys
import operator
from os import walk
import os.path
from map_reduce_lib import *
from datetime import datetime

def mapper(line):
    """ Map function for the listen count job.
    Read from play_history file and splits line into track_id, user, date_time
    """
    # process_print('is processing `%s`' % line)
    output = []
    data = line.split(',')
    if len(data) == 3 and data[0] != 'track_id':
        track_id, user, dtime = data
        if datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S").month == 3 and datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S").year == 2015:
            output.append((track_id, 1))
    return output


def reducer(key_value_item):
    """ Reducer function for the listen count job.
    Sum the count
    """
    key, counts = key_value_item
    return key, sum(counts)


if __name__ == '__main__':
    file_contents = []
    filenames = []
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
    # process whether the input is a dir or a file
    elif os.path.isdir(sys.argv[1]):
        filenames = next(walk(sys.argv[1]), (None, None, []))[2]
        for fn in filenames:
            with open('%s/%s' % (sys.argv[1], fn), 'r') as input_file:
                file_contents.extend(input_file.read().splitlines())
    elif os.path.isfile(sys.argv[1]):
        with open(sys.argv[1], 'r') as input_file:
            file_contents.extend(input_file.read().splitlines())
    elif not os.path.isdir(sys.argv[1]) or not os.path.isfile(sys.argv[1]):
        print('File or dir `%s` not found.' % sys.argv[1])
        sys.exit(-1)

    # Execute MapReduce job in parallel.
    map_reduce = MapReduce(mapper, reducer, 8)
    listen_counts = map_reduce(file_contents, debug=True)

    # Sort the listen_counts by track_id
    listen_counts.sort(key=operator.itemgetter(0))

    print('How often songs were listened on March 2015:')
    for word, count in listen_counts:
        print('{0}\t{1}'.format(word, count))
