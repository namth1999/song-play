#!/usr/bin/python3
import sys
from os import walk
import os.path
from map_reduce_lib import *
from datetime import datetime


def mapper(line):
    """ Map function for the word count job.
    Splits line into words, removes low information words (i.e. stopwords) and outputs (key, 1).
    """
    # process_print('is processing `%s`' % line)
    output = []
    data = line.split(',')
    if data[0] != 'track_id' and data[0] != 'id':
        # ...if it is part of playhistory, which has 5 columns
        if len(data) == 3:
            track_id, user, dtime = data
            # ...write the relevant lines to the standard output
            output.append((user, '{0},{1}'.format('PH', datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S").hour)))
        # ...if it is part of peoples, which has 7 lines
        elif len(data) == 7 and 'TRA' not in data[0]:
            # ...write the relevant lines to the standard output
            output.append((data[0], '%s,%s,%s' % ('P', data[1], data[2])))
    return output


def reducer(key_value_item):
    """ Reduce function for the word count job.
    Converts partitioned shakespear (key, [value]) to a summary of form (key, value).
    """
    result = []

    current_firt_name = ""
    current_last_name = ""
    play_history = []
    current_people_id, values = key_value_item
    for v in values:
        data = v.split(',')
        if data[0] == 'P':
            current_firt_name = data[1]
            current_last_name = data[2]
        elif data[0] == 'PH':
            play_history.append(data[1])

    listen_times = dict((i, play_history.count(i)) for i in play_history)
    listen_times = sorted(listen_times.items(), key=lambda item: item[1], reverse=True)

    count = listen_times[0][1]
    for lt in listen_times:
        if lt[1] == count:
            result.append(lt[0])

    return current_firt_name, current_last_name, result, count


if __name__ == '__main__':
    file_contents = []
    filenames = []
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
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

    print('Hour listen the most')
    for firstname, lastname, hours, count in listen_counts:
        for hour in hours:
            print('{0}\t{1}\t{2}\t{3}'.format(firstname, lastname, hour, count))
