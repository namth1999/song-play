#!/usr/bin/python3
import sys
from os import walk
import os.path
from map_reduce_lib import *
from datetime import datetime

start_hour = 7
end_hour = 8

def mapper(line):
    """ Map function for the listen count job.
    Read all the file and extract needed data
    """
    # process_print('is processing `%s`' % line)
    output = []

    data = line.split(',')
    if data[0] != 'track_id' and data[0] != 'id':
        # ...if it is part of playhistory, which has 3 columns
        if len(data) == 3:
            track_id, user, dtime = data
            if start_hour <= datetime.strptime(dtime, "%Y-%m-%d %H:%M:%S").hour < end_hour:
                # ...write the relevant lines to the standard output
                output.append((track_id, '1'))
        # ...if it is part of tracks, which has 4 lines
        elif len(data) == 4:
            track_id, artist, title, length_seconds = data
            output.append((track_id, '%s,%s,%s' % ('A', title, artist)))
        elif len(data) > 4:
            is_people = False
            if len(data) == 7 and (data[4] == 'Male' or data[4] == 'Female'):
                is_people = True
            # Handle tracks with one or many ',' in its title
            if not is_people:
                start_title_index = line.find(",\"")
                end_title_index = line.find("\",")
                title_data = line.split("\"")
                if start_title_index != -1 and end_title_index != -1 and end_title_index > start_title_index:
                    output.append((data[0], '%s,%s,%s' % ('B', '"' + title_data[1] + '"', data[-1])))

    return output


def reducer(key_value_item):
    """ Reducer function for the listen count job.
    Return 5 songs played the most
    """
    title = ""
    artist = ""
    play_times = -1
    result = []
    key, values = key_value_item
    # extract data
    if len(values) > 1:
        for v in values:
            if "A" in v[0]:
                song_data = v.split(',')
                title = song_data[1]
                artist = song_data[2]
            elif "B" in v[0]:
                song_data = v.split(',')
                title_data = v.split('"')
                title = title_data[1]
                artist = song_data[-1]

        play_times = sum([int(i) for i in values[1:] if type(i) == int or i.isdigit()])
        return title, artist, play_times


def not_None(value):
    return value is not None


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
    an_iterator = filter(not_None, listen_counts)
    listen_counts = list(an_iterator)
    # get the top 5 songs
    listen_counts.sort(key = lambda x: x[2], reverse=True)
    top_5 = listen_counts[0:5]
    print('The 5 Songs played the most at {0} to {1}'.format(start_hour, end_hour))
    for song in top_5:
        print('{0}\t{1}\t{2}'.format(song[0], song[1], song[2]))
