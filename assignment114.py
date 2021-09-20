#!/usr/bin/python3
import sys
from os import walk
import os.path
from map_reduce_lib import *
from datetime import datetime


def mapper_combine_tracks_ph(line):
    """ Map function for the combine job.
    Read tracks, playhistory and and extract needed data
    """
    # process_print('is processing `%s`' % line)
    output = []

    data = line.split(',')
    if data[0] != 'track_id' and data[0] != 'id':
        # ...if it is part of playhistory, which has 3 columns
        if len(data) == 3:
            track_id, user, dtime = data
            # ...write the relevant lines to the standard output
            output.append((track_id, '%s,%s' % ('PH', user)))
        # ...if it is part of tracks, which has 4 lines
        elif len(data) == 4:
            track_id, artist, title, length_seconds = data
            output.append((track_id, '%s,%s' % ('T', artist)))
        elif len(data) > 4:
            is_people = False
            if len(data) == 7 and (data[4] == 'Male' or data[4] == 'Female'):
                is_people = True
            if not is_people:
                output.append((data[0], '%s,%s' % ('T', data[1])))

    return output


def reducer_combine_tracks_ph(key_value_item):
    """ Reduce function for the combine job.
    Join data from table tracks and playhistory
    """
    artist = ""

    key, values = key_value_item
    for v in values:
        data = v.split(',')
        if data[0] == 'T':
            artist = data[1]
    return artist, values[1:]


def mapper_count(line):
    """ Map function for the count job.
        Read from file_contents all the find and and extract needed data
        """
    output = []
    data = line.split(',')
    if data[0] != 'track_id' and data[0] != 'id':
        # ...if it is part of playhistory, which has 3 columns
        if len(data) == 2:
            artist, user = data
            # ...write the relevant lines to the standard output
            output.append((user, artist))
        # ...if it is part of tracks, which has 4 lines
        elif len(data) == 7 and 'TRA' not in data[0]:
            id, first_name, last_name, email, gender, country, dob = data

            output.append((id, '%s,%s' % (first_name, last_name)))
    return output


def reducer_count(key_value_item):
    """ Reduce function for the combine job.
        Join data from last mapper reducer result and people table
        Count the listened the most artist
    """
    artists = []
    key, values = key_value_item
    first_name, last_name = values[0].split(',')
    # listend times
    records = values[1:]
    records_dict = dict((i, records.count(i)) for i in records)
    records = sorted(records_dict.items(), key=lambda item: item[1], reverse=True)
    count = records[0][1]
    for record in records:
        if record[1] == count:
            artists.append(record[0])
    return first_name, last_name, artists, count


if __name__ == '__main__':
    file_contents = []
    filenames = []
    combined_input = []
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
    combine_map_reduce = MapReduce(mapper_combine_tracks_ph, reducer_combine_tracks_ph, 8)
    combine_result = combine_map_reduce(file_contents, debug=True)
    # append the combine result to file_contents
    for result in combine_result:
        artist = result[0]
        for listen in result[1]:
            symbol, user = listen.split(',')
            file_contents.append('%s,%s' % (artist, user))
    # take the appended file_contetns for result
    count_map_reduce = MapReduce(mapper_count, reducer_count, 8)
    count_result = count_map_reduce(file_contents, debug=True)

    print('Artist each user listened the most to:')
    for firstname, lastname, artists, count in count_result:
        for artist in artists:
            print('{0}\t{1}\t{2}\t{3}'.format(firstname, lastname, artist, count))
