# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Example of using MapReduce in Pure Python.
This implementation does its best to follow the Robert Martin's Clean code guidelines.
The comments follows the Google Python Style Guide:
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

__copyright__ = 'Copyright 2021, FCRlab at University of Messina'
__author__ = 'Lorenzo Carnevale <lcarnevale@unime.it>'
__credits__ = ''
__description__ = 'MapReduce in Pure Python'


import operator
import argparse
from mapreduce import MapReduce


def main():
    description = ('%s\n%s' % (__author__, __description__))
    epilog = ('%s\n%s' % (__credits__, __copyright__))
    parser = argparse.ArgumentParser(
        description = description,
        epilog = epilog
    )

    parser.add_argument('-i', '--input',
                        dest='input',
                        help='Text file in input',
                        type=str,
                        required=True)

    parser.add_argument('-t', '--top',
                        dest='top',
                        help='Maximum number of words to print out',
                        type=int,
                        default=50)    

    options = parser.parse_args()
    num_words = options.top

    data = read_data(options.input)


    mapreduce = MapReduce(mapper, reducer)
    word_counts = mapreduce(data)
    word_counts.sort(key=operator.itemgetter(1))
    word_counts.reverse()
    
    print('\nTOP %d WORDS BY FREQUENCY\n' % (num_words))
    show = word_counts[:num_words]
    longest = max(len(word) for word, count in show)
    for word, count in show:
        print('%-*s: %5s' % (longest+1, word, count))


def read_data(filename):
    """Read data file.

    Args:
        filename(str): name of data file.
    
    Returns:
        a list of text lines.
    """
    with open(filename, encoding = "ISO-8859-1") as f:
        data = f.readlines()
    return data

def mapper(data):
    """Define mapper function.

    Function to map inputs to intermediate data. Takes as
    argument one input value and returns a tuple with the key
    and a value to be reduced.
    The preprocessing is herein included.

    Args:
        data(list<str>): portion of text

    Returns:
        output(list<tuple>): list of <word-value> data
    """
    output = list()
    words = data.split()
    for word in words:
        word = word.lower()
        if word.isalpha():
            output.append( (word, 1) )
    return output

def reducer(item):
    """Define reducer function.

    Function to reduce partitioned version of intermediate data
    to final output. Takes as argument a key as produced by
    mapper and a sequence of the values associated with that
    key.

    Args:
        item(tuple): word-values data structure

    Returns:
        a tuple with all summed values.
    """
    word, occurances = item
    return (word, sum(occurances))


if __name__ == '__main__':
    main()