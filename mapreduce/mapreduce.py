# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Simple Implementation of MapReduce in Pure Python.
This implementation does its best to follow the Robert Martin's Clean code guidelines.
The comments follows the Google Python Style Guide:
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

import itertools
import collections
import multiprocessing as mp

class MapReduce(object):
    
    def __init__(self, mapper, reducer, num_workers=None):
        """MapReduce constructor.

        Args:
          mapper(function): defined mapper
          reducer(function): defined reducer
          num_workers(int,default=CPU cores): number of processes to run
        """
        self.mapper = mapper
        self.reducer = reducer
        self.pool = mp.Pool(processes=num_workers)
    
    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        
        Returns:
          an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()
    
    def __call__(self, inputs, chunksize=1):
        """Process the inputs through the map and reduce functions given.
        
        Args:
          inputs(list<str>): an iterable containing the input data to be processed.
          chunksize(int,default=1): the portion of the input data to hand to each worker.
            This can be used to tune performance during the mapping phase.

        Returns:
          the output of the reducer function.
        """
        map_responses = self.pool.map(self.mapper, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reducer, partitioned_data)
        return reduced_values