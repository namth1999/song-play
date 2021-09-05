import collections
from termcolor import cprint, colored
import itertools
import multiprocessing

class MapReduce(object):
    """
        map_func
          Function to map inputs to intermediate shakespear. Takes as
          argument one input value and returns a tuple with the key
          and a value to be reduced.
        reduce_func
          Function to reduce partitioned version of intermediate shakespear
          to final output. Takes as argument a key as produced by
          map_func and a sequence of the values associated with that
          key.
        num_workers
          The number of workers to create in the pool. Defaults to the
          number of CPUs available on the current host.
        """
    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
        self.num_workers = num_workers

    """
        Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    """
    Process the inputs through the map and reduce functions given.
        
        inputs
          An iterable containing the input shakespear to be processed.
        
        chunksize=1
          The portion of the input shakespear to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
    def __call__(self, inputs, chunksize=1, debug=False):

        if debug:
          cprint('=== Mapping to %d mappers with chunk size %d... ===' % (self.num_workers, chunksize), 'red', attrs=['bold'])
        
        # Map and partition
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        map_responses = filter(None, map_responses)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        
        if debug:
          cprint('=== Mapper returned %d keys ===' % len(partitioned_data), 'red', attrs=['bold'])
          cprint('=== Reducing using %d reducers... ===' % self.num_workers, 'red', attrs=['bold'])

        # Reduce
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)

        if debug:
          cprint('=== Reducer finished ===', 'red', attrs=['bold'])

        return reduced_values

def process_print(*args):
  print(colored('[' + multiprocessing.current_process().name + ']', 'yellow'), *args)
