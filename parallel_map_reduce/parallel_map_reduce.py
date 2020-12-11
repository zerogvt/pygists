import time
from multiprocessing import Pool


def map_reduce(data,
               mapf, reducef,
               pool_size, batch_size, cool_secs,
               indicator=True):
    '''
    A map reduce implementation.
    The map phase is executed by a set of parallel subprocesses pool of size
    'pool_size'. Throttling is supported via 'batch_size' and 'cool_secs'.
    There will be 'cool_secs' of inactivity in between each batch of mappers
    start executing. The interface between mappers and the reducer are files.
    If 'indicator' is set there will be a . output in console for every data
    item that gets mapped.
    * mapf should be a function saving its results in a file and it should
    return the full path of the file.
    * reducef should be a function that gets a list of (aforementioned)
    files as its input and returns the total result of the computation.
    '''
    map_res = []
    ind = 0
    batch = []
    with Pool(pool_size) as p:
        while ind < len(data):
            batch.clear()
            while len(batch) < batch_size and ind < len(data):
                batch.append(data[ind])
                ind += 1
            for res in p.map(mapf, batch):
                map_res.append(res)
                if indicator:
                    print(f'{ind} ', end='', flush=True)
            time.sleep(cool_secs)
    return reducef(map_res)
