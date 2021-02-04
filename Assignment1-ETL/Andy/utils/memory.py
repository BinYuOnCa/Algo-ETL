import os
import psutil
from .config import b_to_mb


def memory_using():
    '''
    returns the current memory usage
    '''
    pid = os.getpid()
    p = psutil.Process(pid)
    info = p.memory_full_info()
    curr_memory = info.uss / b_to_mb
    #print('{} MB'.format(curr_memory))
    return curr_memory
