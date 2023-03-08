"""
ThreadPool available from DacBase for managing data ingestion tasks.
"""
from threading import Thread
from threading import Event

class ThreadPool:
    def __init__(self, size=10):
        # Thread pool for publishing topics
        self._thread_pool_dict = {}

        # Thread pool max size
        self._max_thread_pool = size

    def _get_thread_name(self, ds_type, ds_name):
        '''Returns the unique thread name for the specified data structure type and name.'''
        return ds_type + ":" + ds_name

    def _get_ds_info(self, thread_name):
        '''Returns parsed ds_type and ds_name of the specified thread name.'''
        x = thread_name.split(':')
        return x[0], x[1]

    def stop_all(self):
        '''Stops and releases all active threads in the pool.'''
        for thread_name, thread_info_list in self._thread_pool_dict.items():
            thread_event = thread_info_list[1]
            thread_event.set()
        self._thread_pool_dict.clear()

    def refresh(self, ds_type, ds_name_list):
        '''Refreshes the thread pool by removing all threads that are not
        in the specified ds_name_list.'''
        removal_thread_name_list = []
        for thread_name, thread_info_list in self._thread_pool_dict.items():
            t_ds_type, t_ds_name = self._get_ds_info(thread_name)
            if t_ds_type == ds_type and t_ds_name in ds_name_list == False:
                thread_event = thread_info_list[1]
                thread_event.set()
                removal_thread_name_list.append(thread_name)
        for thread_name in removal_thread_name_list:
            del self._thread_pool_dict[thread_name]

    def is_max(self):
        '''Returns true if the thread pool reached the max size.'''
        return len(self._thread_pool_dict) >= self._max_thread_pool

    def get_max(self):
        return self._max_thread_pool
    
    def stop_thread(self, ds_type, ds_name):
        '''Stops thread and remove it from the thread pool.'''
        thread_name = self._get_thread_name(ds_type, ds_name)
        if thread_name in self._thread_pool_dict:
            thread_info_list = self._thread_pool_dict[thread_name]
            thread_event = thread_info_list[1]
            thread_event.set()
            del self._thread_pool_dict[thread_name]        

    def get_active_thread_count(self):
        '''Returns the active thread count.'''
        return len(self._thread_pool_dict)

    def get_thread(self, ds_type, ds_name, target, args=()):
        '''Returns the Thread and Event identified by the specified ds_type and ds_name. It creates
        a new thread if not found.'''
        thread_name = self._get_thread_name(ds_type, ds_name)
        if thread_name in self._thread_pool_dict:
            thread_info_list = self._thread_pool_dict[thread_name]
        else:
            thread_event = Event() 
            t_args = args + (thread_event,)
            thread = Thread(target=target, args=t_args)
            thread_info_list = [thread, thread_event]
            self._thread_pool_dict[thread_name] = thread_info_list
        return thread_info_list[0], thread_info_list[1]

    def get_ds_names(self, ds_type):
        '''Returns a list of all data structure names for the specified ds_type.'''
        ds_names = []
        for thread_name in self._thread_pool_dict.keys():
            t_ds_type, t_ds_name = self._get_ds_info(thread_name)
            if t_ds_type == ds_type:
                ds_names.append(t_ds_name)
        return ds_names