import json
import time
import random
from faker import Faker
from  hazelcast.core import HazelcastJsonValue

from padogrid.hazelcast.playground.nw_portable import Customer
from padogrid.hazelcast.playground.nw_portable import Order

class DataIngestor():
    fake = Faker()
    fake_customer = Faker()
    fake_order = Faker()
    customerId_prefix = "000-"

    def __init__(self, er_dict={}, hazelcast_cluster=None):
        self.er_dict = er_dict
        self.hazelcast_cluster = hazelcast_cluster

    def get_class(self, kls):
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m

    def _create_er_objects(self, key, obj, is_dict, er, key_type='random', key_range=100_000_000):
        er_keys = []
        er_objects = []
        if er.otm_type == 'exact':
            count = er.otm
        else:
            count = random.randint(0, er.otm)
        for i in range(count):
            er_obj, er_key = er.create_function(er_key=key, key_type=key_type, key_range=key_range)
            for attr in er.attribute_list:
                if attr.from_attr == '__key':
                    er_obj, er_key = er.create_function(key_type=key_type, key_range=key_range)
                    if er.is_dict:
                        er_obj[attr.to_attr] = key
                    else:
                        setattr(er_obj, attr.to_attr, key)
                else:
                    if is_dict: 
                        attr_value = obj[attr.from_attr]
                    else:
                        attr_value = getattr(obj, attr.from_attr)
                    if er.is_dict:
                        er_obj[attr.to_attr] = attr_value
                    else:
                        setattr(er_obj, attr.to_attr, attr_value)
            if er.is_dict:
                er_obj = HazelcastJsonValue(json.dumps(er_obj))
            er_keys.append(er_key)
            er_objects.append(er_obj)
        return er_keys, er_objects

    def _get_er_list(self, er_object_name):
        '''Returns from_er and to_er_list constructed based on er info mapped by the specified er_object_name.
        Args:
            er_object_name: ER object name to look up self.er_dict
        
        Returns:
            from_er: FromEr object. None if undefined in self.er_dict.
            to_er_list: A list of ToEr objects. Empty if undefined in self.er_dict.
        '''
        to_er_list = []
        if er_object_name != None and self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            if er_object_name in self.er_dict:
                er_list = self.er_dict[er_object_name]
                for er in er_list:
                    ds_name = er['ds_name']
                    key = er['key']
                    object = er['object']
                    create_function = er['function']
                    attributes = er['attributes']
                    otm = er['otm']
                    otm_type= er['otm-type']
                    to_er = ToEr(ds_name, key, object, create_function, attributes, otm, otm_type)
                    to_er_list.append(to_er)
        return to_er_list

    def _update_er_list(self, ds, create_function, er_object_name, key_type='random', key_range=100_000_000):
        '''
        Returns er_list complete with data structure instance and is_dict

        Args:
            ds: Data structure instance.
            create_function: Object creation function.
            er_object_name: ER object name.

        Returns: (Tuple)
            True if create_function's object is dictionary.
            er_list
        '''
        # Construct ER list
        er_list = self._get_er_list(er_object_name)
        obj_type = str(type(ds))
        for er in er_list:
            if 'hazelcast.proxy.list' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_list(er.ds_name).blocking()
            elif 'hazelcast.proxy.map' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_map(er.ds_name).blocking()
            elif 'hazelcast.proxy.multi_map' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_multi_map(er.ds_name).blocking()
            elif 'hazelcast.proxy.queue' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_queue(er.ds_name).blocking()
            elif 'hazelcast.proxy.reliable_topic' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_reliable_topic(er.ds_name).blocking()
            elif 'hazelcast.proxy.replicated_map' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_replicated_map(er.ds_name).blocking()
            elif 'hazelcast.proxy.ringbuffer' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_ringbuffer(er.ds_name).blocking()
            elif 'hazelcast.proxy.set' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_set(er.ds_name).blocking()
            elif 'hazelcast.proxy.topic' in obj_type:
                er.ds = self.hazelcast_cluster.hazelcast_client.get_topic(er.ds_name).blocking()

        # Determine dictionary objects which will be converted to JSON in the loop
        obj, key = create_function(key_type=key_type, key_range=key_range)
        is_dict = isinstance(obj, dict)
        for er in er_list:
            er_obj, er_key = er.create_function(key_type=key_type, key_range=key_range)
            er.is_dict = isinstance(er_obj, dict)
        return is_dict, er_list

    def ingest_flake_id_generator(self, ds_name, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           ds_name flake_id_generator by invoking new_id().
        
        Args:
            ds_name: List name. 
        '''
        if self.hazelcast_cluster != None:
            if self.hazelcast_cluster.hazelcast_client != None:
                fig = self.hazelcast_cluster.hazelcast_client.get_flake_id_generator(ds_name).blocking()
                delay = delay_in_msec / 1000
                last_index = count+1
                for i in range(1, last_index):
                    fig.new_id()
                    if progress != None:
                        progress.count = i
                    if thread_event != None and thread_event.is_set():
                        break
                    elif delay_in_msec > 0:
                        time.sleep(delay)

    def ingest_list(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           list by invoking add(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_list(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.add(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.add(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.add(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_map_set(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           map by invoking set(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.set(key, HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.set(key, obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.set(er_key, er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_map_put(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           map by invoking put(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.put(key, HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.put(key, obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.put(er_key, er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_map_put_all(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=1000, batch_size=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           map by invoking put_all(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            batch_map = {}
            last_index = count + 1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    batch_map[key] = HazelcastJsonValue(json.dumps(obj))
                else:
                    batch_map[key] = obj
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.batch[er_key] = er_objects[j]
                        j += 1
                if i % batch_size == 0:
                    ds.put_all(batch_map)
                    batch_map.clear()
                    for er in er_list:
                        er.ds.put_all(er.batch)
                        er.batch.clear()
                    if progress != None:
                        progress.count = i
                    if thread_event != None and thread_event.is_set():
                        break
                    elif delay_in_msec > 0:
                        time.sleep(delay)
            # Put the remaining batches
            if thread_event == None or thread_event.is_set() == False:
                if len(batch_map) > 0:
                    ds.put_all(batch_map)
                    batch_map.clear()
                    for er in er_list:
                        er.ds.put_all(er.batch)
                        er.batch.clear()

    def ingest_multi_map(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           multi_map by invoking put(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        ''' 
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_multi_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.put(key, HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.put(key, obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.put(er_key, er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_pn_counter(self, ds_name, count=100, delay_in_msec=0, thread_event=None, progress=None, ds_method_name='get_and_add', delta=1):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           ds_name pn_counter by invoking the specified method.
        
        Args:
            ds_name: PNCounter name. 
            ds_method_name: Name of the method to invoke. Allowed values are 'get_and_add', 'add_and_get',
                            'get_and_subtract', 'subtract_and_get'. Default: 'get_and_add'
        '''
        if self.hazelcast_cluster != None:
            if self.hazelcast_cluster.hazelcast_client != None:
                ds = self.hazelcast_cluster.hazelcast_client.get_pn_counter(ds_name).blocking()
                delay = delay_in_msec / 1000
                last_index = count+1
                for i in range(1, last_index):
                    if ds_method_name == 'get_and_add':
                        ds.get_and_add(delta)
                    elif ds_method_name == 'add_and_get':
                        ds.add_and_get(delta)
                    elif ds_method_name == 'get_and_subtract':
                        ds.get_and_subtract(delta)
                    elif ds_method_name == 'subtract_and_get':
                        ds.subtract_and_get(delta)
                    else:
                        ds.get_and_add(delta)
                    if progress != None:
                        progress.count = i
                    if thread_event != None and thread_event.is_set():
                        break
                    elif delay_in_msec > 0:
                        time.sleep(delay)

    def ingest_queue(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           queue by invoking offer(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_queue(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.offer(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.offer(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.offer(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_reliable_topic(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           topic by invoking publish(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_reliable_topic(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.publish(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.publish(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.publish(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay) 

    def ingest_replicated_map_put(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           replicated_map by invoking put(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_replicated_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.put(key, HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.put(key, obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.put(er_key, er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_replicated_map_put_all(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=1000, batch_size=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           replicated_map by invoking put_all(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_replicated_map(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            batch_map = {}
            last_index = count + 1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    batch_map[key] = HazelcastJsonValue(json.dumps(obj))
                else:
                    batch_map[key] = obj
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.batch[er_key] = er_objects[j]
                        j += 1
                if i % batch_size == 0:
                    ds.put_all(batch_map)
                    batch_map.clear()
                    for er in er_list:
                        er.ds.put_all(er.batch)
                        er.batch.clear()
                    if progress != None:
                        progress.count = i
                    if thread_event != None and thread_event.is_set():
                        break
                    elif delay_in_msec > 0:
                        time.sleep(delay)
            # Put the remaining batches
            if thread_event == None or thread_event.is_set() == False:
                if len(batch_map) > 0:
                    ds.put_all(batch_map)
                    batch_map.clear()
                    for er in er_list:
                        er.ds.put_all(er.batch)
                        er.batch.clear()

    def ingest_ringbuffer(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           reingbuffer by invoking add(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_ringbuffer(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.add(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.add(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.add(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)
                    
    def ingest_set(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           set by invoking add(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_set(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.add(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.add(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.add(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay)

    def ingest_topic(self, ds_name, create_function, er_object_name, key_type='random', key_range=100_000_000, count=100, delay_in_msec=0, thread_event=None, progress=None):
        '''Creates objects by invoking the specified create_function and ingests them in the specified
           topic by invoking publish(). If er_object_name is not None, then it creates one-to-many entity
           relationship (er) by passing the object key returned by create_function to ER objects as
           er_key and performs additional ER mappings defined in playground.yaml.

        Args:
            ds_name: Data structure name.
            create_function: Object creation function. This function must return the object and key to
                             be ingested.
            er_object_name: ER object name that maps entity relationships defined in playground.yaml.
            count: Number of objects to ingest in the data structure (ds_name).
            delay_in_msec: Delay between data structure calls.
            thread_event: Thread event for terminating the ingestion loop.
            progress: Progress object for updating Progress.count.
        '''
        if self.hazelcast_cluster and self.hazelcast_cluster.hazelcast_client != None:
            # Construct ER list
            ds = self.hazelcast_cluster.hazelcast_client.get_topic(ds_name).blocking()
            is_dict, er_list = self._update_er_list(ds, create_function, er_object_name, key_type=key_type, key_range=key_range)

            # Loop until interrupted by 'thread_event' or the primary object ingestion count reaches 'count'
            delay = delay_in_msec / 1000
            last_index = count+1
            for i in range(1, last_index):
                obj, key = create_function(key_type=key_type, key_range=key_range)
                if is_dict:
                    ds.publish(HazelcastJsonValue(json.dumps(obj)))
                else:
                    ds.publish(obj)
                for er in er_list:
                    er_keys, er_objects = self._create_er_objects(key, obj, is_dict, er)
                    j = 0
                    for er_key in er_keys:
                        er.ds.publish(er_objects[j])
                        j += 1
                if progress != None:
                    progress.count = i
                if thread_event != None and thread_event.is_set():
                    break
                elif delay_in_msec > 0:
                    time.sleep(delay) 

class FromEr():
    def __init__(self, ds_name, key, object, create_function):
        self.ds_name = ds_name
        self.key = key
        self.object = object
        self.create_function = create_function
        self.ds = None

class ToEr():
    def __init__(self, ds_name, key, object, create_function, attributes, otm, otm_type):
        self.ds_name = ds_name
        self.key = key
        self.object = object
        self.create_function = create_function
        self.attributes = attributes
        self.otm = otm
        self.otm_type = otm_type
        self.ds = None
        self.attribute_list = []
        self.batch = {}
        self.is_dict = False

        for attr in attributes:
            attribute = Attribute(attr['from'], attr['to'])
            self.attribute_list.append(attribute)

class Attribute():
    def __init__(self, from_attr, to_attr):
        self.from_attr = from_attr
        self.to_attr = to_attr