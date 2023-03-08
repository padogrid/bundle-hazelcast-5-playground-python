"""
Created on January 12, 2023

@author: dpark

HazelcastUtil provides Hazelcast specific utility methods.
"""

from hazelcast.core import HazelcastJsonValue

from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_class_name

class HazelcastUtil():

    def get_map_dict(hazelcast_client):
        '''Returns a dictionary containing (map_name, Map) pairs.
    
        Args:
            hazelcast_client: Hazelcast client
        '''
        map_dict = {}
        if hazelcast_client != None:
            for x in hazelcast_client.get_distributed_objects():
                obj_type = str(type(x))
                if obj_type == "<class 'hazelcast.proxy.map.Map'>":
                    if x.name.startswith("__") == False:
                        map_dict[x.name]=x
        return map_dict
    
    def get_queue_dict(hazelcast_client):
        '''Returns a dictionary containing (queue_name, Queue) pairs.
    
        Args:
            hazelcast_client: Hazelcast client
        '''
        queue_dict = {}
        if hazelcast_client != None:
            for x in hazelcast_client.get_distributed_objects():
                obj_type = str(type(x))
                if obj_type == "<class 'hazelcast.proxy.queue.Queue'>":
                    if x.name.startswith("__") == False:
                        queue_dict[x.name]=x
        return queue_dict
    
    def get_key_list(map, sort=True):
        '''Returns a list containing keys extracted from the specified Map.
    
        Args:
            map: Hazelcast Map
            sort: True to sort the list. Default: True
        '''
        if map == None:
            list = []
        else:
            future=map.key_set()
            result = HazelcastUtil.get_future_value(future)
            list=[]
            for x in result:
                list.append(x)
            if sort == True:
                list.sort()
        return list

    def object_to_json(obj):
        '''Returns a JSON value if the specified object is HazelcastJsonValue, otherwise,
        returns a dictionary containing all public object attributes.'''
        if type(obj) == HazelcastJsonValue:
            json_value = obj.loads()
        else:
            json_value = '{}'
            if obj != None:
                json_value = get_attributes(obj)
        return json_value
    
    def get_future_value(future):
        obj_type=str(type(future))
        if obj_type == "<class 'hazelcast.future.Future'>" or obj_type == "<class 'hazelcast.future.ImmediateFuture'>":
            result=future.result()
        else:
            result=future
        return result

    def get_object(ds):
        '''Returns a single object from the specified data structure. It returns None if
        the data structure is None, empty, or unsupported.

        Note: This function is expensive as it makes a remote call to retrieve objects to
        determine the object type. Data structures such as ReplicatedMap do not support "limit"
        and thus the entire key set must be retrieved in order to get a single value.
        '''
        if ds == None:
            return None
        obj_type = str(type(ds)) 
        obj = None    
        if obj_type == "<class 'hazelcast.proxy.list.List'>":
            size = HazelcastUtil.get_future_value(ds.size())
            if size > 0:
                obj = HazelcastUtil.get_future_value(ds.get(0))
        elif obj_type == "<class 'hazelcast.proxy.map.Map'>" or obj_type == "<class 'hazelcast.proxy.replicated_map.ReplicatedMap'>":
            #if hazelcast_cluster != None and hazelcast_cluster.hazelcast_client != None:
            #    query = f'select * from "{ds.name}" limit 1'
            #    result = hazelcast_cluster.hazelcast_client.sql.execute(query).result()
            #    for row in result:
            #        columns = row.metadata.columns
            keys = HazelcastUtil.get_future_value(ds.key_set())
            for key in keys:
                obj = HazelcastUtil.get_future_value(ds.get(key))
                break
        elif obj_type == "<class 'hazelcast.proxy.multi_map.MultiMap'>":
            keys = HazelcastUtil.get_future_value(ds.key_set())
            for key in keys:
                obj_list = HazelcastUtil.get_future_value(ds.get(key))
                if len(obj_list) > 0:
                    obj = obj_list[0]
                break
        elif obj_type == "<class 'hazelcast.proxy.queue.Queue'>":
            obj = HazelcastUtil.get_future_value(ds.peek())
        elif obj_type == "<class 'hazelcast.proxy.ringbuffer.Ringbuffer'>":
            tail = HazelcastUtil.get_future_value(ds.tail_sequence())
            if tail >= 0:
                obj = HazelcastUtil.get_future_value(ds.read_one(tail))
        elif obj_type == "<class 'hazelcast.proxy.set.Set'>":
            obj_list = HazelcastUtil.get_future_value(ds.get_all())
            if len(obj_list) > 0:
                obj = obj_list[0]
        return obj

    def get_object_type(ds):
        '''Returns the object type (fully-qualified class name) of the objects in
        the specified data structure. It return None if the data structure is None, empty,
        or unsupported.

        Note: This function is expensive as it makes a remote call to retrieve objects to
        determine the object type. Data structures such as ReplicatedMap do not support "limit"
        and thus the entire key set must be retrieved in order to get a single value.
        '''
        obj = HazelcastUtil.get_object(ds)
        if obj == None:
            object_type = None
        else:
            object_type =get_class_name(obj)
        return object_type
        
