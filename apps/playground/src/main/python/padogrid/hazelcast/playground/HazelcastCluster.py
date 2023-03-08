"""
Created on January 14, 2023

@author: dpark

HazelcastCluster provides convenience methods for extracting Hazelcast metadata
and data structure information.
"""

from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil
from padogrid.hazelcast.playground.DacBase import DacBase

class HazelcastCluster():

    hazelcast_client = None
    cluster_url = None
    component_dict = {}

    def __init__(self, hazelcast_client=None, cluster_url=None):
        self.reset(hazelcast_client=hazelcast_client, cluster_url=cluster_url)

    def reset(self, hazelcast_client=hazelcast_client, cluster_url=None):
        '''Sets Hazelcast Client and refreshes all components.
        Args:
            hazelcast_client: Hazelcast Client instance. If None, then clears all components.
            cluster_url: Hazelcast cluster endpoint URL in the form of 'cluster_name@host:port'
        '''
        self.hazelcast_client = hazelcast_client
        self.cluster_url = cluster_url
        self.refresh()
        
    def _clear_dict(self):
        self.flake_id_generator_dict = {}
        self.list_dict = {}
        self.map_dict = {}
        self.multi_map_dict = {}
        self.queue_dict = {}
        self.pn_counter_dict = {}
        self.reliable_topic_dict = {}
        self.replicated_map_dict = {}
        self.ringbuffer_dict = {}
        self.set_dict = {}
        self.topic_dict = {}
        self.transactional_list_dict = {}
        self.transactional_map_dict = {}
        self.transactional_multi_map_dict = {}
        self.transactional_queue_dict = {}
        self.transactional_set_dict = {}

    def _clear_cache(self):
        '''Clears the cache that keeps the last scanned data structure info.
        '''
        self._clear_dict()

    def get_ds_size_from_hz(self, ds):
        '''Returns the size of the specified data structure from the cluster. It returns
        None if the specified ds is invalid.
        
        Args:
            ds: Data structure. If None or size() not supported, returns None.
        '''
        ds_size = None
        if ds != None:
            try:
                ds_size = HazelcastUtil.get_future_value(ds.size())
            except:
                pass
        return ds_size

    def refresh(self, is_reset=False):
        '''Refreshes this object with the latest data from Hazelcast.

        Args:
            is_reset: True to reset components, False to refresh. Each component decides
                      what to do based on this flag. Reset typically performs refresh and more.
        '''
        self._clear_cache()
        if self.hazelcast_client != None:
            try:
                for ds in self.hazelcast_client.get_distributed_objects():
                    if ds.name.startswith("_") == False:
                        obj_type = str(type(ds))
                        ds_dict = None
                        if obj_type == "<class 'hazelcast.proxy.flake_id_generator.FlakeIdGenerator'>":
                            ds_dict = self.flake_id_generator_dict
                        elif obj_type == "<class 'hazelcast.proxy.list.List'>":
                            ds_dict = self.list_dict
                        elif obj_type == "<class 'hazelcast.proxy.map.Map'>":
                            ds_dict = self.map_dict
                        elif obj_type == "<class 'hazelcast.proxy.multi_map.MultiMap'>":
                            ds_dict = self.multi_map_dict
                        elif obj_type == "<class 'hazelcast.proxy.pn_counter.PNCounter'>":
                            ds_dict = self.pn_counter_dict
                        elif obj_type == "<class 'hazelcast.proxy.queue.Queue'>":
                            ds_dict = self.queue_dict
                        elif obj_type == "<class 'hazelcast.proxy.reliable_topic.ReliableTopic'>":
                            ds_dict = self.reliable_topic_dict
                        elif obj_type == "<class 'hazelcast.proxy.replicated_map.ReplicatedMap'>":
                            ds_dict = self.replicated_map_dict
                        elif obj_type == "<class 'hazelcast.proxy.ringbuffer.Ringbuffer'>":
                            ds_dict = self.ringbuffer_dict
                        elif obj_type == "<class 'hazelcast.proxy.set.Set'>":
                            ds_dict = self.set_dict
                        elif obj_type == "<class 'hazelcast.proxy.topic.Topic'>":
                            ds_dict = self.topic_dict
                        elif obj_type == "<class 'hazelcast.proxy.transactional_list.TransactionalList'>":
                            ds_dict = self.transactional_list_dict
                        elif obj_type == "<class 'hazelcast.proxy.transactional_map.TransactionalMap'>":
                            ds_dict = self.transactional_map_dict
                        elif obj_type == "<class 'hazelcast.proxy.transactional_multi_map.Map'>":
                            ds_dict = self.transactional_multi_map_dict
                        elif obj_type == "<class 'hazelcast.proxy.transactional_queue.TransactionalQueue'>":
                            ds_dict = self.transactional_queue_dict
                        elif obj_type == "<class 'hazelcast.proxy.transactional_set.TransactionalSet'>":
                            ds_dict = self.transactional_set_dict
                        if ds_dict != None:
                            ds_dict[ds.name] = DsItem(ds, self.get_ds_size_from_hz(ds))
            except Exception as ex:
                # Exception may occur if Hazelcast connection is lost
                self._clear_cache()

        # Refresh all registered components
        comp_dict = self.component_dict.copy()
        for component in comp_dict.values():
            component.reset(self, is_reset=is_reset)

    def get_ds_dict(self, ds_type):
        '''Returns the data structure dictionary for this component. The returned dictionary contains
        DsItem objects providing data structure instances and their sizes. It returns an empty dictionary
        if the specified data structure type is not supported.

        Args:
            ds_type: Data structure type.
        '''
        ds_dict = {}
        if ds_type == 'FlakeIdGenerator':
            ds_dict = self.flake_id_generator_dict
        elif ds_type == 'List':
            ds_dict = self.list_dict
        elif ds_type == 'Map':
            ds_dict = self.map_dict
        elif ds_type == 'MultiMap':
            ds_dict = self.multi_map_dict
        elif ds_type == 'PNCounter':
            ds_dict = self.pn_counter_dict
        elif ds_type == 'Queue':
            ds_dict = self.queue_dict
        elif ds_type == 'ReliableTopic':
            ds_dict = self.reliable_topic_dict
        elif ds_type == 'ReplicatedMap':
            ds_dict = self.replicated_map_dict
        elif ds_type == 'Ringbuffer':
            ds_dict = self.ringbuffer_dict
        elif ds_type == 'Set':
            ds_dict = self.set_dict
        elif ds_type == 'Topic':
            ds_dict = self.topic_dict
        elif ds_type == 'TransactionalList':
            ds_dict = self.transactional_list_dict
        elif ds_type == 'TransactionalMap':
            ds_dict = self.transactional_map_dict
        elif ds_type == 'TransactionalMultiMap':
            ds_dict = self.transactional_multi_map_dict
        elif ds_type == 'TransactionalQueue':
            ds_dict = self.transactional_queue_dict
        elif ds_type == 'TransactionalSet':
            ds_dict = self.transactional_set_dict
        return ds_dict

    def get_ds(self, ds_type, ds_name):
        '''Returns the specified data structure instance from the dictionary. It returns None
        if the specified ds_type is not supported. It never creates a new instance if it does
        not exist in the dictionary.
        
        Args:
            ds_type: Data structure type.
            ds_name: Data structure name.
        '''
        ds = None
        ds_dict = self.get_ds_dict(ds_type)
        if ds_name in ds_dict:
            ds = ds_dict[ds_name].ds
        return ds

    def get_ds_size(self, ds_type, ds_name):
        '''Returns the cached size of the specified data structure. It returns None if
        ds_name is not in the cache.

        Args:
            ds_name: Data structure name.
        '''
        ds_size = None
        ds_dict = self.get_ds_dict(ds_type)
        if ds_name in ds_dict:
            ds_size = ds_dict[ds_name].ds_size
        return ds_size

    def get_ds_names(self, ds_type):
        '''Returns a list of all data structure instance names for the specified ds_type.
        It returns an empty list if invalid ds_type.
        '''
        ds_dict = self.get_ds_dict(ds_type)
        return list(ds_dict)

    def get_ds_from_hz(self, ds_type, ds_name):
        '''Returns the specified data structure instance by invoking HazelcastClient. If the
        data structure does not exists, a new instance is created. It returns None if the specified
        ds_type is not supported.
        
        Args:
            ds_type: Data structure type.
            ds_name: Data structure name.
        '''
        ds = None
        if ds_type == 'FlakeIdGenerator':
            ds = self.hazelcast_client.flake_id_generator(ds_name)
        elif ds_type == 'List':
            ds = self.hazelcast_client.get_list(ds_name)
        elif ds_type == 'Map':
            ds = self.hazelcast_client.get_map(ds_name)
        elif ds_type == 'MultiMap':
            ds = self.hazelcast_client.get_multi_map(ds_name)
        elif ds_type == 'PNCounter':
            ds = self.hazelcast_client.get_pn_counter(ds_name)
        elif ds_type == 'ReliableTopic':
            ds = self.hazelcast_client.get_reliable_topic(ds_name)
        elif ds_type == 'ReplicatedMap':
            ds = self.hazelcast_client.get_replicated_map(ds_name)
        elif ds_type == 'Ringbuffer':
            ds = self.hazelcast_client.get_ringbuffer(ds_name)
        elif ds_type == 'Set':
            ds = self.hazelcast_client.get_set(ds_name)
        elif ds_type == 'Queue':
            ds = self.hazelcast_client.get_queue(ds_name)
        elif ds_type == 'Topic':
            ds = self.hazelcast_client.get_topic(ds_name) 
        return ds

    def get_map_key_list(self, map_name, sort=True):
        '''Returns a list containing keys extracted from the specified map. It returns
        an empty list if the specified map name is invalid or not found.
    
        Args:
            map_name: Hazelcast Map name
            sort: True to sort the list. Default: True
        '''
        list=[]
        if map_name != None:
            try:
                map_dict = self.get_ds('map')
                if map_dict != None and map_name in map_dict:
                    map = map_dict[map_name]
                    future=map.key_set()
                    result = HazelcastUtil.get_future_value(future)
                    for x in result:
                        list.append(x)
                    if sort == True:
                        list.sort()
            except:
                pass
        return list
    
    def shutdown(self):
        if self.hazelcast_client != None:
            self.hazelcast_client.shutdown()
        self.refresh()
    
    def add_component(self, component):
        '''Adds the specified component.
        
        Args:
            component: Component to add. If None or not DacBase type, then this call has no effect.
        '''
        if component == None or isinstance(component, DacBase) == False:
            return
        self.component_dict[component.dac_id] = component

    def remove_component(self, component):
        '''Removes the specified component.

        Args:
            component: Component to add. If None or not DacBase type, then this call has no effect.
        '''
        if component == None or isinstance(component, DacBase) == False:
            return
        try:
            del self.component_dict[component.dac_id]
        except:
            pass
    
    def clear_components(self):
        self.component_dict.clear()

class DsItem():

    def __init__(self, ds, ds_size):
        self.ds = ds
        self.ds_size = ds_size