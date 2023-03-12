# %%
"""
DacBase is the base class for all Dac components.
"""

from padogrid.hazelcast.playground.init_util import get_create_functions
from padogrid.hazelcast.playground.init_util import get_er
from padogrid.hazelcast.playground.class_util import get_class_name
from padogrid.hazelcast.playground.ThreadPool import ThreadPool
from padogrid.hazelcast.playground.DataIngestor import DataIngestor
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil

class DacBase():

    button_width = 100
    size_text_width = 100
    thread_pool = ThreadPool(size=10)

    config = None
    obj_creation_function_dict = None
    er_dict = None
    obj_type_list = None
    er_name_list = None
    component_list = None

    #obj_creation_function_dict = get_create_functions(playground_config)
    #er_dict = get_er(playground_config, obj_creation_function_dict)
    #obj_type_list = list(obj_creation_function_dict.keys())
    #er_name_list = list(er_dict.keys()) + ['N/A']

    ingestor = DataIngestor(er_dict)

    def __init__(self, **params):
        if "dac_id" in params:
            self.dac_id = params["dac_id"]
        else:
            self.dac_id = get_class_name(self)
        if "hazelcast_cluster" in params:
            self.hazelcast_cluster = params["hazelcast_cluster"]
        else:
            self.hazelcast_cluster = None

    def _refresh_execute(self, event):
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh()

    def get_object_name(self, obj):
        '''Returns the object name of the specified object. The returned object name
        maps 'obj_creation_function_dict' which defines object creation functions.

        Args:
            obj: Serializable object by Hazelcast.
        '''
        obj_name = get_class_name(obj).split('.')[-1]
        if obj_name == 'HazelcastJsonValue':
            dict = obj.loads() 
            if 'orderId' in dict:
                obj_name = 'OrderJson'
            else:
                obj_name = 'CustomerJson'
        return obj_name

    def get_obj_creation_function(self, obj_type):
        self.ingestor.hazelcast_cluster = self.hazelcast_cluster
        if obj_type != None and obj_type in self.obj_creation_function_dict:
            return self.obj_creation_function_dict[obj_type]
        else:
            return None
    
    def get_object_name_list(self):
        '''Returns a list of allowed object names.'''
        return self.obj_type_list

    def get_er_name_list(self):
        '''Returns a list of allowed ER names.'''
        return self.er_name_list
        
    def get_object_name_in_ds(self, ds):
        '''Returns the name of the objects in the specified data structure.'''
        obj = HazelcastUtil.get_object(ds)                    
        if obj == None:
            object_type_in_ds = None
        else:
            object_type_in_ds = self.get_object_name(obj)
        return object_type_in_ds

    def reset(self, hazelcast_cluster, is_reset=False):
        '''Resets and refreshes this component with the specified HazelcastCluster.
        Args:
            hazelcast_cluster: HazelcastCluster instance. If None, then clears this component.
        '''

        # Add/update this component into HazelcastCluster
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.add_component(self)
        self.refresh(is_reset=is_reset)

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast. Each component
        can implement this method to refresh accordingly.

        Args:
            is_reset: True to reset components, False to refresh. The subclass component decides
                      what to do based on this flag. Reset typically performs refresh and more.
        '''
        return
    
