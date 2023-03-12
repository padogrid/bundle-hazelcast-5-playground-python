import os
import shutil
import yaml
from importlib import import_module
from padogrid.hazelcast.playground.class_util import get_instance
from padogrid.hazelcast.playground.class_util import get_class_or_function
from padogrid.hazelcast.playground.class_util import get_classes_in_module
from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_short_class_name

def get_playground_config(file_path=None):
    '''Returns the playground configuration in a dictionary.

    Args: 
        file_path: Yaml file containing playground configuration. If not specified, then
                    defaults to 'padogrid.hazelcast.playground.playground.yaml'
    '''
    if file_path == None:
        playground_home = os.environ.get('PLAYGROUND_HOME')
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        template_file_path=f'{dir_path}/playground.yaml'
        if playground_home == None:
            file_path = template_file_path
        else:
            etc_dir = playground_home + "/etc"
            file_path =  etc_dir + "/playground.yaml"
            if os.path.isfile(file_path) == False:
                if os.path.exists(etc_dir) == False:
                    os.mkdir(etc_dir)
                shutil.copy(template_file_path, file_path)                
    with open(file_path) as file:
        config = yaml.safe_load(file)
    file.close()
    return config

def get_create_functions(playground_config):
    '''Returns a dictionary of <object_name, create_function> entries defined by the specified config.

    Args:
        playground_config: Playground configuration read from 'playground.yaml'.
    '''
    function_dict = {}
    if 'objects' in playground_config:
        mock = playground_config['objects']
        for key, value in mock.items():
            splits = value.rsplit('.',  maxsplit=1)
            class_name = splits[0]
            try:
                obj = get_instance(class_name)
                function_name = splits[1]
                function = getattr(obj, function_name)
            except:
                function = get_class_or_function(value)
            function_dict[key] = function
    return function_dict

#portable_factories={Customer.FACTORY_ID: {Customer.CLASS_ID: Customer, Order.CLASS_ID: Order}})
def get_portable_factories(playground_config):
    '''Returns a portable factory dictionary for initializing HazelcastClient.
    Args:
        playground_config: Playground configuration read from 'playground.yaml'.
    '''
    pf_dict = {}
    if 'serialization' in playground_config:
        serialization = playground_config['serialization']
        if 'portable-factories' in serialization:
            portable_factories = serialization['portable-factories']
            for factory in portable_factories:
                if 'factory-id' in factory:
                    factory_id = factory['factory-id']
                else:
                    factory_id = None
                if 'module-name' in factory:
                    module_name = factory['module-name']
                else:
                    module_name = None
                try:
                    if module_name != None and factory_id == None:
                        module = import_module(module_name)
                        factory_id = getattr(module, 'FACTORY_ID')
                except:
                    # Silent ignore non-conforming factory classes
                    pass
                class_dict = {}
                if factory_id != None:
                    class_list = get_classes_in_module(module_name)
                    for clazz in class_list:
                        try:
                            class_id = getattr(clazz, 'CLASS_ID')
                            class_dict[class_id] = clazz
                        except:
                            # Silent ignore non-conforming factory classes
                            pass
                if factory_id != None and len(class_dict) > 0:
                    pf_dict[factory_id] = class_dict
    return pf_dict

def get_er(playground_config, function_dict):
    object_dict = {}
    if 'er' in playground_config:
        er = playground_config['er']
        for object, value in er.items():
            er_list = []
            for ds_name, ds_value in value.items():
                if 'key' in ds_value:
                    key = ds_value['key']
                    if key != 'random' and key != 'uuid':
                        key = 'random'
                else:
                    key = 'random'
                if 'object' in ds_value:
                    to_object = ds_value['object']
                    if to_object in function_dict == False:
                        raise AttributeError(f'Invalid configuration. Undefined object [{to_object}]: er.{object}.{ds_name}.object.')
                    to_function = function_dict[to_object]
                else:
                    raise AttributeError(f'Invalid configuration. Undefined element: er.{object}.{ds_name}.object')
                attr_list = []
                if 'attributes' in ds_value:
                    attributes = ds_value['attributes'] 
                    for attr in attributes:
                        if 'attribute' in attr:
                            attribute = attr['attribute']
                            attr_map = {}
                            for key, value in attribute.items():
                                if key == 'from' or key == 'to':
                                    attr_map[key] = value
                                else:
                                    raise AttributeError(f'Invalid configuration. Undefined element: er.{object}.{ds_name}.attributes.attribute.{key}.')
                            attr_list.append(attr_map)
                else:
                    attributes = []
                if 'otm' in ds_value:
                    otm = ds_value['otm'] 
                else:
                    otm = 1
                if 'otm-type' in ds_value:
                    otm_type = ds_value['otm-type'] 
                    if otm_type != 'random' or otm_type != 'exact':
                        otm_type = 'exact'
                else:
                    otm_type = 'exact'

                ds_dict = {}
                ds_dict['ds_name'] = ds_name
                ds_dict['key'] = key
                ds_dict['object'] = to_object
                ds_dict['function'] = to_function
                ds_dict['attributes'] = attr_list
                ds_dict['otm'] = otm
                ds_dict['otm-type'] = otm_type
                er_list.append(ds_dict)
            object_dict[object] = er_list
    return object_dict

def get_components(playground_config, hazelcast_cluster):
    component_list = []
    if 'components' in playground_config:
        components_list_config = playground_config['components']
        for component_config in components_list_config:
            try:
                if 'component' in component_config:
                    comp = component_config['component']
                    component = {} 
                    if 'class' in comp:
                        class_name = comp['class']
                        obj = get_instance(class_name)
                        attr_dict =get_attributes(obj)
                        # workaround: isinstance() throws exception due to a cyclic reference of DacBase
                        if 'hazelcast_cluster' in attr_dict:
                            obj.hazelcast_cluster = hazelcast_cluster
                        component['component'] = obj

                        if 'name' in comp:
                            component['name'] = comp['name']
                        else:
                            component['name'] = get_short_class_name(obj)
                        if 'title' in comp:
                            component['title'] = comp['title']
                        else:
                            component['title'] = component['name']
                        if 'tab' in comp:
                            component['tab'] = comp['tab']
                        else:
                            component['tab'] = 'root'
                    component_list.append(component)
            except Exception as ex:
                # Catch invalid class exception. Ignore for now.
                print(ex)
                pass
    return component_list
