'''
Created on Oct 15, 2017

@author: dpark
'''
import inspect
from importlib import import_module

def get_class_name_from_class(clazz):
    '''Returns the fully-qualified class name of the specified class.
    
    Args:
        clazz: Class
    '''
    return clazz.__module__ + "." + clazz.__name__

def get_class_name(obj):
    '''Returns the fully-qualified class name of the specified object.
    
    Args:
        obj: Any object.
    '''
    c = obj.__class__.__mro__[0]
    return c.__module__ + "." + c.__name__

def get_short_class_name(obj):
    '''Returns the short class name.'''
    c = obj.__class__.__mro__[0]
    return c.__name__

def get_class_introspect(method):
    '''Returns the class of the specified method.
    
    Args:
        method: Class method. 
    
    Returns: None if invalid method.
    '''
    if inspect.ismethod(method):
        for cls in inspect.getmro(method.__self__.__class__):
            if cls.__dict__.get(method.__name__) is method:
                return cls
        method = method.__func__  # fallback to __qualname__ parsing
    if inspect.isfunction(method):
        cls = getattr(inspect.getmodule(method),
                      method.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0])
        if isinstance(cls, type):
            return cls
    return None

def get_class_name_introspect(method):
    '''
    Returns the fully-qualified name (including module name) of the specified method.
    
    Args:
        method: Class method. 
    
    Returns: None if invalid method.
    '''
    c = get_class_introspect(method)
    if c == None:
        return None
    else:
        return c.__module__ + "." + c.__name__
    
def get_class_method_names(class_method_name):
    '''
    Returns the class and method name extracted from the specified fully-qualified method name.
    
    Args:
        class_method_name: Fully-qualified method name that includes the module name
            the method name in dot notations.
            
    Returns:
        class_name, method_name.
    '''
    index = class_method_name.rindex('.')
    classname = class_method_name[0:index]
    method_name = class_method_name[index+1:]
    return classname, method_name

def get_attributes(clazz):
    '''
    Returns a dictionary of (attribute, value) pairs of all public attributes 
    of the specified class or object. It returns None if the specified clazz is None.
    '''
    if clazz != None:
        return {name: attr for name, attr in clazz.__dict__.items()
                if not name.startswith("__") 
                and not callable(attr)
                and not type(attr) is staticmethod}
    return None

def get_class_or_function(class_name):
    '''
    Returns class or function.

    Args:
        class_name: Fully-qualified class or function name

    Exception:
        ImportError: Thrown if the specified class name is not valid.
    '''
    try:
        module_path, class_name = class_name.rsplit('.', 1)
        module = import_module(module_path)
        clazz = getattr(module, class_name)
        return clazz
    except (ImportError, AttributeError) as e:
        raise e

def get_instance(class_name):
    '''
    Returns an instance of the specified class name

    Args:
        class_name: Fully-qualified class name
    '''
    clazz = get_class_or_function(class_name)
    return clazz()

def get_classes_in_module(module_name):
    '''Returns a list of immediate classes found in the specified module. The returned list
    includes only the classes defined in the module and excludes imported classes.'''
    class_list = []
    module = import_module(module_name)
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            if obj.__module__ == module_name:
                class_list.append(obj)
    return class_list