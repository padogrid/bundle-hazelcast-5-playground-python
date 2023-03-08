# %%
"""
DacList displays Hazelcast List contents.
"""

import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_class_name
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil
from padogrid.hazelcast.playground.DacBase import DacBase

pn.extension('jsoneditor')

class DacList(DacBase, Viewer):
    
    ds_type = 'List'

    _on_click_callback = None

    width = param.Integer(1000)
    page_size = param.Integer(20)
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        self._status_text = pn.widgets.TextInput(disabled=True, value='')
        ds_name_list = []
        self._list_select = pn.widgets.Select(name='Lists', sizing_mode='fixed', options=ds_name_list)
        self._list_select.param.watch(self.__list_value_changed__, 'value')
        self._ds_size_text = pn.widgets.TextInput(name='Size', disabled=True, width=self.size_text_width)
        self._index_input = pn.widgets.IntInput(name='Index', value=0, step=1, start=0, end=1000, width=100)
        self._get_button = pn.widgets.Button(name='get()', button_type='primary', width=self.button_width)    
        self._get_button.on_click(self.__get_execute__)
        self._remove_button = pn.widgets.Button(name='remove_at()', button_type='primary', width=self.button_width)
        self._remove_button.on_click(self.__remove_execute__)
        self._clear_button = pn.widgets.Button(name='clear()', button_type='primary', width=self.button_width)
        self._clear_button.on_click(self.__clear_button_on_click__)

        df = pd.DataFrame({}) 
        self._blotter = pn.widgets.Tabulator(df, width=self.width, disabled=True, pagination='local', 
                                    page_size=self.page_size, sizing_mode='stretch_width')                                    
        self._json_editor = pn.widgets.JSONEditor(name='Object', mode='tree', value={}, height=800)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 pn.Row(self._list_select, self._ds_size_text), pn.Row(self._index_input), 
                                 pn.Row(self._get_button, self._remove_button, self._clear_button),
                                 self._blotter, self._json_editor)
        self._sync_widgets()

    def refresh(self, is_reset=False):
        ds_name = self._list_select.value
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        old_name_list = self._list_select.options
        if ds_name_list == old_name_list:
            self.__refresh_size__()
            return

        self._list_select.options = ds_name_list
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        self.select(ds_name)
        self._sync_widgets()

    def clear(self):
        data = {}
        self._blotter.page = 1
        df = pd.DataFrame(data)
        self._blotter.value = df
        self._json_editor.value = {}
        self._index_input.start = -1
        self._index_input.end = -1
        # It takes some time to clear the map. Assume the queue properly clears.
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh()
        # self.__refresh_size__()
        
    def __panel__(self):
        return self._layout
    
    def __list_value_changed__(self, event):
        self.select(event.obj.value)

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._list_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._list_select.value

    def __get_execute__(self, event):
        obj = None
        try:
            obj = self.get()
            self.__update_component__(obj)
            self.__refresh_size__()   
        except Exception as ex:
            self.__update_component__(obj)
            self.__refresh_size__()
            self._status_text.value = repr(ex)
        if self._on_click_callback != None:
            self._on_click_callback(obj, exception=None)

    def __remove_execute__(self, event):
        obj = None
        try:
            index = self._index_input.value
            is_removed = self.remove_at(index)
            if is_removed:
                self.__update_component__()
                self.__refresh_size__()
                self._status_text.value = 'Removed index: ' + str(index)
        except Exception as ex:
            self.__update_component__(obj)
            self.__refresh_size__()
            self._status_text.value = repr(ex)       
        if self._on_click_callback != None:
            self._on_click_callback(obj, exception=ex)

    def __clear_button_on_click__(self, event):
        ds_name = self._list_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            ds.clear()
        self.clear()

    def __refresh_size__(self):
        try:
            ds_name = self._list_select.value
            if ds_name == None:
                self._ds_size_text.value = ''
            else:
                ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
                if ds_size != None:
                    self._ds_size_text.value = str(ds_size)
                else:
                    self._ds_size_text.value = 'N/A'
                self._index_input.end = ds_size-1
        except Exception as ex:
            self._status_text.value = repr(ex)

    def __update_component__(self, obj = None):
        self._status_text.value = ''
        data = {}
        is_exception = False
        if obj != None:
            try:
                obj = HazelcastUtil.get_future_value(obj)
                class_name = get_class_name(obj)
                is_builtin_type = class_name.startswith('builtin')
                is_json = class_name == "hazelcast.core.HazelcastJsonValue"
                try:
                    if is_builtin_type:
                        data['value'] = [obj]
                    else:
                        if is_json:
                            attr_dict = HazelcastUtil.object_to_json(obj)
                        else:
                            attr_dict = get_attributes(obj)
                        for item in attr_dict.items():
                            column = item[0]
                            value = item[1]
                            data[column] = [value]
                except Exception as ex1:
                    # Exception raised if the entries are not portable
                    self._status_text.value = repr(ex1)
                    is_exception = True
            except Exception as ex2:
                self._status_text.value = repr(ex2)
                is_exception = True
        
        df = pd.DataFrame(data)
        self._blotter.value = df

        if is_exception or obj == None:
            self._json_editor.value = {}
        else:
            json = HazelcastUtil.object_to_json(obj)
            self._json_editor.value = json
        self.__refresh_size__()

    def select(self, ds_name):
        self._list_select.value = ds_name
        if self.hazelcast_cluster != None:
            ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
        else:
            ds_size = None

        index = self._index_input.value
        if ds_size == None:
            index = -1
        elif index > ds_size-1:
            index = ds_size - 1
        elif index < 0:
            index = 0
        self._index_input.value = index
        
        obj = None
        try:
            obj = self.get(index)
        except:
            pass
        self.__update_component__(obj)
    
    def get(self, index=-1):
        if index < 0:
            index = self._index_input.value
        # index maybe -1 if the list is empty
        if index < 0:
            return None
        ds_name = self._list_select.value
        if self.hazelcast_cluster != None:
           ds  = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        result = None
        if ds != None:
            future = ds.get(index)
            result = HazelcastUtil.get_future_value(future)
        return result
    
    def remove_at(self, index=-1):
        if index < 0:
            index = self._index_input.value
        # index maybe -1 if the list is empty
        if index < 0:
            return False
        ds_name = self._list_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        result = None
        if ds != None:
            future = ds.remove_at(index)
            result = HazelcastUtil.get_future_value(future)
            if result == True:
                if self.hazelcast_cluster != None:
                    # Set value in case it gets reset by the refresh() call
                    value = self._list_select.value
                    self.hazelcast_cluster.refresh()
                    self._list_select.value = value
        return result
    
    def on_click(self, callback=None):
        self._on_click_callback = callback
