# %%
"""
DacMultiMap provides a blotter for viewing MultiMap contents.
"""

import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_class_name
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil
from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.HazelcastCluster import HazelcastCluster

class DacMultiMap(DacBase, Viewer):
    
    ds_type = 'MultiMap'

    width = param.Integer(1000)
    page_size = param.Integer(20)
    value = None
    title_text = param.String("MultiMap")
    
    def __init__(self, **params):
        super().__init__(**params)
        
        self._status_text = pn.widgets.input.TextInput(disabled=True)
        map_list = []
        self._map_select = pn.widgets.Select(name='MultiMap', sizing_mode='fixed', options=map_list)
        self._map_select.param.watch(self.__map_selected__, 'value')
        self._ds_size_text = pn.widgets.TextInput(name='Size', disabled=True, width=self.size_text_width)    
        self._clear_button = pn.widgets.Button(name='clear()', button_type='primary', width=self.button_width)
        self._clear_button.on_click(self.__clear_button_on_click__)

        df = pd.DataFrame({})        
        self._map_blotter = pn.widgets.Tabulator(df, groupby = ['__key'], width=self.width, disabled=True, pagination='local', 
                        page_size=self.page_size, sizing_mode='stretch_width', buttons={'Remove': "<i class='fa fa-trash'></i>"})
        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 pn.Row(self._map_select, self._ds_size_text), 
                                 pn.Row(self._clear_button),
                                 self._map_blotter)
        self._sync_widgets()

    def clear(self):
        # It takes some time to clear the map. Assume the map properly clears.
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh()
            data = {}
            self._map_blotter.page = 1
            df = pd.DataFrame(data)
            self._map_blotter.value = df

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        # Return if reset is False, i.e., skip execute map
        if is_reset == False:
            old_name_list = self._map_select.options
            if ds_name_list == old_name_list:
                self.__refresh_size__()
                return

        # If reset, then execute map
        self._map_select.options = ds_name_list
        map = None
        ds_name = self._map_select.value
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        if self.hazelcast_cluster != None:
            map = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        self.execute_map(map)
    
    def __panel__(self):
        return self._layout

    def __map_selected__(self, event):
        ds_name = self._map_select.value
        map = None
        if self.hazelcast_cluster != None:
            map = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        self.execute_map(map)
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._map_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._map_select.value

    def __clear_button_on_click__(self, event):
        ds_name = self._map_select.value
        if ds_name != None and ds_name != '':
            if self.hazelcast_cluster != None:
                map = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
                if map != None:
                    map.clear()
                self.clear()

    def __refresh_size__(self):
        ds_name = self._map_select.value
        if ds_name == None:
            self._ds_size_text.value = ''
        else:
            ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
            self._ds_size_text.value = str(ds_size)

    def execute_map(self, map):
        data = {}
        # df = pd.DataFrame(data)   
        # self._map_blotter.value = df
        self._map_blotter.page = 1
        is_exception = False
        if map == None:
            ds_name = None
        else:
            ds_name = map.name
            future = map.entry_set()
            entry_set=HazelcastUtil.get_future_value(future)
            status = ''
            if len (entry_set) > 0:
                try:
                    for entry in entry_set:
                        key = entry[0]
                        value = entry[1]
                        break;
                    class_name = get_class_name(value)
                    is_builtin_type = class_name.startswith('builtin')
                    is_json = class_name == "hazelcast.core.HazelcastJsonValue"

                    data = {}
                    data['__key'] = []
                    if is_builtin_type:
                        data['value'] = []
                    else:
                        if is_json:
                            attr_dict = HazelcastUtil.object_to_json(value)
                        else:
                            attr_dict = get_attributes(value)
                        columns = attr_dict.keys()
                        for column in columns:
                            data[column] = []
                    
                    for entry in entry_set:
                        key = entry[0]
                        value = entry[1]
                        data['__key'].append(key)
                        if is_builtin_type:
                            data['value'].append(value) 
                        else:
                            if is_json:
                                attr_dict = HazelcastUtil.object_to_json(value)
                            else:
                                attr_dict = get_attributes(value)
                            for item in attr_dict.items():
                                column = item[0]
                                value = item[1]
                                data[column].append(value)
                except Exception as ex:
                    # Exception raised if the entries are not portable
                    status = repr(ex)
                    try:
                        data['__key'] = HazelcastUtil.get_key_list(map)
                    except Exception as ex:
                        status = status + " = " + repr(ex)
                        is_exception = True
                    is_exception = True
            
            df = pd.DataFrame(data)   
            # A bug in Panel. title gets cleared. Use title_text instead.
            if is_exception == True:
                if self._map_select.value != ds_name:
                    self._map_select.value = ds_name
            else:
                if ds_name == None:
                    self._map_select.value = "(Map undefined)"
                else:
                    self._map_select.value = ds_name
            self._map_blotter.value = df
            self._status_text.value = status
            self._ds_size_text.value = str(HazelcastUtil.get_future_value(map.size()))
