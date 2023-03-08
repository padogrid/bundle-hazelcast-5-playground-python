# %%
"""
QueryTable provides an SQL Query components for executing ad-hoc queries.
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

pn.extension('jsoneditor')

class DacMapKeySearch(DacBase, Viewer):

    value = None
    width = param.Integer(1000)
    page_size = param.Integer(20)

    def __init__(self, **params):
        super().__init__(**params)
        key_list = []
        map_list = []

        if "map_type" in params:
            self.map_type = params["map_type"]
        else:
            self.map_type = 'Map'

        self._status_text = pn.widgets.input.TextInput(disabled=True)
        self._map_select = pn.widgets.Select(sizing_mode='fixed', name='Map', options=map_list)
        self._map_select.param.watch(self.__set_keys__, 'value')
        self._key_input = pn.widgets.AutocompleteInput(min_characters=1, sizing_mode='fixed',
            name='Key', options=key_list,
            placeholder='Enter key')
        self._key_input.param.watch(self.__key_input_value_changed__, 'value')
        self._get_button = pn.widgets.Button(name='get()', width=self.button_width, button_type='primary')
        self._get_button.on_click(self.__click_get_button__)

        df = pd.DataFrame({})   
        self._map_blotter = pn.widgets.Tabulator(df, width=self.width, disabled=True,
                                                 pagination='local', 
                                                 page_size=self.page_size,
                                                 sizing_mode='stretch_width')

        self._json_editor = pn.widgets.JSONEditor(name='Object', mode='tree', value={}, height=800)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 self._map_select, self._key_input, self._get_button,
                                 self._map_blotter, self._json_editor)
        self._sync_widgets()

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.map_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        # Return if reset is False, i.e., do not refresh keys
        if is_reset == False:
            old_name_list = self._map_select.options
            if ds_name_list == old_name_list:
                return

        # If reset, then get new ket set.
        self._map_select.options = ds_name_list
        if is_reset:
            self.__set_keys__(None)
        else:
            self._key_input.value = ''
        self._sync_widgets()
    
    def __panel__(self):
        return self._layout
    
    def __set_keys__(self, event):
        map_name = self._map_select.value
        map = None
        if self.hazelcast_cluster != None:
            map = self.hazelcast_cluster.get_ds(self.map_type, map_name)
        else:
            map = None
        list= HazelcastUtil.get_key_list(map)
        self._key_input.options=list
        self._key_input.value = ''

    def __key_input_value_changed__(self, event):
        self.__update_component__()
    
    def __click_get_button__(self, event):
        self.__update_component__()

    def __update_component__(self):
        map_name = self._map_select.value
        key = self._key_input.value
        self._status_text.value = ''
        data = {}
        if map_name != None and len(key) > 0:
            is_exception = False
            if self.hazelcast_cluster != None:
                map = self.hazelcast_cluster.get_ds(self.map_type, map_name)
            else:
                map = None
            if map != None:
                obj = map.get(key)
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
            
            df = pd.DataFrame(data)
            self._map_blotter.value = df

            if is_exception:
                self._json_editor.value = {}
            else:
                json = HazelcastUtil.object_to_json(obj)
                self._json_editor.value = json

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._key_input.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._key_input.value
    
    def __clear_status__(self):
        self._status_text.value = ''

    def execute_map(self, map):
        return

    def select(self, ds_name):
        self.__clear_status__()
        self._map_select.value = ds_name
        if self.hazelcast_cluster != None:
            map = self.hazelcast_cluster.get_ds(self.map_type, ds_name)
        else:
            map = None
        self.execute_map(map)
