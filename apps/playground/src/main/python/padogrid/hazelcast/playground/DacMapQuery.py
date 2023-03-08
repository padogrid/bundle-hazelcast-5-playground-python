# %%
"""
DacMapQuery provides an SQL Query components for executing ad-hoc queries.
"""

import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_class_name
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil
from padogrid.hazelcast.playground.DacBase import DacBase

class DacMapQuery(DacBase, Viewer):
    
    _on_click_callback = None
    width = param.Integer(1000)
    page_size = param.Integer(20)
    value = None

    title_text = param.String("Map")
    
    def __init__(self, **params):
        super().__init__(**params)
        
        self._status_text = pn.widgets.input.TextInput(disabled=True)
        self._sql_input = pn.widgets.input.TextAreaInput(name='SQL', placeholder='select * from ', height=200)
        self._sql_input.value = "select * from "
        self._sql_input.param.watch(self.__sql_execute__, 'value')
        self._sql_execute_button = pn.widgets.Button(name='Execute Query', button_type='primary', width=self.button_width)    
        self._sql_execute_button.on_click(self.__sql_execute__)
        self._table_header_text = pn.widgets.input.TextInput(disabled=True)

        df = pd.DataFrame({})        
        self._map_blotter = pn.widgets.Tabulator(df, width=self.width, disabled=True, 
                                                 pagination='local', page_size=self.page_size, 
                                                 sizing_mode='stretch_width')
        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 self._sql_input, self._sql_execute_button, 
                                 self._table_header_text,
                                 self._map_blotter)
        self._sync_widgets()
    
    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._sql_input.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._sql_input.value
    
    def __sql_execute__(self, event):
        query = self._sql_input.value
        self.execute_query(query)
        if self._on_click_callback != None:
            self._on_click_callback(self.title_text)
    @property
    def title_text(self):
        return self._table_header_text.value
    
    def on_click(self, callback=None):
        self._on_click_callback = callback
        
    def refresh(self, is_reset=False):
        # Noththing to do
        return
        
    def execute_query(self, query):
        result = None
        try:
            if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
                result = self.hazelcast_cluster.hazelcast_client.sql.execute(query).result()
                status = ''
            else:
                status = 'Hazelcast client not available'
        except Exception as ex:
            # Create mapping
            status = ex.suggestion
            if status == None:
                status = ''
            if ex.suggestion != None:
                try:
                    status = status.replace('\n', ' ')
                    result = self.hazelcast_cluster.hazelcast_client.sql.execute(status).result()
                except:
                    # assume mapping created
                    result = None
                try:
                    result = self.hazelcast_cluster.hazelcast_client.sql.execute(query).result()
                except Exception as ex:
                    if status == "":
                        status = repr(ex)
                    else:
                        status = status + " - " + repr(ex)
        self._status_text.value = status 

        data = {}
        if result != None:
            count = 0
            try:
                is_json = False
                for row in result:
                    columns = row.metadata.columns
                    count = count + 1
                    if count == 1:
                        for column in columns:
                            value = row[column.name]
                            class_name = get_class_name(value)
                            if class_name == "hazelcast.core.HazelcastJsonValue":
                                is_json = True
                                value = HazelcastUtil.object_to_json(value)
                                for c, v in value.items():
                                    data[c] = []
                            else:
                                data[column.name] = []
                    
                    if is_json:
                        for column in columns:
                            value = row[column.name]
                            class_name = get_class_name(value)
                            if class_name == "hazelcast.core.HazelcastJsonValue":
                                value = HazelcastUtil.object_to_json(value)
                                for c, v in value.items():
                                    data[c].append(v)
                            else:
                                data[column.name].append(value)
                    else:
                        for column in columns:
                            try:
                                value = row[column.name]
                                class_name = get_class_name(value)
                                if class_name == "builtins.bytearray":
                                    value = "bytearray"
                            except:
                                value = "{}"
                            data[column.name].append(value)
            except:
                # Ignore - unable to iterate
                data = {}
        df = pd.DataFrame(data)
        # df.max_rows = 10000
        self._table_header_text.value = query
        self._map_blotter.value = df
        # A bug in Tabulator. If page is set to 1 for an empty df, it throws an exception
        if df.size > 0:
            self._map_blotter.page = 1
        
    def execute_map(self, map):
        data = {}
        is_exception = False
        if map == None:
            map_name = None
        else:
            map_name = map.name
            future = map.entry_set()
            obj_type=str(type(future))
            if obj_type == "<class 'hazelcast.future.Future'>":
                entry_set=future.result()
            else:
                entry_set=future

            status = ''
            if len (entry_set) > 0:
                try:
                    for entry in entry_set:
                        key = entry[0]
                        value = entry[1]
                        break;

                    attr_dict = get_attributes(value)
                    data = {}
                    keys = attr_dict.keys()
                    data['__key'] = []
                    for key in keys:
                        data[key] = []
                    for entry in entry_set:
                        key = entry[0]
                        value = entry[1]
                        data['__key'].append(key)
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
                self._table_header_text.value = map_name
            else:
                if map_name == None:
                    self._table_header_text.value = "(Map undefined)"
                else:
                    self._table_header_text.value = map_name
            self._map_blotter.value = df
            if df.size > 0:
                self._map_blotter.page = 1
            self._status_text.value = status
