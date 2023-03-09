# %%
"""
DacSet displays Hazelcast Set contents
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

class DacSet(DacBase, Viewer):
    
    ds_type = 'Set'

    _on_click_callback = None

    width = param.Integer(1000)
    page_size = param.Integer(10)
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        int_input_width = 100
        text_input_width = 100
        ds_name_list = []

        self._status_text = pn.widgets.TextInput(disabled=True, value='')

        self._set_select = pn.widgets.Select(name='Sets', sizing_mode='fixed', options=ds_name_list)
        self._set_select.param.watch(self.__set_select_value_changed__, 'value')
        self._ds_size_text = pn.widgets.TextInput(name='Size', disabled=True, width=self.size_text_width)

        self._destroy_button = pn.widgets.Button(name='destroy()', button_type='primary', width=self.button_width)
        self._destroy_button.on_click(self.__destroy_execute__)

        self._add_button = pn.widgets.Button(name='add()', button_type='primary', width=self.button_width)    
        self._add_button.on_click(self.__add_execute__)
        self._add_select = pn.widgets.Select(name='Object', options=self.obj_type_list, width=160)
        self._add_input = pn.widgets.IntInput(name='Count', value=1, step=1, start=1, width=int_input_width)

        self._remove_button = pn.widgets.Button(name='remove()', button_type='primary', width=self.button_width)
        self._remove_button.on_click(self.__remove_execute__)

        self._clear_button = pn.widgets.Button(name='clear()', button_type='primary', width=self.button_width)
        self._clear_button.on_click(self.__clear_button_on_click__)

        self._new_button = pn.widgets.Button(name='New', button_type='primary', width=self.button_width)
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=180)
        self._new_text.param.watch(self.__new_execute__, 'value')

        df = pd.DataFrame({})
        self._data_list = []
        self._ds_table = pn.widgets.Tabulator(df, width=self.width, height=370,
                                              disabled=True, pagination='local', 
                                              page_size=self.page_size,
                                              sizing_mode='stretch_width',
                                              selectable='checkbox',
                                              buttons={'Remove': "<i class='fa fa-trash'></i>"})                                    
        self._ds_table.on_click(self.__ds_table_execute__)
        # self._ds_table.selectable = 1
        self._json_editor = pn.widgets.JSONEditor(name='Object', mode='tree', value={}, height=800)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 pn.Row(self._new_button, self._new_text),
                                 pn.Row(self._set_select, self._ds_size_text),
                                 pn.Row(self._destroy_button),
                                 pn.Row(self._add_button, self._add_select, self._add_input),
                                 pn.Row(self._remove_button, self._clear_button),
                                 self._ds_table, self._json_editor)
        self._sync_widgets()

    def refresh(self, is_reset=False):
        ds_name = self._set_select.value
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        # Return if reset is False, i.e., skip refreshing component
        if is_reset == False:
            old_name_list = self._set_select.options
            if ds_name_list == old_name_list:
                self.__refresh_size__()
                return

        self._set_select.options = ds_name_list
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        self.select(ds_name)
        self._sync_widgets()

    def clear(self):
        data = {}
        self._ds_table.page = 1
        df = pd.DataFrame(data)
        self._ds_table.value = df
        self._json_editor.value = {}
        # It takes some time to clear the map. Assume the queue properly clears.
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh()
        # self.__refresh_size__()
        
    def __panel__(self):
        return self._layout
    
    def __set_select_value_changed__(self, event):
        if self.hazelcast_cluster == None:
            return
        ds_name = event.obj.value
        self.select(ds_name)
        if len(self._data_list) > 0:
            for obj in self._data_list:
                obj_name = self.get_object_name(obj)
                self._add_select.options = [obj_name]
                break
        else:
            self._add_select.options = self.obj_type_list

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._set_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._set_select.value

    def __clear_status__(self):
        self._status_text.value = ''

    def __ds_table_execute__(self, event):
        self.__clear_status__()
        row = event.row
        if row < len(self._data_list):
            obj = self._data_list[row]
            json = HazelcastUtil.object_to_json(obj)
            self._json_editor.value = json

    def __remove_execute__(self, event):
        self.__clear_status__()
        if self.hazelcast_cluster == None:
            return
        
        ds_name = self._set_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds == None:
            return
        
        selected_rows = self._ds_table.selection
        
        if len(selected_rows) > 0:
            is_removed = False
            count = 0
            try:
                for row in selected_rows:
                    index = row - count
                    obj = self._data_list[index]
                    if HazelcastUtil.get_future_value(ds.remove(obj)):
                        is_removed = True
                    del self._data_list[index]
                    count += 1
            except Exception as ex:
                self.__refresh_size__()
                self._status_text.value = repr(ex)
            
            # is_removed is True if one or more removed successfully
            if is_removed:
                self.hazelcast_cluster.refresh(is_reset=True)
                self._status_text.value = f'Removed entries: {count}'
    
        if self._on_click_callback != None:
            self._on_click_callback(obj, exception=ex)

    def __clear_button_on_click__(self, event):
        self.__clear_status__()
        ds_name = self._set_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            ds.clear()
        self.clear()

    def __refresh_size__(self):
        ds_name = self._set_select.value
        if ds_name == None:
            self._ds_size_text.value = ''
        else:
            if self.hazelcast_cluster != None:
                ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
            else:
                ds_size = None
            if ds_size == None:
                self._ds_size_text.value = 'N/A'
            else:
                self._ds_size_text.value = str(ds_size)

    def __new_execute__(self, event):
        ds_name = self._new_text.value.strip()
        if len(ds_name) == 0:
            return
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds_from_hz(self.ds_type, ds_name)
            self.hazelcast_cluster.refresh()
            self._set_select.value = ds_name

    def __destroy_execute__(self, event):
        self.__clear_status__()
        ds_name = self._set_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            ds.destroy()
        self.hazelcast_cluster.refresh()

    def select(self, ds_name):
        self.__clear_status__()
        self._set_select.value = ds_name
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        self.execute_set(ds)
    
    def __add_execute__(self, event):
        if self.hazelcast_cluster == None or self.hazelcast_cluster.hazelcast_client == None:
            return
        ds_name = self._set_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = self._add_input.value
            obj_type = self._add_select.value
            create_function = self.get_obj_creation_function(obj_type)
            self.ingestor.ingest_set(ds_name, create_function, None, count=count)
            # If _set_select has more than one item, this was the first
            # item added. Change the options to allow only one object type.
            if self._add_select.size > 1:
                self._add_select.options = [obj_type]
            self.hazelcast_cluster.refresh()
    
    def on_click(self, callback=None):
        self._on_click_callback = callback

    def execute_set(self, set):
        data = {}
        self._ds_table.page = 1
        is_exception = False
        if set == None:
            ds_name = None
        else:
            ds_name = set.name
            future = set.get_all()
            self._data_list = HazelcastUtil.get_future_value(future)
            status = ''
            if len(self._data_list) > 0:
                try:
                    value = self._data_list[0]
                    class_name = get_class_name(value)
                    is_builtin_type = class_name.startswith('builtin')
                    is_json = class_name == "hazelcast.core.HazelcastJsonValue"

                    data = {}
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

                    for value in self._data_list:
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
                    is_exception = True
            
            # Hazelcast returns ImmutableLazyDataList, which keeps
            # deserialized objects in ImmutableLazyDataList._list_obj.
            # Let's clone that list so that we can freely remove items
            # from the cloned list.
            self._data_list = self._data_list._list_obj.copy()
            
            df = pd.DataFrame(data)   
            # A bug in Panel. title gets cleared. Use title_text instead.
            if is_exception == True:
                if self._set_select.value != ds_name:
                    self._set_select.value = ds_name
            else:
                if ds_name == None:
                    self._set_select.value = "(Set undefined)"
                else:
                    self._set_select.value = ds_name
            self._ds_table.value = df
            self._status_text.value = status
            self._ds_size_text.value = str(HazelcastUtil.get_future_value(set.size()))
