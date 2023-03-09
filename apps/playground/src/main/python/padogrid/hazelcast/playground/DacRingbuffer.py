# %%
"""
DacRingbuffer displays Hazelcast Set contents
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

class DacRingbuffer(DacBase, Viewer):
    
    ds_type = 'Ringbuffer'

    _on_click_callback = None

    width = param.Integer(1000)
    page_size = param.Integer(10)
    
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        int_input_width = 100
        ds_name_list = []

        self._status_text = pn.widgets.TextInput(disabled=True, value='')

        self._ringbuffer_select = pn.widgets.Select(name='Ringbuffer', sizing_mode='fixed', options=ds_name_list)
        self._ringbuffer_select.param.watch(self.__ringbuffer_select_value_changed__, 'value')
        self._ds_size_text = pn.widgets.TextInput(name='size()', disabled=True, width=self.size_text_width)
        self._capacity_text = pn.widgets.TextInput(name='capacity()', disabled=True, width=self.size_text_width)
        self._remaining_capacity_text = pn.widgets.TextInput(name='remaining_capacity()', disabled=True, width=self.size_text_width)

        self._destroy_button = pn.widgets.Button(name='destroy()', button_type='primary', width=self.button_width)
        self._destroy_button.on_click(self.__destroy_execute__)

        self._add_button = pn.widgets.Button(name='add()', button_type='primary', width=self.button_width)    
        self._add_button.on_click(self.__add_execute__)
        self._add_select = pn.widgets.Select(name='Object', options=self.obj_type_list, width=160)
        self._add_input = pn.widgets.IntInput(name='Count', value=1, step=1, start=1, width=int_input_width)

        self._head_sequence_text = pn.widgets.TextInput(name='head_sequence()', disabled=True, width=self.size_text_width)   
        self._tail_sequence_text = pn.widgets.TextInput(name='tail_sequence()', disabled=True, width=self.size_text_width)
        
        
        self._remaining_sequence_button = pn.widgets.Button(name='remaining_capacity()', button_type='primary', width=self.button_width) 
        self._remaining_sequence_text = pn.widgets.TextInput(disabled=True, width=self.size_text_width)

        self._read_one_button = pn.widgets.Button(name='read_one()', button_type='primary', width=self.button_width)
        self._read_one_start_sequence_input = pn.widgets.IntInput(name='start_sequence', value=0, step=1, start=0, width=int_input_width)
        self._read_one_button.on_click(self.__read_one_execute__)

        self._read_many_button = pn.widgets.Button(name='read_many()', button_type='primary', width=self.button_width)
        self._read_many_start_sequence_input = pn.widgets.IntInput(name='start_sequence', value=0, step=1, start=0, width=int_input_width)
        self._read_many_min_count_input = pn.widgets.IntInput(name='min_count', value=1, step=1, start=1, width=int_input_width)
        self._read_many_max_count_input = pn.widgets.IntInput(name='max_count', value=1, step=1, start=1, width=int_input_width)
        self._read_many_button.on_click(self.__read_many_execute__)

        self._new_button = pn.widgets.Button(name='New', button_type='primary', width=self.button_width)
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=180)
        self._new_text.param.watch(self.__new_execute__, 'value')


        df = pd.DataFrame({})
        self._data_list = []
        self._ds_table = pn.widgets.Tabulator(df, width=self.width, height=370,
                                              disabled=True, pagination='local', 
                                              page_size=self.page_size,
                                              sizing_mode='stretch_width')                                    
        self._ds_table.on_click(self.__ds_table_execute__)
        self._ds_table.selectable = 1

        self._json_editor = pn.widgets.JSONEditor(name='Object', mode='tree', value={}, height=800)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 pn.Row(self._new_button, self._new_text),
                                 pn.Row(self._ringbuffer_select, self._ds_size_text),
                                 pn.Row(self._head_sequence_text, self._tail_sequence_text),
                                 pn.Row(self._capacity_text, self._remaining_capacity_text),
                                 pn.Row(self._destroy_button),
                                 pn.Row(self._add_button, self._add_select, self._add_input),
                                 pn.Row(self._read_one_button, self._read_one_start_sequence_input),
                                 pn.Row(self._read_many_button, self._read_many_start_sequence_input, self._read_many_min_count_input, self._read_many_max_count_input),
                                 self._ds_table, self._json_editor)
        self._sync_widgets()
        
    def refresh(self, is_reset=False):
        ds_name = self._ringbuffer_select.value
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        old_name_list = self._ringbuffer_select.options
        if ds_name_list == old_name_list:
            return

        self._ringbuffer_select.options = ds_name_list
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        self.select(ds_name)
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._ringbuffer_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._ringbuffer_select.value

    def __ringbuffer_select_value_changed__(self, event):
        if self.hazelcast_cluster == None:
            return
        ds_name = event.obj.value
        self.select(ds_name)
        self.__update_select__()

    def __update_select__(self):
        if len(self._data_list) > 0:
            for obj in self._data_list:
                obj_name = self.get_object_name(obj)
                self._add_select.options = [obj_name]
                break
        else:
            self._add_select.options = self.obj_type_list

    def __clear_status__(self):
        self._status_text.value = ''

    def __ds_table_execute__(self, event):
        self.__clear_status__()
        row = event.row
        if row < len(self._data_list):
            obj = self._data_list[row]
            json = HazelcastUtil.object_to_json(obj)
            self._json_editor.value = json

    def __get_execute__(self, event):
        self.__clear_status__()
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

    def __refresh_size__(self):
        ds_name = self._map_select.value
        if ds_name == None:
            self._ds_size_text.value = ''
        else:
            ds_size = None
            if self.hazelcast_cluster != None:
                ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
            if ds_size == None:
                self._ds_size_text.value = str('N/A')
            else:
                self._ds_size_text.value = str(ds_size)
                self._index_input.end = ds_size-1

    def __update_component__(self, obj = None):
        self.__clear_status__()
        data = {}
        is_exception = False
        if obj != None:
            try:
                obj = HazelcastUtil.get_future_value(obj)
                try:
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
        self._ds_table.value = df

        if is_exception or obj == None:
            self._json_editor.value = {}
        else:
            attr_dict = get_attributes(obj)
            json = HazelcastUtil.object_to_json(obj)
            self._json_editor.value = json
        self.__refresh_size__()

    def __new_execute__(self, event):
        ds_name = self._new_text.value.strip()
        if len(ds_name) == 0:
            return
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds = self.hazelcast_cluster.hazelcast_client.get_ringbuffer(ds_name)
            self.hazelcast_cluster.refresh()
            self._ringbuffer_select.value = ds_name

    def __destroy_execute__(self, event):
        self.__clear_status__()
        ds_name = self._ringbuffer_select.value
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            if self.hazelcast_cluster != None:
                ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
            else:
                ds = None
            if ds != None:
                ds.destroy()
                self.hazelcast_cluster.refresh()

    def select(self, ds_name):
        self.__clear_status__()
        self._ringbuffer_select.value = ds_name
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            future = ds.read_many(start_sequence=0, min_count=0, max_count=100)
            self._data_list = HazelcastUtil.get_future_value(future)
        self.execute_ringbuffer(ds)
    
    def __add_execute__(self, event):
        if self.hazelcast_cluster == None or self.hazelcast_cluster.hazelcast_client == None:
            return
        ds_name = self._ringbuffer_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = self._add_input.value
            obj_type = self._add_select.value
            create_function = self.get_obj_creation_function(obj_type)
            self.ingestor.ingest_ringbuffer(ds_name, create_function, None, count=count)
            self.hazelcast_cluster.refresh()
            self.__update_select__()
    
    def on_click(self, callback=None):
        self._on_click_callback = callback

    def __read_one_execute__(self, event):
        self.__clear_status__()
        if self.hazelcast_cluster == None or self.hazelcast_cluster.hazelcast_client == None:
            return
        ds_name = self._ringbuffer_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            start_sequence = self._read_one_start_sequence_input.value
            tail_sequence = HazelcastUtil.get_future_value(ds.tail_sequence())
            try:
                if start_sequence > tail_sequence:
                    self._status_text.value = f'ERROR: read_one() - start_squence > tail_sequence. Aborted.'
                else:
                    future = ds.read_one(start_sequence)
                    obj = HazelcastUtil.get_future_value(future)
                    self._data_list = []
                    if obj != None:
                        self._data_list.append(obj)
            except Exception as ex:
                self._status_text.value = f'ERROR: read_one() - {repr(ex)}'
        self.execute_ringbuffer(ds)

    def __read_many_execute__(self, event):
        self.__clear_status__()
        if self.hazelcast_cluster == None or self.hazelcast_cluster.hazelcast_client == None:
            return
        ds_name = self._ringbuffer_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            start_sequence = self._read_many_start_sequence_input.value
            min_count = self._read_many_min_count_input.value
            max_count = self._read_many_max_count_input.value
            tail_sequence = HazelcastUtil.get_future_value(ds.tail_sequence())
            is_error = False
            try:
                if start_sequence > tail_sequence:
                    self._status_text.value = f'ERROR: read_many() - start_sequence > tail_sequence. Aborted.'
                    is_error = True
                else:
                    if min_count > tail_sequence - start_sequence + 1:
                        self._status_text.value = f'ERROR: read_many() - min_count > (tail_sequence-start_sequence+1). Aborted.'
                        is_error = True
                    else:
                        future = ds.read_many(start_sequence, min_count, max_count)
                        self._data_list = HazelcastUtil.get_future_value(future)
            except Exception as ex:
                is_error = True
                self._status_text.value = f'ERROR: read_many() - {repr(ex)}'
            if is_error == False:
                self.execute_ringbuffer(ds)
        
    def execute_ringbuffer(self, ringbuffer):
        data = {}
        self._ds_table.page = 1
        is_exception = False
        if ringbuffer == None:
            ds_name = None
            size = -1
            capacity = -1
            remaining_capacity = -1
            head_sequence = -1
            tail_sequence = -1
        else:
            ds_name = ringbuffer.name
            future_list = []
            future_list.append(ringbuffer.size())
            future_list.append(ringbuffer.capacity())
            future_list.append(ringbuffer.remaining_capacity())
            future_list.append(ringbuffer.tail_sequence())
            future_list.append(ringbuffer.head_sequence())
        
            size = HazelcastUtil.get_future_value(future_list[0])
            capacity = HazelcastUtil.get_future_value(future_list[1])
            remaining_capacity = HazelcastUtil.get_future_value(future_list[2])
            tail_sequence = HazelcastUtil.get_future_value(future_list[3])
            head_sequence = HazelcastUtil.get_future_value(future_list[4])
            
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
            try:
                self._data_list = self._data_list._list_obj.copy()
            except:
                # Thrown if self._data_list is not ImmutableLazyDataList
                pass
            
            df = pd.DataFrame(data)   
            if is_exception == True:
                if self._ringbuffer_select.value != ds_name:
                    self._ringbuffer_select.value = ds_name
            else:
                self._ringbuffer_select.value = ds_name
            self._ds_table.value = df
            self._status_text.value = status
            self._ds_size_text.value = str(size)
            self._remaining_capacity_text.value=str(remaining_capacity)
            self._capacity_text.value = str(capacity)
            self._head_sequence_text.value = str(head_sequence)
            self._tail_sequence_text.value = str(tail_sequence)
