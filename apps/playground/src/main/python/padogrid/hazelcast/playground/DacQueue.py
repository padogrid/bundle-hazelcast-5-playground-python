# %%
"""
DacQueue displays the specified queue data.
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

class DacQueue(DacBase, Viewer):
    
    ds_type = 'Queue'

    _on_click_callback = None

    width = param.Integer(1000)
    page_size = param.Integer(20)
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        self._status_text = pn.widgets.TextInput(disabled=True, value='')
        ds_name_list = []
        self._queue_select = pn.widgets.Select(name='Queues', sizing_mode='fixed', options=ds_name_list)
        self._queue_select.param.watch(self.__queue_value_changed__, 'value')
        self._ds_size_text = pn.widgets.TextInput(name='Size', disabled=True, width=self.size_text_width)    
        self._peek_button = pn.widgets.Button(name='peek()', button_type='primary', width=self.button_width)    
        self._peek_button.on_click(self.__peek_execute__)
        self._poll_button = pn.widgets.Button(name='poll()', button_type='primary', width=self.button_width)
        self._poll_button.on_click(self.__poll_execute__)
        self._clear_button = pn.widgets.Button(name='clear()', button_type='primary', width=self.button_width)
        self._clear_button.on_click(self.__clear_button_on_click__)
        
        df = pd.DataFrame({}) 
        self._blotter = pn.widgets.Tabulator(df, width=self.width, disabled=True, pagination='local', 
                                    page_size=self.page_size, sizing_mode='stretch_width', buttons={'Remove': "<i class='fa fa-trash'></i>"})                                    
        self._json_editor = pn.widgets.JSONEditor(name='Object', mode='tree', value={}, height=800)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                            pn.Row(self._queue_select, self._ds_size_text),
                            pn.Row(self._peek_button, self._poll_button, self._clear_button),
                            self._blotter, self._json_editor)
        self._sync_widgets()

    def clear(self):
        # It takes some time to clear the map. Assume the queue properly clears.
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh()
            data = {}
            self._blotter.page = 1
            df = pd.DataFrame(data)
            self._blotter.value = df
            self._json_editor.value = {}
        
    def refresh(self, is_reset=False):
        ds_name = self._queue_select.value
        ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        ds_name_list.sort()
        old_name_list = self._queue_select.options
        if ds_name_list == old_name_list:
            self.__refresh_size__()
            return

        self._queue_select.options = ds_name_list
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        self.select(ds_name)
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    def __queue_value_changed__(self, event):
        self.select(event.obj.value)

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._queue_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._queue_select.value

    def __peek_execute__(self, event):
        obj = None
        try:
            obj = self.peek()
            self.__update_component__(obj)
            self.__refresh_size__()
            if self._on_click_callback != None:
                self._on_click_callback(obj, exception=None)
        except Exception as ex:
            self.__update_component__(obj)
            self.__refresh_size__()
            self._status_text.value = repr(ex)
            if self._on_click_callback != None:
                self._on_click_callback(obj, exception=ex)

    def __poll_execute__(self, event):   
        obj = None
        try:
            obj = self.poll()
            self.__update_component__(obj)
            if self.hazelcast_cluster != None:
                self.hazelcast_cluster.refresh()
            if self._on_click_callback != None:
                self._on_click_callback(obj, exception=None)
        except Exception as ex:
            self.__update_component__(obj)
            self.__refresh_size__()
            self._status_text.value = repr(ex)
            if self._on_click_callback != None:
                self._on_click_callback(obj, exception=ex)

    def __clear_button_on_click__(self, event):
        ds_name = self._queue_select.value
        if ds_name != None and ds_name != '': 
            if self.hazelcast_cluster != None:
                queue = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
                if queue != None:
                    queue.clear()
                self.clear()

    def __refresh_size__(self):
        ds_name = self._queue_select.value
        if ds_name == None:
            self._ds_size_text.value = ''
        else:
            ds_size = self.hazelcast_cluster.get_ds_size(self.ds_type, ds_name)
            self._ds_size_text.value = str(ds_size)

    def __update_component__(self, obj):
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
        self._queue_select.value = ds_name
        obj = None
        try:
            obj = self.peek()
            self._status_text.value = repr(obj)
        except Exception as ex:
            self._status_text.value = repr(ex)
        self.__update_component__(obj)

    def peek(self):
        ds_name = self._queue_select.value
        result = None
        if ds_name != None:
            queue = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
            if queue != None:
                future = queue.peek()
                result = HazelcastUtil.get_future_value(future)
        return result
    
    def poll(self):
        ds_name = self._queue_select.value
        result = None
        if ds_name != None:
            queue = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
            if queue != None:
                future = queue.poll()
                result = HazelcastUtil.get_future_value(future)
        return result
    
    def on_click(self, callback=None):
        self._on_click_callback = callback
