# %%
"""
DacPNCounter displays Hazelcast PNCounter contents
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

class DacPNCounter(DacBase, Viewer):
    
    ds_type = 'PNCounter'

    width = param.Integer(1000)
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        button_width = 140
        int_input_width = 100
        text_input_width = 100

        self._status_text = pn.widgets.TextInput(disabled=True, value='', width=self.width)
        ds_name_list = []
        self._pn_counter_select = pn.widgets.Select(name='PNCounters', sizing_mode='fixed', options=ds_name_list)
        self._pn_counter_select.param.watch(self.__pn_counter_value_changed__, 'value')

        self._get_text = pn.widgets.TextInput(disabled=True, width=text_input_width)
        self._get_button = pn.widgets.Button(name='get()', button_type='primary', width=button_width)    
        self._get_button.on_click(self.__get_execute__)

        self._get_and_add_button = pn.widgets.Button(name='get_and_add()', button_type='primary', width=button_width)
        self._get_and_add_button.on_click(self.__get_and_add_execute__)
        self._get_and_add_input = pn.widgets.IntInput(value=1, step=1, start=1, width=int_input_width)
        self._get_and_add_text = pn.widgets.TextInput(disabled=True, width=text_input_width)

        self._add_and_get_button = pn.widgets.Button(name='add_and_get()', button_type='primary', width=button_width)
        self._add_and_get_button.on_click(self.__add_and_get_execute__)
        self._add_and_get_input = pn.widgets.IntInput(value=1, step=1, start=1, width=int_input_width)
        self._add_and_get_text = pn.widgets.TextInput(disabled=True, width=text_input_width)
        
        self._get_and_subtract_button = pn.widgets.Button(name='get_and_subtract()', button_type='primary', width=button_width)
        self._get_and_subtract_input = pn.widgets.IntInput(value=1, step=1, start=1, width=int_input_width)
        self._get_and_subtract_button.on_click(self.__get_and_subtract_execute__)
        self._get_and_subtract_text = pn.widgets.TextInput(disabled=True, width=text_input_width)

        self._subtract_and_get_button = pn.widgets.Button(name='subtract_and_get()', button_type='primary', width=button_width)
        self._subtract_and_get_input = pn.widgets.IntInput(value=1, step=1, start=1, width=int_input_width)
        self._subtract_and_get_button.on_click(self.__subtract_and_get_execute__)
        self._subtract_and_get_text = pn.widgets.TextInput(disabled=True, width=text_input_width)

        self._reset_button = pn.widgets.Button(name='reset()', button_type='primary', width=button_width)
        self._reset_button.on_click(self.__reset_execute__)

        self._destroy_button = pn.widgets.Button(name='destroy()', button_type='primary', width=button_width)
        self._destroy_button.on_click(self.__destroy_execute__)

        self._new_button = pn.widgets.Button(name='New', button_type='primary', width=button_width)
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=220)
        self._new_text.param.watch(self.__new_execute__, 'value')

        # ds_table
        name_list = []
        count_list = []
        count_bar_list = []
        # (type.ds_name, ds) pairs
        self._df = pd.DataFrame({
            'Name': name_list,
            'Count': count_list,
            'Bar' : count_bar_list
        })
        tabulator_formatters = {
            'Bar': {'type': 'progress', 'min': 0, 'max': 100},
        }
        self._ds_table = pn.widgets.Tabulator(self._df, name='PNCounters', disabled=True, 
                    formatters=tabulator_formatters,
                    widths={'Bar': 400})

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                            pn.Row(self._new_button, self._new_text),
                            pn.Row(self._pn_counter_select),
                            pn.Row(self._get_button, self._get_text), 
                            pn.Row(self._get_and_add_button, self._get_and_add_text, self._get_and_add_input),
                            pn.Row(self._add_and_get_button, self._add_and_get_text, self._add_and_get_input),
                            pn.Row(self._get_and_subtract_button, self._get_and_subtract_text, self._get_and_subtract_input),
                            pn.Row(self._subtract_and_get_button, self._subtract_and_get_text, self._subtract_and_get_input),
                            pn.Row(self._reset_button),
                            pn.Row(self._destroy_button),
                            pn.Row(self._ds_table))
        self._sync_widgets()
        
    def refresh(self, is_reset=False):
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

        old_name_list = self._pn_counter_select.options
        if ds_name_list == old_name_list:
            return

        self._pn_counter_select.options = ds_name_list
        if ds_name == None and len(ds_name_list) > 0:
            ds_name = ds_name_list[0]
        self.select_pn_counter(ds_name)

        count_list = []
        count_bar_list = []
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.get())
            count_list.append(count)
            count_bar_list.append(count)
        if len(count_list) > 0:
            min_count = min(count_list)
            max_count = max(count_list)
        else:
            min_count = 0
            max_count = 100
        self._df = pd.DataFrame({
            'Name': ds_name_list,
            'Count': count_list,
            'Bar' : count_bar_list
        })
        tabulator_formatters = {
            'Bar': {'type': 'progress', 'min': min_count, 'max': max_count},
        }
        self._ds_table.value = self._df
        self._ds_table.formatters=tabulator_formatters
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    def __pn_counter_value_changed__(self, event):
        self.select_pn_counter(event.obj.value)

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._pn_counter_select.value
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._pn_counter_select.value

    def __update_row__(self, pn_counter_name, count):
        for index, row in self._df.iterrows():
            if row['Name'] == pn_counter_name:
                self._ds_table.patch({
                    'Count' : [
                            (index, count)
                        ],
                    'Bar' : [
                            (index, count)
                        ]
                    })
                break
        
    def __get_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if ds_name == None:
            return
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.get())
            self._get_text.value = str(count)
            self.__update_row__(ds_name, count)

    def __get_and_add_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.get_and_add(self._get_and_add_input.value))
            self._get_and_add_text.value = str(count)
            self.__get_execute__(None)

    def __add_and_get_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.add_and_get(self._add_and_get_input.value))
            self._add_and_get_text.value = str(count)
            self.__get_execute__(None)

    def __get_and_subtract_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.get_and_subtract(self._get_and_subtract_input.value))
            self._get_and_subtract_text.value = str(count)
            self.__get_execute__(None)

    def __subtract_and_get_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = HazelcastUtil.get_future_value(ds.subtract_and_get(self._subtract_and_get_input.value))
            self._subtract_and_get_text.value = str(count)
            self.__get_execute__(None)

    def __reset_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            count = ds.reset()

    def __new_execute__(self, event):
        ds_name = self._new_text.value
        ds_name = ds_name.strip()
        if len(ds_name) == 0:
            return
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds_name = self.hazelcast_cluster.hazelcast_client.get_pn_counter(ds_name)
            self.hazelcast_cluster.refresh()
            self._pn_counter_select.value = ds_name

    def __destroy_execute__(self, event):
        self.__clear_status__()
        ds_name = self._pn_counter_select.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds != None:
            ds.destroy()
            self.hazelcast_cluster.refresh()

    def __clear_status__(self):
        self._status_text.value = ''

    def select_pn_counter(self, ds_name):
        self.__clear_status__()
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        if ds == None:
            return
        self._pn_counter_select.value = ds_name
        self.__get_execute__(None)
        self._get_and_add_text.value = ''
        self._add_and_get_text.value = ''
        self._get_and_subtract_text.value = ''
        self._subtract_and_get_text.value = ''
