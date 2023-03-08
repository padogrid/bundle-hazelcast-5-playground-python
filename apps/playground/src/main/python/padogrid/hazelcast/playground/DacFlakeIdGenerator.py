# %%
"""
DacFlakeIdGenerator displays Hazelcast DacFlakeIdGenerator data structures
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

class DacFlakeIdGenerator(DacBase, Viewer):

    ds_type = 'FlakeIdGenerator'
    
    width = param.Integer(1000)
    value = None

    def __init__(self, **params):
        super().__init__(**params)
        
        self._new_button = pn.widgets.Button(name='New', button_type='primary', width=self.button_width)
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=220)
        self._new_text.param.watch(self.__new_execute__, 'value')
    
        self._status_text = pn.widgets.TextInput(disabled=True, value='', width=self.width)

        # ds_table
        name_list = ['']
        id_list = [-1]
        self._df = pd.DataFrame({
            'Name': name_list,
            'Id': id_list,
        })
        self._blotter = pn.widgets.Tabulator(self._df, name='FlakeIdGenerator', disabled=True, 
                    widths={'Id': 200},
                    buttons={
                        'New': '<i class="fa fa-play"></i>',
                        'Destroy': '<i class="fa fa-trash"></i>'
                    })
        self._blotter.on_click(self.__blotter_on_click__)

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                            pn.Row(self._new_button, self._new_text),
                            pn.Row(self._blotter))
        self._sync_widgets()
        
    def refresh(self, is_reset=False):
        if self.hazelcast_cluster != None:
            flake_id_generator_dict = self.hazelcast_cluster.flake_id_generator_dict
        else:
            flake_id_generator_dict = {}

        ds_name_list = list(flake_id_generator_dict)
        ds_name_list.sort()
        old_name_list = self._df['Name'].tolist()
        old_name_list.sort()
        if ds_name_list == old_name_list:
            return

        # A workaround for a bug in Tabulator: If the table is initially empty 
        # and new rows are added later, Panel does not display table contents.
        # A workaround is to initialize it with a single empty row.
        len_list = len(ds_name_list)
        if len_list == 0:
            ds_name_list = ['']
            len_list = 1
        id_list = ['' for num in range(len_list)]
        # Preserve existing names
        for index, row in self._df.iterrows():
            for i in range(len_list):
                if row['Name'] == ds_name_list[i]:
                    id_list[i] = row['Id']
                    break

        self._df = pd.DataFrame({
            'Name': ds_name_list,
            'Id': id_list,
        })
        self._blotter.value = self._df
        self._sync_widgets()

    def __panel__(self):
        return self._layout

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._blotter
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._blotter

    def __blotter_on_click__(self, event):
        self._status_text.value = ''
        row = event.row
        column = event.column
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds_name = self._df.iloc[row]['Name']
            if ds_name == '':
                return
            if self.hazelcast_cluster != None:
                ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
            else:
                ds = None
            if column == 'New':
                try:
                    if ds != None:
                        new_id = HazelcastUtil.get_future_value(ds.new_id())
                        self._blotter.patch({
                            'Id' : [
                                (row, new_id)
                            ]})
                except Exception as ex:
                    self._status_text.value = repr(ex)
            elif column == 'Destroy':
                # Destroy
                if ds != None:
                    is_destroyed = ds.destroy()
                    if is_destroyed == True:
                        self.hazelcast_cluster.refresh()
                    self._status_text.value = f'Destroyed: {ds_name} - {is_destroyed}'

    def __new_execute__(self, event):
        ds_name = self._new_text.value.strip()
        if len(ds_name) == 0:
            return
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds = self.hazelcast_cluster.hazelcast_client.get_flake_id_generator(ds_name)
            self.hazelcast_cluster.refresh()
            # Select the new row
            for index, row in self._df.iterrows():
                if ds_name == row['Name']:
                    self._blotter.selection = [index]
                    break

    def select(self, ds_name):
        if ds_name == None:
            return
        row_count = len(self._df.index)
        for row in range(row_count):
            if ds_name == self._df.iloc[row]['Name']:
                self._blotter.selection = [row]
                break
