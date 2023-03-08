# %%
"""
DacDataStructureTable is a Panel custom component for displaying Hazelcast data
structures in a immutable Tabulator. The table contains three (3) columns:
Type, Name, and Size. It is grouped by Type and sorted by Type and Name.
"""

import time
import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer
from threading import Thread

from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil

class DacDataStructureTable(DacBase, Viewer):
    
    value = None
    
    def __init__(self, **params):
        super().__init__(**params)

        self._cluster_refresh_button = pn.widgets.Button(name='Refresh', width=50, button_type='primary')
        self._cluster_refresh_button.on_click(self.__click_refresh_button__)

        self._type_list = ['']
        self._name_list = ['']
        self._size_list = ['N/A']
        # (type.ds_name, ds) pairs
        self._ds_dict = {}
        self._df = pd.DataFrame({
            'Type': self._type_list,
            'Name': self._name_list,
            'Size': self._size_list
        })
        self._ds_table = pn.widgets.Tabulator(self._df, name='Data Structures', disabled=True)
        self._layout = pn.Column(self._cluster_refresh_button, self._ds_table)
        self.reset(self.hazelcast_cluster)

        # Start auto refresh thread
        self._refresh_thread = Thread(target=self.__refresh_task__, args=[])
        self._refresh_thread.start()

    def __refresh_task__(self, delay_in_msec=5000):
        delay = delay_in_msec / 1000
        while True:
            if self.hazelcast_cluster != None:
                self.hazelcast_cluster.refresh()
            time.sleep(delay)

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        # (type.ds_name, ds) pairs
        self._ds_dict = {}
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            self.hazelcast_cluster.add_component(self)
            try:
                if self.hazelcast_cluster.hazelcast_client != None:
                    for ds in self.hazelcast_cluster.hazelcast_client.get_distributed_objects():
                        if ds.name.startswith("_") == False:
                            obj_type = str(type(ds))
                            split = obj_type.split('.')
                            ds_type = split[-1].rstrip('\'>')
                            self._ds_dict[ds_type + "." + ds.name] = ds
            except:
                # In case connection loss
                self._ds_dict = {}
        self._ds_dict = dict(sorted(self._ds_dict.items()))

        #self._name_list = ['FlakeIdGenerator', 'List', 'Map', 'MultiMap', 'PNCounter', 'Queue', 'ReliableTopic', 'ReplicatedMap', 'Ringbuffer', 'Set', 'Topic']
        #count = len(self._name_list)
        #self._type_list = ['Ingestors' for num in range(count)]
        #self._size_list = ['N/A' for num in range(count)]
        
        if len(self._ds_dict) == 0:
            self._name_list = ['']
            self._type_list = ['']
            self._size_list = ['N/A']
        else:
            self._name_list = []
            self._type_list = []
            self._size_list = []

        for key, ds in self._ds_dict.items():
            obj_type = str(type(ds))
            split = obj_type.split('.')
            ds_type = split[-1].rstrip('\'>')
            self._type_list.append(ds_type)
            self._name_list.append(ds.name)
            ds_size = self.hazelcast_cluster.get_ds_size(ds_type, ds.name)
            if ds_size == None:
                self._size_list.append('N/A')
            else:
                self._size_list.append(ds_size)
            if ds_type == 'Map':
                self._type_list.append('MapKeySearch')
                self._name_list.append(ds.name)
                self._size_list.append(ds_size)
            elif ds_type == 'ReplicatedMap':
                self._type_list.append('ReplicatedMapKeySearch')
                self._name_list.append(ds.name)
                self._size_list.append(ds_size)
   
        self._df = pd.DataFrame({
            'Type': self._type_list,
            'Name': self._name_list,
            'Size': self._size_list
        })
        self._ds_table.value = self._df
        self._ds_table.groupby = ['Type']
        # self._ds_table = pn.widgets.Tabulator(self._df, name='Data Structures', groupby=['Type'], disabled=True, pagination='local', page_size=20)
        # self._layout = pn.Row(self._ds_table)
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._ds_table
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._ds_table
    
    def __click_refresh_button__(self, event):
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.refresh(is_reset=True)
        
    def on_click(self, callback=None):
        self._ds_table.on_click(callback)
        
    def get_type_name(self, row=0):
        # if row >= len(self._df.iloc):
        #     return None, None
        return self._df.iloc[row]['Type'], self._df.iloc[row]['Name']

    def get_data_structure(self, row=0):
        # if row >= len(self._df.iloc):
        #     return None
        ds_type = self._df.iloc[row]['Type']
        ds_name = self._df.iloc[row]['Name']
        return self._ds_dict[ds_type + "." + ds_name]
