# %%
"""
DacIngestor ingests mock data into Hazelcast.
"""

import time
import param
import panel as pn
import pandas as pd
from panel.viewable import Viewer
from threading import Thread

from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.DacMapIngestor import DacMapIngestor
from padogrid.hazelcast.playground.DacDataStructureIngestor import DacDataStructureIngestor

class DacIngestor(DacBase, Viewer):
    
    page_size = param.Integer(10)
    value = None
    
    def __init__(self, **params):
        super().__init__(**params)

        if 'ingestor_progress' in params:
            self.ingestor_progress = params['ingestor_progress']
            self._progress_dict = self.ingestor_progress._progress_dict
        else:
            self.ingestor_progress = None
            self._progress_dict = None
        flake_id_generator_ingestor = DacDataStructureIngestor(dac_id='FlakeIdGeneratorIngestor', ds_type='FlakeIdGenerator', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        list_ingestor = DacDataStructureIngestor(dac_id='ListIngestor', ds_type='List', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        map_ingestor = DacMapIngestor(dac_id='MapIngestor', map_type='Map', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        multi_map_ingestor = DacDataStructureIngestor(dac_id='MultiMapIngestor', ds_type='MultiMap', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        pn_counter_ingestor = DacDataStructureIngestor(dac_id='PNCounterIngestor', ds_type='PNCounter', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        queue_ingestor = DacDataStructureIngestor(dac_id='QueueIngestor', ds_type='Queue', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        reliable_topic_ingestor = DacDataStructureIngestor(dac_id='ReliableTopicIngestor', ds_type='ReliableTopic', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        replicated_map_ingestor = DacMapIngestor(dac_id='ReplicatedMapIngestor', map_type='ReplicatedMap', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        ringbuffer_ingestor = DacDataStructureIngestor(dac_id='RingbufferIngestor', ds_type='Ringbuffer', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        set_ingestor = DacDataStructureIngestor(dac_id='SetIngestor', ds_type='Set', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        topic_ingestor = DacDataStructureIngestor(dac_id='TopicIngestor', ds_type='Topic', hazelcast_cluster=self.hazelcast_cluster, parent=self)
        self._tabs = pn.Tabs(('FlakeIdGenerator', flake_id_generator_ingestor),
                             ('List', list_ingestor),
                             ('Map', map_ingestor), 
                             ('MultiMap', multi_map_ingestor), 
                             ('PNCounter', pn_counter_ingestor), 
                             ('Queue', queue_ingestor), 
                             ('ReliableTopic', reliable_topic_ingestor), 
                             ('ReplicatedMap', replicated_map_ingestor), 
                             ('Ringbuffer', ringbuffer_ingestor), 
                             ('Set', set_ingestor), 
                             ('Topic', topic_ingestor), 
                             dynamic=True)

        self._layout = pn.Column(self._tabs)

        # self.reset(self.hazelcast_cluster)

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        type_list = []
        name_list = []
        operation_list = []
        object_list = []
        delta_list = []
        count_list = []
        batch_size_list = []
        delay_list = []
        progress_list = []
        ingest_list = []

        # Remove non-existing data structures in the cluster
        old_df = self._df
        removal_dict =  {}
        for index, row in old_df.iterrows():
            ds_type = row['Type']
            ds_name = row['Name']
            ds = self.hazelcast_cluster.get_ds(ds_type, ds_name)
            if ds == None:
                if ds_type in removal_dict:
                    ds_name_list = removal_dict[ds_type]
                else:
                    ds_name_list = []
                    removal_dict[ds_type] = ds_name_list
                ds_name_list.append(ds_name)
            else:
                type_list.append(ds_type) 
                name_list.append(ds_name) 
                operation_list.append(old_df.iloc[index]['Operation'])
                object_list.append(old_df.iloc[index]['Object'])
                delta_list.append(old_df.iloc[index]['Delta'])
                count_list.append(old_df.iloc[index]['Count'])
                batch_size_list.append(old_df.iloc[index]['Batch Size'])
                delay_list.append(old_df.iloc[index]['Call Delay (msec)'])
                ingest_list.append(old_df.iloc[index]['Ingested'])
                progress_list.append(old_df.iloc[index]['Progress'])

        # Stop and remove existing threads that are not part of the new map list.
        # This cleans up the existing thread events in case the maps are destroyed
        # by other clients.
        for ds_type, ds_name_list in removal_dict.items():
            DacBase.thread_pool.refresh(ds_type, ds_name_list)

        new_df = pd.DataFrame({
                'Type': type_list,
                'Name': name_list,
                'Operation': operation_list,
                'Object': delta_list,
                'Delta': delta_list,
                'Count': count_list,
                'Batch Size': batch_size_list,
                'Call Delay (msec)': delay_list,
                'Ingested' : ingest_list,
                'Progress' : progress_list,
            })    

        self._df = new_df
        self._blotter.value = new_df
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._tabs
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._tabs

    def __blotter_on_click__(self, event):
        row = event.row
        column = event.column
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds_type = self._df.iloc[row]['Type']
            ds_name = self._df.iloc[row]['Name']
            if column == 'Destroy':
                # Remove
                progress = self.get_progress(ds_type, ds_name)
                if progress != None:
                    self.remove_progress(progress)
        
    def __get_progress_id_by_name__(self, ds_type, ds_name):
        return ds_type + ":" + ds_name

    def get_progress(self, ds_type, ds_name):
        '''Returns the specified progress. It returns None if not found.'''
        progress_id = self.__get_progress_id_by_name__(ds_type, ds_name)
        if progress_id in self._progress_dict:
            progress = self._progress_dict[progress_id]
        else:
            progress = None
        return progress

    def get_progress_id(self, progress):
        return self.__get_progress_id_by_name__(progress.ds_type, progress.ds_name)

    def add_progress(self, progress):
        if self.ingestor_progress != None:
            self.ingestor_progress.add_progress(progress)

    def remove_progress(self, progress):
        if self.ingestor_progress != None:
            self.ingestor_progress.remove_progress(progress)

    def remove_progress_by_name(self, ds_type, ds_name):
        if self.ingestor_progress != None:
            self.ingestor_progress.remove_progress_by_name(ds_type, ds_name)

    def get_tab_index(self, tab_name):
        '''Returns the tab index of the specified tab name. It returns -1 if not found.

        Args:
            tab_name: Tab name.
        '''
        index = -1
        i = 0
        for name in self._tabs._names:
            if tab_name == name:
                index = i
                break
            i += 1
        return index

    def select(self, tab_name):
        '''Selects the specified tab.
        
        Args:
            tab_name: Tab name.
        '''
        index = self.get_tab_index(tab_name)
        if index != -1:
            self._tabs.active = index
