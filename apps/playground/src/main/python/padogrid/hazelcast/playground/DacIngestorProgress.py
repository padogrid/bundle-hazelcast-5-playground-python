# %%
"""
DacIngestorProgress displays ingestion job status
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

class DacIngestorProgress(DacBase, Viewer):
    
    page_size = param.Integer(10)
    value = None
    
    def __init__(self, **params):
        super().__init__(**params)

        self._df = pd.DataFrame({
                'Type': [''],
                'Name': [''],
                'Operation': [''],
                'Key': [''],
                'Key Range': [-1],
                'Object': [''],
                'ER': [''],
                'Delta': [-1],
                'Count': [-1],
                'Batch Size': [-1],
                'Call Delay (msec)': [-1],
                'Ingested': [-1],
                'Progress': [-1],
            })
        tabulator_formatters = {
            'Ingest': {'type': 'tickCross'},
            'Progress': {'type': 'progress', 'min': 0, 'max': 1}
        }
        self._blotter = pn.widgets.Tabulator(self._df, name='Progress',
                                             disabled=True,
                                             layout='fit_columns',
                                             buttons={'Destroy': "<i class='fa fa-trash'></i>"},
                                             widths={'Progress': 150},
                                             formatters=tabulator_formatters)
        self._blotter.on_click(self.__blotter_on_click__)

        self._ingestion_jobs_text = pn.widgets.input.StaticText(name='Ingestion Jobs', value='0')

        self._progress_dict = {}
        self._progress_thread = Thread(target=self.__update_progress_loop__, args=[])
        self._progress_thread.start()

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._ingestion_jobs_text, self._blotter)

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        type_list = []
        name_list = []
        operation_list = []
        key_list = []
        key_range_list = []
        object_list = []
        er_list = []
        delta_list = []
        count_list = []
        batch_size_list = []
        delay_list = []
        progress_list = []
        ingested_list = []

        # Remove nonexistent structures in the cluster
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
                key_list.append(old_df.iloc[index]['Key'])
                key_range_list.append(old_df.iloc[index]['Key Range'])
                object_list.append(old_df.iloc[index]['Object'])
                er_list.append(old_df.iloc[index]['ER'])
                delta_list.append(old_df.iloc[index]['Delta'])
                count_list.append(old_df.iloc[index]['Count'])
                batch_size_list.append(old_df.iloc[index]['Batch Size'])
                delay_list.append(old_df.iloc[index]['Call Delay (msec)'])
                ingested_list.append(old_df.iloc[index]['Ingested'])
                progress_list.append(old_df.iloc[index]['Progress'])

        # Stop and remove existing threads that are not part of the new map list.
        # This cleans up the existing thread events in case the maps are destroyed
        # by other clients or there is a connection loss.
        for ds_type, ds_name_list in removal_dict.items():
            DacBase.thread_pool.refresh(ds_type, ds_name_list)

        # A workaround to a Tabulator bug
        if len(type_list) == 0:
            type_list = ['']
            name_list = ['']
            operation_list = ['']
            key_list = ['']
            key_range_list = [-1]
            object_list = ['']
            er_list = ['']
            delta_list = [-1]
            count_list = [-1]
            batch_size_list = [-1]
            delay_list = [-1]
            progress_list = [-1]
            ingested_list = [-1]
            self.__stop_all_progress__()

        new_df = pd.DataFrame({
                'Type': type_list,
                'Name': name_list,
                'Operation': operation_list,
                'Key': key_list,
                'Key Range': key_range_list,
                'Object': object_list,
                'ER': er_list,
                'Delta': delta_list,
                'Count': count_list,
                'Batch Size': batch_size_list,
                'Call Delay (msec)': delay_list,
                'Ingested' : ingested_list,
                'Progress' : progress_list,
            })    

        self._df = new_df
        self._blotter.value = new_df
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._df
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._df

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
        
    def __stop_all_progress__(self):
        DacBase.thread_pool.stop_all()
        self._progress_dict.clear()

    def __update_progress_loop__(self, delay_in_msec=1000):
        '''Periodically updates the Progress column.'''
        delay = delay_in_msec / 1000
        while True:
            time.sleep(delay)
            if len(self._progress_dict) > 0:
                # Copy progress_dict to prevent race conditions
                progress_dict = self._progress_dict.copy()
                progress_id_list = list(progress_dict)
                for progress_id in progress_id_list:
                    progress = progress_dict[progress_id]
                    # Remove progress from the dictionary if it is done
                    if self.__update_progress__(progress) or progress.count >= progress.max:
                        if progress_id in self._progress_dict:
                            del self._progress_dict[progress_id]
                            self.remove_progress(progress)
            job_count_str = str(len(self._progress_dict))
            if job_count_str != self._ingestion_jobs_text.value: 
                self._ingestion_jobs_text.value = str(job_count_str)

    def __update_progress__(self, progress):
        '''Updates the specified progress in the blotter. Returns True if the progress is to be removed.'''
        is_remove = False
        for index, row in self._df.iterrows():
            if row['Type'] == progress.ds_type and row['Name'] == progress.ds_name:
                self._blotter.patch({
                                'Progress' : [(index, progress.count/progress.max)],
                                'Ingested' : [(index, progress.count)],
                                })
                if progress.count >= progress.max:
                    is_remove = True
                break
        return is_remove

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
        progress_id = self.get_progress_id(progress)
        self._progress_dict[progress_id] = progress

        old_df = self._df
        found = False
        for index, row in old_df.iterrows():
            ds_type = row['Type']
            ds_name = row['Name']
            if ds_type == '':
                found = True
                break
            if ds_type == progress.ds_type and ds_name == progress.ds_name:
                found = True
                break

        # Set non-applicable column values
        key_type = 'N/A'
        key_range = -1
        if progress.ds_type == 'FlakeIdGenerator':
            object_type = 'N/A'
            er_object_name = 'N/A'
            delta = -1
        elif progress.ds_type == 'PNCounter':
            object_type = 'N/A'
            er_object_name = 'N/A'
            delta = progress.delta
        else:
            if progress.ds_type == 'Map' or progress.ds_type == 'MultiMap' or progress.ds_type == 'ReplicatedMap':
                key_type = progress.key_type
                key_range = progress.key_range
            object_type = progress.object_type
            er_object_name = progress.er_object_name
            if er_object_name == None:
                er_object_name = 'N/A'
            delta = -1

        if found:
            # patch
            self._blotter.patch({
                            'Type' : [(index, progress.ds_type)],
                            'Name' : [(index, progress.ds_name)],
                            'Operation' : [(index, progress.operation)],
                            'Key' : [(index, key_type)],
                            'Key Range' : [(index, key_range)],
                            'Object' : [(index, object_type)],
                            'ER' : [(index, er_object_name)],
                            'Delta' : [(index, delta)],
                            'Count' : [(index, progress.max)],
                            'Batch Size' : [(index, progress.batch_size)],
                            'Call Delay (msec)' : [(index, progress.delay_in_msec)],
                            'Ingested' : [(index, progress.count)],
                            'Progress' : [(index, progress.count/progress.max)],
                            })

        else:
            # If a new progress, then append to the existing column lists
            type_list = old_df['Type'].to_list()
            type_list.append(progress.ds_type)
            name_list = old_df['Name'].to_list()
            name_list.append(progress.ds_name)
            operation_list = old_df['Operation'].to_list()
            operation_list.append(progress.operation)
            key_list = old_df['Key'].to_list()
            key_list.append(key_type)
            key_range_list = old_df['Key Range'].to_list()
            key_range_list.append(key_range)
            object_list = old_df['Object'].to_list()
            object_list.append(object_type)
            er_list = old_df['ER'].to_list()
            er_list.append(er_object_name)
            delta_list = old_df['Delta'].to_list()
            delta_list.append(delta)
            count_list = old_df['Count'].to_list()
            count_list.append(progress.max)
            batch_size_list = old_df['Batch Size'].to_list()
            batch_size_list.append(progress.batch_size)
            delay_list = old_df['Call Delay (msec)'].to_list()
            delay_list.append(progress.delay_in_msec)
            progress_list = old_df['Progress'].to_list()
            progress_list.append(progress.count)
            ingested_list = old_df['Ingested'].to_list()
            ingested_list.append(True)

            new_df = pd.DataFrame({
                    'Type': type_list,
                    'Name': name_list,
                    'Operation': operation_list,
                    'Key': key_list,
                    'Key Range': key_range_list,
                    'Object': object_list,
                    'ER': er_list,
                    'Delta': delta_list,
                    'Count': count_list,
                    'Batch Size': batch_size_list,
                    'Call Delay (msec)': delay_list,
                    'Ingested' : ingested_list,
                    'Progress' : progress_list,
                })    
            self._df = new_df
            self._blotter.value = new_df
            index = len(type_list) - 1
            self._blotter.selection = [index]
            self._sync_widgets()

    def remove_progress(self, progress):
        old_df = self._df
        found = False
        for index, row in old_df.iterrows():
            ds_type = row['Type']
            ds_name = row['Name']
            if ds_type == '':
                found = True
                break
            if ds_type == progress.ds_type and ds_name == progress.ds_name:
                found = True
                break
        if found:
            type_list = old_df['Type'].to_list()
            del type_list[index]
            name_list = old_df['Name'].to_list()
            del name_list[index]
            operation_list = old_df['Operation'].to_list()
            del operation_list[index]
            key_list = old_df['Key'].to_list()
            del key_list[index]
            key_range_list = old_df['Key Range'].to_list()
            del key_range_list[index]
            object_list = old_df['Object'].to_list()
            del object_list[index]
            er_list = old_df['ER'].to_list()
            del er_list[index]
            delta_list = old_df['Delta'].to_list()
            del delta_list[index]
            count_list = old_df['Count'].to_list()
            del count_list[index]
            batch_size_list = old_df['Count'].to_list()
            del batch_size_list[index]
            delay_list = old_df['Call Delay (msec)'].to_list()
            del delay_list[index]
            ingested_list = old_df['Ingested'].to_list()
            del ingested_list[index]
            progress_list = old_df['Progress'].to_list()
            del progress_list[index]

            new_df = pd.DataFrame({
                    'Type': type_list,
                    'Name': name_list,
                    'Operation': operation_list,
                    'Key': key_list,
                    'Key Range': key_range_list,
                    'Object': object_list,
                    'ER': er_list,
                    'Delta': delta_list,
                    'Count': count_list,
                    'Batch Size': batch_size_list,
                    'Call Delay (msec)': delay_list,
                    'Ingested' : ingested_list,
                    'Progress' : progress_list,
                })    
            self._df = new_df
            self._blotter.value = new_df
            DacBase.thread_pool.stop_thread(progress.ds_type, progress.ds_name)
            progress_id = self.get_progress_id(progress)
            if progress_id in self._progress_dict:
                del self._progress_dict[progress_id]
            job_count = len(self._progress_dict)
            self._ingestion_jobs_text.value = str(job_count)
            self._sync_widgets()

    def remove_progress_by_name(self, ds_type, ds_name):
        progress = self.get_progress(ds_type, ds_name)
        if progress != None:
            self.remove_progress(progress)

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
