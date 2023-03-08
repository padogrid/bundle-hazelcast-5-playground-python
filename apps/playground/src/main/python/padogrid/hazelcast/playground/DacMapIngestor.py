# %%
"""
DacMapIngestor ingests mock data into Hazelcast maps.
"""
import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer
from threading import Thread

from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.Progress import Progress
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil

class DacMapIngestor(DacBase, Viewer):
    
    default_operation = param.String('put')
    default_key = param.String('random')
    default_key_range = param.Integer(100_000)
    default_object = param.String('Customer')
    default_er = param.String('N/A')
    default_count = param.Integer(1000)
    default_batch_size = param.Integer(100)
    default_delay_in_msec = param.Integer(1000)

    value = None
    
    def __init__(self, **params):
        super().__init__(**params)

        if "parent" in params:
            self._parent = params["parent"]
        else:
            self._parent = None

        if "map_type" in params:
            self.map_type = params["map_type"]
        else:
            self.map_type = 'Map'

        if self.map_type == 'ReplicatedMap':
            blotter_name = 'ReplicatedMapIngestor'
            operation_list = ['put', 'put_all']
        else:
            blotter_name = 'MapIngestor'
            operation_list = ['put', 'put_all', 'set']
        key_list = ['random', 'uuid']
    
        self._status_text = pn.widgets.input.TextInput(disabled=True)
        self._new_button = pn.widgets.Button(name='New', width=self.button_width, button_type='primary')
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=300)
        self._new_text.param.watch(self.__new_execute__, 'value')

        self._type_list = []
        self._name_list = []
        self._size_list = []
        self._df = pd.DataFrame({
            'Name': [],
            'Operation': [],
            'Key': [],
            'Key Range': [],
            'Object': [],
            'ER': [],
            'Ingest': [],
            'Count': [],
            'Batch Size': [],
            'Call Delay (msec)': []
        })
        tabulator_formatters = {
            'Ingest': {'type': 'tickCross'}
        }
        map_table_editors = {
            'Name': None,
            'Operation': {'type': 'list', 'values': operation_list},
            'Key': {'type': 'list', 'values': key_list},
            'Key Range': {'type': 'number', 'min': 100, 'max': 100_000_000, 'step': 100, 'set': self.default_key_range},
            'Object': {'type': 'list', 'values': self.get_object_name_list()},
            'ER': {'type': 'list', 'values': self.get_er_name_list()},
            'Count': {'type': 'number', 'min': 1, 'max': 100000, 'step': 10, 'set': 100},
            'Batch Size': {'type': 'number', 'max': 10000, 'step': 10, 'set': 100},
            'Call Delay (msec)': {'type': 'number', 'min': 10, 'max': 100000, 'step': 10, 'set': 1000},
            'Ingest': {'type': 'tickCross'}
        }

        self._blotter = pn.widgets.Tabulator(self._df, name=blotter_name,
                                             editors=map_table_editors, 
                                             layout='fit_columns',
                                             buttons={'Destroy': "<i class='fa fa-trash'></i>"},
                                             formatters=tabulator_formatters)
        
        self._blotter.on_edit(self.__blotter_on_edit__)
        self._blotter.on_click(self.__blotter_on_click__)
        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                 pn.Row(self._new_button, self._new_text),
                                 self._blotter)
        self._sync_widgets()

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.map_type)
        else:
            ds_name_list = []
        ds_name_list.sort()

         # Return if reset is False, i.e., execute map
        old_df = self._df
        old_name_list = old_df['Name'].tolist()
        old_name_list.sort()
        if is_reset == False:
            if ds_name_list == old_name_list:
                return

        # A workaround for a bug in Tabulator: If the table is initially empty 
        # and new rows are added later, Panel does not display table contents.
        # A workaround is to initialize it with a single empty row.
        if len(ds_name_list) == 0:
            name_list = ['']
            operation_list = ['']
            key_list = ['']
            key_range_list = [-1]
            object_list = ['']
            er_list = ['']
            count_list = [-1]
            batch_size_list = [-1]
            delay_list = [-1]
            ingest_list = [False]
        else:
            name_list = []
            operation_list = []
            key_list = []
            key_range_list = []
            object_list = []
            er_list = []
            count_list = []
            batch_size_list = []
            delay_list = []
            ingest_list = []

        # Retain the previously running tasks
        for name in ds_name_list:
            name_list.append(name)
            if name in old_name_list:
                for index, row in old_df.iterrows():
                    old_name = row['Name']
                    if old_name == name:
                        operation_list.append(old_df.iloc[index]['Operation'])
                        key_list.append(old_df.iloc[index]['Key'])
                        key_range_list.append(old_df.iloc[index]['Key Range'])
                        object_list.append(old_df.iloc[index]['Object'])
                        er_list.append(old_df.iloc[index]['ER'])
                        ingest_list.append(old_df.iloc[index]['Ingest'])
                        count_list.append(old_df.iloc[index]['Count'])
                        batch_size_list.append(old_df.iloc[index]['Batch Size'])
                        delay_list.append(old_df.iloc[index]['Call Delay (msec)'])
                        break
            else:
                ds = self.__get_map__(name)
                operation_list.append(self.default_operation)
                key_list.append(self.default_key)
                key_range_list.append(self.default_key_range)
                object_type_in_ds = self.get_object_name_in_ds(ds)
                if object_type_in_ds == None:
                    object_type_in_ds = self.default_object
                object_list.append(object_type_in_ds)
                er_list.append(self.default_er)
                ingest_list.append(False)
                count_list.append(self.default_count)
                batch_size_list.append(self.default_batch_size)
                delay_list.append(self.default_delay_in_msec)

        # Stop and remove existing threads that are not part of the new map list.
        # This cleans up the existing thread events in case the maps are destroyed
        # by other clients.
        DacBase.thread_pool.refresh(self.map_type, ds_name_list)
        new_df = pd.DataFrame({
            'Name': name_list,
            'Operation': operation_list,
            'Key': key_list,
            'Key Range': key_range_list,
            'Object': object_list,
            'ER': er_list,
            'Count': count_list,
            'Batch Size': batch_size_list,
            'Call Delay (msec)': delay_list,
            'Ingest' : ingest_list,
        })    
        self._df = new_df
        self._blotter.value = new_df
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self._blotter
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._blotter
    
    def get_map_from_hz(self, map_name):
        '''Returns the specified map. The map is created if it does not exist.
        '''
        if self.map_type == 'ReplicatedMap':
            map = self.hazelcast_cluster.hazelcast_client.get_replicated_map(map_name)
        else:
            map = self.hazelcast_cluster.hazelcast_client.get_map(map_name)
        return map
    
    def __new_execute__(self, event):
        self.__clear_status__()
        name = self._new_text.value.strip()
        if len(name) == 0:
            return
        found = False
        for index, row in self._df.iterrows():
            if name == row['Name']:
                found = True
                break
        # Update df if a new name
        if found == False:
            name_list = self._df['Name'].to_list()
            # Replace the place holder (empty) row  used as a workaround to a Panel bug
            is_place_holder = len(name_list) == 1 and name_list[0] == ''
            if is_place_holder:
                name_list = [name]
                operation_list = [self.default_operation]
                key_list = [self.default_key]
                key_range_list = [self.default_key_range]
                object_list = [self.default_object]
                er_list = [self.default_er]
                count_list = [self.default_count]
                batch_size_list = [self.default_batch_size]
                delay_list = [self.default_delay_in_msec]
                ingest_list = [False]
            else:
                name_list.append(name)
                name_list.sort()
                index = 0
                for n in name_list:
                    if n == name:
                        break
                    index += 1
                operation_list = self._df['Operation'].to_list()
                operation_list.insert(index, self.default_operation)
                key_list = self._df['Key'].to_list()
                key_list.insert(index, self.default_key)
                key_range_list = self._df['Key Range'].to_list()
                key_range_list.insert(index, self.default_key_range)
                object_list = self._df['Object'].to_list()
                object_list.insert(index, self.default_object)
                er_list = self._df['ER'].to_list()
                er_list.insert(index, self.default_er)
                count_list = self._df['Count'].to_list()
                count_list.insert(index, self.default_count)
                batch_size_list = self._df['Batch Size'].to_list()
                batch_size_list.insert(index, self.default_batch_size)
                delay_list = self._df['Call Delay (msec)'].to_list()
                delay_list.insert(index, self.default_delay_in_msec)
                ingest_list = self._df['Ingest'].to_list()
                ingest_list.insert(index, False)

            new_df = pd.DataFrame({
                'Name': name_list,
                'Operation': operation_list,
                'Key': key_list,
                'Key Range': key_range_list,
                'Object': object_list,
                'ER': er_list,
                'Count': count_list,
                'Batch Size': batch_size_list,
                'Call Delay (msec)': delay_list,
                'Ingest' : ingest_list,
            })   
            self._df = new_df
            self._blotter.value = new_df

        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            map = self.get_map_from_hz(name)
            # Select the new row
            for index, row in self._df.iterrows():
                if name == row['Name']:
                    self._blotter.selection = [index]
                    break

    def __click_remove_button__(self, event):
        return

    def __get_map__(self, map_name):
        '''Returns the specified map from the cached list in HazelcastCluster.
        If not found, then returns None.'''
        map = None
        if self.hazelcast_cluster != None:
            map = self.hazelcast_cluster.get_ds(self.map_type, map_name)
        return map

    def __clear_status__(self):
        self._status_text.value = ''

    def __blotter_on_edit__(self, event):
        name = self._df.iloc[event.row]['Name']
        operation = event.column
        operation_value = event.value
        map = self.__get_map__(name)
        if name == '':
            map = None
        else:
            # map may be None if it is newly created
            if map == None:
                map = self.get_map_from_hz(name)
        if map != None:
            if operation == 'Ingest':
                if name == '':
                    self._blotter.patch({
                        'Ingest' : [
                            (event.row, False)
                        ]})
                    return
                if operation_value == True:
                    # Abort if max threads reached
                    if DacBase.thread_pool.is_max():
                        self._status_text.value = f'ERROR: Max number of jobs reached [{DacBase.thread_pool.get_max()}]. Command aborted.'
                        self._blotter.patch({
                            'Ingest' : [
                                (event.row, False)
                            ]})
                        return

                    key_type = self._df.iloc[event.row]['Key']
                    key_range = self._df.iloc[event.row]['Key Range']
                    if key_range < 100:
                        key_range = 100
                    elif key_range > 100_000_000:
                        key_range = 100_000_000
                    
                    # Get a single object to check whether the selected object type
                    # is same as the object in the data structure
                    object_type_in_ds = self.get_object_name_in_ds(map)
                    
                    object_type = self._df.iloc[event.row]['Object']
                    if object_type_in_ds != None and object_type != object_type_in_ds:
                        self._status_text.value = f'ERROR: Invalid Object. Data structure has {object_type_in_ds} but your selection is {object_type}. Command aborted.'
                        self._blotter.patch({
                            'Ingest' : [
                                (event.row, False)
                            ]})
                        return

                    # Offload publishing task to a thread
                    map_method_name = self._df.iloc[event.row]['Operation']
                    er_object_name = self._df.iloc[event.row]['ER']
                    if er_object_name == 'N/A':
                        er_object_name = None

                    delta = -1
                    max = self._df.iloc[event.row]['Count']
                    batch_size = self._df.iloc[event.row]['Batch Size']
                    delay_in_msec = self._df.iloc[event.row]['Call Delay (msec)']
                    # A bug in Tabulator. Min value not honored if number entered manually
                    if delay_in_msec < 10:
                        delay_in_msec = 10
                        self._blotter.patch({
                            'Call Delay (msec)' : [
                                (event.row, delay_in_msec)
                            ]}) 
                    thread, thread_event = DacBase.thread_pool.get_thread(self.map_type, name, 
                                                                          self.__ingest_task__, 
                                                                          args=(name, key_type, key_range, object_type, er_object_name, delta, map_method_name, max, batch_size, delay_in_msec))
                    if thread.is_alive() == False:
                        try: 
                            thread.start()
                        except Exception as ex:
                            self._status_text.value = f'ERROR: {repr(ex)}'
                else:
                    # Stop thread and remove it from the thread pool
                    DacBase.thread_pool.stop_thread(self.map_type, name)
                    if self._parent != None:
                        self._parent.remove_progress_by_name(self.map_type, name)

    def __ingest_task__(self, map_name, key_type, key_range, object_type, er_object_name, delta, map_method_name, max, batch_size, delay_in_msec, thread_event):
        '''Thread task for ingesting data.
        '''
        # Ingest objects
        create_function = self.get_obj_creation_function(object_type)
        progress = Progress(self.map_type, map_name, key_type, key_range, object_type, er_object_name, delta, map_method_name, max, batch_size, delay_in_msec)
        if self._parent != None:
            self._parent.add_progress(progress)

        try:
            if self.map_type == 'ReplicatedMap':
                if map_method_name == "put_all":
                    self.ingestor.ingest_replicated_map_put_all(map_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, batch_size=batch_size, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
                else:
                    # replicated_map does not support set()
                    self.ingestor.ingest_replicated_map_put(map_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            else:
                if map_method_name == "put_all":
                    self.ingestor.ingest_map_put_all(map_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, batch_size=batch_size, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
                elif map_method_name == "set":
                    self.ingestor.ingest_map_set(map_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
                else:
                    self.ingestor.ingest_map_put(map_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
        except Exception as ex:
            self._status_text.value = f'ERROR: {repr(ex)}'
                
        # Deselect Ingest checkbox
        for index, row in self._df.iterrows():
            if row['Name'] == map_name:
                self._blotter.patch({
                                'Ingest' : [
                                    (index, False)
                                ]})
                break
        DacBase.thread_pool.stop_thread(self.map_type, map_name)
        if self._parent != None:
            self._parent.remove_progress(progress)

    def __blotter_on_click__(self, event):
        self.__clear_status__()
        row = event.row
        column = event.column
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            name = self._df.iloc[row]['Name']
            if name == '':
                return
            map = self.__get_map__(name)
            # map may be None if it is newly created
            if map == None:
                map = self.get_map_from_hz(name)

            if column == 'Destroy':
                # Destroy
                is_destroyed = map.destroy()
                if is_destroyed == True:
                    self.hazelcast_cluster.refresh()
                    DacBase.thread_pool.stop_thread(self.map_type, name)
                self._status_text.value = f'Destroyed: {name} - {is_destroyed}'
