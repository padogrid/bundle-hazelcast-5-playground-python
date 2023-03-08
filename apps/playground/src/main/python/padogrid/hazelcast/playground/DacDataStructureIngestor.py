# %%
"""
DacDataStructureIngestor ingests mock data into Hazelcast data structures other than
Map and ReplicatedMap.
"""
import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer
from threading import Thread

from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.Progress import Progress
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil

class DacDataStructureIngestor(DacBase, Viewer):
    
    default_operation = param.String('add')
    default_key = param.String('random')
    default_key_range = param.Integer(100_000)
    default_object = param.String('Customer')
    default_er = param.String('N/A')
    default_delta = param.Integer(1)
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

        if "ds_type" in params:
            self.ds_type = params["ds_type"]
        else:
            self.ds_type = 'Unspecified'

        if self.ds_type == 'FlakeIdGenerator':
            operation_list = ['new_id']
            self.default_operation = 'new_id'
        elif self.ds_type == 'List':
            operation_list = ['add']
            self.default_operation = 'add'
        elif self.ds_type == 'MultiMap':
            operation_list = ['put']
            self.default_operation = 'put'
        elif self.ds_type == 'PNCounter':
            operation_list = ['get_and_add', 'add_and_get', 'get_and_subtract', 'subtract_and_get']
            self.default_operation = 'get_and_add'
        elif self.ds_type == 'ReliableTopic':
            operation_list = ['publish']
            self.default_operation = 'publish'
        elif self.ds_type == 'Queue':
            operation_list = ['offer']
            self.default_operation = 'offer'
        elif self.ds_type == 'Ringbuffer':
            operation_list = ['add']
            self.default_operation = 'add'
        elif self.ds_type == 'Set':
            operation_list = ['offer']
            self.default_operation = 'offer'
        elif self.ds_type == 'Topic':
            operation_list = ['publish']
            self.default_operation = 'publish'
        else:
            operation_list = ['add']
        blotter_name = self.ds_type
        key_list = [ 'random', 'uuid' ]

        self._status_text = pn.widgets.input.TextInput(disabled=True)
        self._new_button = pn.widgets.Button(name='New', width=self.button_width, button_type='primary')
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=300)
        self._new_text.param.watch(self.__new_execute__, 'value')

        if self.ds_type == 'flake_id_generator': 
            self._df = pd.DataFrame({
                'Name': [''],
                'Operation': [''],
                'Count': [-1],
                'Call Delay (msec)': [-1],
                'Ingest': [False]
            })
        elif self.ds_type == 'pn_counter':
            self._df = pd.DataFrame({
                'Name': [''],
                'Operation': [''],
                'Delta': [-1],
                'Count': [-1],
                'Call Delay (msec)': [-1],
                'Ingest': [False]
            })
        elif self.ds_type == 'multi_map':
            self._df = pd.DataFrame({
                'Name': [''],
                'Operation': [''],
                'Key': [''],
                'Key Range': [-1],
                'Object': [''],
                'ER': [''],
                'Count': [-1],
                'Call Delay (msec)': [-1],
                'Ingest': [False]
            })
        else:
            self._df = pd.DataFrame({
                'Name': [''],
                'Operation': [''],
                'Object': [''],
                'ER': [''],
                'Count': [-1],
                'Call Delay (msec)': [-1],
                'Ingest': [False]
            })
        tabulator_formatters = {
            'Ingest': {'type': 'tickCross'}
        }
        blotter_editors = {
            'Name': None,
            'Operation': {'type': 'list', 'values': operation_list},
            'Key': {'type': 'list', 'values': key_list},
            'Key Range': {'type': 'number', 'min': 100, 'max': 100_000_000, 'step': 100, 'set': self.default_key_range},
            'Object': {'type': 'list', 'values': self.get_object_name_list()},
            'ER': {'type': 'list', 'values': self.get_er_name_list()},
            'Delta': {'type': 'number', 'min': 1, 'max': 1000, 'step': 1, 'set': 1},
            'Count': {'type': 'number', 'min': 1, 'max': 100000, 'step': 10, 'set': 100},
            'Call Delay (msec)': {'type': 'number', 'min': 10, 'max': 100000, 'step': 10, 'set': 1000},
            'Ingest': {'type': 'tickCross'}
        }

        self._blotter = pn.widgets.Tabulator(self._df, name=blotter_name, editors=blotter_editors,
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
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.ds_type)
        else:
            ds_name_list = []
        ds_name_list.sort()
        old_df = self._df
        old_name_list = old_df['Name'].tolist()
        old_name_list.sort()
        if ds_name_list == old_name_list:
            return

        # A workaround for a bug in Tabulator: If the table is initially empty 
        # and new rows are added later, Panel does not display table contents.
        # A workaround is to initialize it with a single empty row.
        len_list = len(ds_name_list)
        if len_list == 0:
            name_list = ['']
            operation_list = ['']
            key_list = ['']
            key_range_list = ['']
            object_list = ['']
            er_list = ['N/A']
            delta_list = [-1]
            count_list = [-1]
            delay_list = [-1]
            ingest_list = [False]
        else:
            name_list = []
            operation_list = []
            key_list = []
            key_range_list = []
            object_list = []
            er_list = []
            delta_list = []
            count_list = []
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
                        if self.ds_type == 'PNCounter':
                            delta_list.append(old_df.iloc[index]['Delta'])
                        elif self.ds_type != 'FlakeIdGenerator':
                            if self.ds_type == 'MultiMap':
                                key_list.append(old_df.iloc[index]['Key'])
                                key_range_list.append(old_df.iloc[index]['Key Range'])
                            object_list.append(old_df.iloc[index]['Object'])
                            er_list.append(old_df.iloc[index]['ER'])
                        count_list.append(old_df.iloc[index]['Count'])
                        delay_list.append(old_df.iloc[index]['Call Delay (msec)'])
                        ingest_list.append(old_df.iloc[index]['Ingest'])
                        break
            else:
                operation_list.append(self.default_operation)
                if self.ds_type == 'PNCounter':
                    delta_list.append(self.default_delta)
                elif self.ds_type != 'FlakeIdGenerator':
                    if self.ds_type == 'MultiMap':
                        key_list.append(self.default_key)
                        key_range_list.append(self.default_key_range)
                    ds = self.__get_ds__(name)
                    object_type_in_ds = self.get_object_name_in_ds(ds)
                    if object_type_in_ds == None:
                        object_list.append(self.default_object)
                    else:
                        object_list.append(object_type_in_ds)
                    er_list.append(self.default_er)
                count_list.append(self.default_count)
                delay_list.append(self.default_delay_in_msec)
                ingest_list.append(False)

        # Stop and remove existing threads that are not part of the new map list.
        # This cleans up the existing thread events in case the maps are destroyed
        # by other clients.
        DacBase.thread_pool.refresh(self.ds_type, ds_name_list)

        if self.ds_type == 'FlakeIdGenerator':
            new_df = pd.DataFrame({
                'Name': name_list,
                'Operation': operation_list,
                'Count': count_list,
                'Call Delay (msec)': delay_list,
                'Ingest' : ingest_list,
            })    
        elif self.ds_type == 'PNCounter':
            new_df = pd.DataFrame({
                'Name': name_list,
                'Operation': operation_list,
                'Delta': delta_list,
                'Count': count_list,
                'Call Delay (msec)': delay_list,
                'Ingest' : ingest_list,
            })    
        elif self.ds_type == 'MultiMap':
            new_df = pd.DataFrame({
                'Name': name_list,
                'Operation': operation_list,
                'Key': key_list,
                'Key Range': key_range_list,
                'Object': object_list,
                'ER': er_list,
                'Count': count_list,
                'Call Delay (msec)': delay_list,
                'Ingest' : ingest_list,
            })    
        else:
            new_df = pd.DataFrame({
                'Name': name_list,
                'Operation': operation_list,
                'Object': object_list,
                'ER': er_list,
                'Count': count_list,
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
        self.value = self._df
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self._df

    def get_ds_from_hz(self, ds_name):
        '''Returns the specified data structure. The data structure is created if it does not exist.
        '''
        ds = None
        if self.hazelcast_cluster != None:
            if self.ds_type == 'FlakeIdGenerator':
                ds = self.hazelcast_cluster.hazelcast_client.get_flake_id_generator(ds_name)
            if self.ds_type == 'List':
                ds = self.hazelcast_cluster.hazelcast_client.get_list(ds_name)
            elif self.ds_type == 'MultiMap':
                ds = self.hazelcast_cluster.hazelcast_client.get_multi_map(ds_name)
            elif self.ds_type == 'PNCounter':
                ds = self.hazelcast_cluster.hazelcast_client.get_pn_counter(ds_name)
            elif self.ds_type == 'ReliableTopic':
                ds = self.hazelcast_cluster.hazelcast_client.get_reliable_topic(ds_name)
            elif self.ds_type == 'Ringbuffer':
                ds = self.hazelcast_cluster.hazelcast_client.get_ringbuffer(ds_name)
            elif self.ds_type == 'Set':
                ds = self.hazelcast_cluster.hazelcast_client.get_set(ds_name)
            elif self.ds_type == 'Queue':
                ds = self.hazelcast_cluster.hazelcast_client.get_queue(ds_name)
            elif self.ds_type == 'Topic':
                ds = self.hazelcast_cluster.hazelcast_client.get_topic(ds_name)
        return ds
    
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
                delta_list = [self.default_delta]
                count_list = [self.default_count]
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
                count_list = self._df['Count'].to_list()
                count_list.insert(index, self.default_count)
                delay_list = self._df['Call Delay (msec)'].to_list()
                delay_list.insert(index, self.default_delay_in_msec)
                ingest_list = self._df['Ingest'].to_list()
                ingest_list.insert(index, False)
                if self.ds_type == 'FlakeIdGenerator':
                    pass
                elif self.ds_type == 'PNCounter':
                    delta_list = self._df['Delta'].to_list()
                    delta_list.insert(index, self.default_delta)
                else:
                    if self.ds_type == 'MultiMap':
                        key_list = self._df['Key'].to_list()
                        key_list.insert(index, self.default_key)
                        key_range_list = self._df['Key Range'].to_list()
                        key_range_list.insert(index, self.default_key_range)
                    object_list = self._df['Object'].to_list()
                    object_list.insert(index, self.default_object)
                    er_list = self._df['ER'].to_list()
                    er_list.insert(index, self.default_er)

            if self.ds_type == 'FlakeIdGenerator':
                new_df = pd.DataFrame({
                    'Name': name_list,
                    'Operation': operation_list,
                    'Count': count_list,
                    'Call Delay (msec)': delay_list,
                    'Ingest' : ingest_list,
                })    
            elif self.ds_type == 'PNCounter':
                new_df = pd.DataFrame({
                    'Name': name_list,
                    'Operation': operation_list,
                    'Delta': delta_list,
                    'Count': count_list,
                    'Call Delay (msec)': delay_list,
                    'Ingest' : ingest_list,
                })    
            elif self.ds_type == 'MultiMap':
                new_df = pd.DataFrame({
                    'Name': name_list,
                    'Operation': operation_list,
                    'Key': key_list,
                    'Key Range': key_range_list,
                    'Object': object_list,
                    'ER': er_list,
                    'Count': count_list,
                    'Call Delay (msec)': delay_list,
                    'Ingest' : ingest_list,
                })    
            else:
                new_df = pd.DataFrame({
                    'Name': name_list,
                    'Operation': operation_list,
                    'Object': object_list,
                    'ER': er_list,
                    'Count': count_list,
                    'Call Delay (msec)': delay_list,
                    'Ingest' : ingest_list,
                })    
            self._df = new_df
            self._blotter.value = new_df

        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            ds = self.get_ds_from_hz(name)
            # Select the new row
            for index, row in self._df.iterrows():
                if name == row['Name']:
                    self._blotter.selection = [index]
                    break

    def __click_remove_button__(self, event):
        return

    def __get_ds__(self, ds_name):
        '''Returns the specified data structure from the cached list in HazelcastCluster.
        If not found, then returns None.
        '''
        if self.hazelcast_cluster != None:
            return self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            return None

    def __clear_status__(self):
        self._status_text.value = ''

    def __blotter_on_edit__(self, event):
        name = self._df.iloc[event.row]['Name']
        operation = event.column
        operation_value = event.value
        if name == '':
            ds = None
        else:
            ds = self.__get_ds__(name)
            # ds may be None if it is newly created
            if ds == None:
                ds = self.get_ds_from_hz(name)
        if ds != None:
            if operation == 'Ingest':
                if operation_value == True:
                    # Abort if max threads reached
                    if DacBase.thread_pool.is_max():
                        self._status_text.value = f'ERROR: Max number of jobs reached [{DacBase.thread_pool.get_max()}]. Command aborted.'
                        self._blotter.patch({
                            'Ingest' : [
                                (event.row, False)
                            ]})
                        return
                    
                    # Get a single object to check whether the selected object type
                    # is same as the object in the data structure
                    object_type_in_ds = self.get_object_name_in_ds(ds)

                    # Offload publishing task to a thread
                    ds_method_name = self._df.iloc[event.row]['Operation']
                    delta = None
                    key_type = None
                    key_range = None
                    object_type = None
                    er_object_name = None
                    if self.ds_type == 'PNCounter':
                        delta = self._df.iloc[event.row]['Delta'] 
                    elif self.ds_type != 'FlakeIdGenerator':
                        if self.ds_type == 'MultiMap':
                            key_type = self._df.iloc[event.row]['Key']
                            key_range = self._df.iloc[event.row]['Key Range']
                            if key_range < 100:
                                key_range = 100
                            elif key_range > 100_000_000:
                                key_range = 100_000_000
                        object_type = self._df.iloc[event.row]['Object']
                        if object_type_in_ds != None and object_type != object_type_in_ds:
                            self._status_text.value = f'ERROR: Invalid Object. Data structure has {object_type_in_ds} but your selection is {object_type}. Command aborted.'
                            self._blotter.patch({
                                'Ingest' : [
                                    (event.row, False)
                                ]})
                            return
                        er_object_name = self._df.iloc[event.row]['ER']
                        if er_object_name == 'N/A':
                            er_object_name = None
                    max = self._df.iloc[event.row]['Count']
                    batch_size = -1
                    delay_in_msec = self._df.iloc[event.row]['Call Delay (msec)']
                    # A bug in Tabulator. Min value not honored if number entered manually
                    if delay_in_msec < 10:
                        delay_in_msec = 10
                        self._blotter.patch({
                            'Call Delay (msec)' : [
                                (event.row, delay_in_msec)
                            ]}) 
                    thread, thread_event = DacBase.thread_pool.get_thread(self.ds_type, name, 
                                                                          self.__ingest_task__, 
                                                                          args=(name, key_type, key_range, object_type, er_object_name, delta, ds_method_name, max, batch_size, delay_in_msec))
                    if thread.is_alive() == False:
                        try:
                            thread.start()
                        except Exception as ex:
                            self._status_text.value = f'ERROR: {repr(ex)}'
                else:
                    # Stop thread and remove it from the thread pool
                    DacBase.thread_pool.stop_thread(self.ds_type, name)
                    if self._parent != None:
                        self._parent.remove_progress_by_name(self.ds_type, name)

    def __ingest_task__(self, ds_name, key_type, key_range, object_type, er_object_name, delta, ds_method_name, max, batch_size, delay_in_msec, thread_event):
        '''Thread task for ingesting data.
        '''
        # Ingest objects
        create_function = self.get_obj_creation_function(object_type)
        progress = Progress(self.ds_type, ds_name, key_type, key_range, object_type, er_object_name, delta, ds_method_name, max, batch_size, delay_in_msec)
        if self._parent != None:
            self._parent.add_progress(progress)

        try:
            if self.ds_type == 'FlakeIdGenerator':
                self.ingestor.ingest_flake_id_generator(ds_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'List':
                self.ingestor.ingest_list(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'PNCounter':
                self.ingestor.ingest_pn_counter(ds_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress, ds_method_name=ds_method_name, delta=delta)
            elif self.ds_type == 'MultiMap':
                self.ingestor.ingest_multi_map(ds_name, create_function, er_object_name, key_type=key_type, key_range=key_range, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'Queue':
                self.ingestor.ingest_queue(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'ReliableTopic':
                self.ingestor.ingest_reliable_topic(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'Ringbuffer':
                self.ingestor.ingest_ringbuffer(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'Set':
                self.ingestor.ingest_set(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
            elif self.ds_type == 'Topic':
                self.ingestor.ingest_topic(ds_name, create_function, er_object_name, count=max, delay_in_msec=delay_in_msec, thread_event=thread_event, progress=progress)
        except Exception as ex:
            self._status_text.value = f'ERROR: {repr(ex)}'

        # Deselect Ingest checkbox
        for index, row in self._df.iterrows():
            if row['Name'] == ds_name:
                self._blotter.patch({
                                'Ingest' : [
                                    (index, False)
                                ]})
                break
        DacBase.thread_pool.stop_thread(self.ds_type, ds_name)
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
            ds = self.__get_ds__(name)
            # ds may be None if it is # newly created
            if ds == None:
                ds = self.get_ds_from_hz(name)
            if column == 'Destroy':
                # Destroy
                is_destroyed = ds.destroy()
                if is_destroyed == True:
                    self.hazelcast_cluster.refresh()
                    DacBase.thread_pool.stop_thread(self.ds_type, name)
                self._status_text.value = f'Destroyed: {name} - {is_destroyed}'
