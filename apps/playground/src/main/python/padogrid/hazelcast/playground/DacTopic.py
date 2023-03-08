# %%
"""
DacTopic displays Hazelcat Topics.
"""

import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil

pn.extension('terminal')

class DacTopic(DacBase, Viewer):

    default_object = param.String('Customer')
    default_publish_max = param.Integer(100)
    default_publish_delay_in_msec = param.Integer(1000)
    width = param.Integer(1000)
    page_size = param.Integer(10)
    value = None
    
    def __init__(self, **params):
        super().__init__(**params)

        if "topic_type" in params:
            self.topic_type = params["topic_type"]
        else:
            self.topic_type = 'Topic'

        if self.topic_type == 'ReliableTopic':
            topic_name = 'ReliableTopic'
        else:
            topic_name = 'Topic'

        self._reliable_topic_listener = ReliableTopicListener(self)

        # Active topic listeners
        self._listener_dict = {}

        self._status_text = pn.widgets.input.TextInput(disabled=True)
        self._new_button = pn.widgets.Button(name='New', width=self.button_width, button_type='primary')
        self._new_button.on_click(self.__new_execute__)
        self._new_text = pn.widgets.TextInput(width=300)
        self._new_text.param.watch(self.__new_execute__, 'value')
        
        self._df = pd.DataFrame({
            'Topic': [''],
            'Subscribe': [False],
        })
        tabulator_formatters = {
            'Subscribe': {'type': 'tickCross'},
        }
        table_editors = {
            'Topic': None,
            'Subscribe': {'type': 'tickCross'},
        } 
        self._blotter = pn.widgets.Tabulator(self._df, width=self.width, height=370,
                                             pagination='local', 
                                             page_size=self.page_size, sizing_mode='stretch_width',
                                             editors=table_editors, formatters=tabulator_formatters)
        self._blotter.on_edit(self.__blotter_on_edit__)
        
        self._terminal = pn.widgets.Terminal(
            "Hazelcast Playground\n",
            options={"cursorBlink": True},
            height=400, sizing_mode='stretch_width'
        )

        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(self._status_text,
                                pn.Row(self._new_button, self._new_text),
                                self._blotter,
                                self._terminal)
        # self._terminal.subprocess.run("bash", "--init-file", ".bash_init")
        self._sync_widgets()

    def refresh(self, is_reset=False):
        '''Refreshes this component with the latest data from Hazelcast.
        '''
        if self.hazelcast_cluster != None:
            ds_name_list = self.hazelcast_cluster.get_ds_names(self.topic_type)
        else:
            ds_name_list = []
        ds_name_list.sort()
        old_df = self._df
        old_name_list = old_df['Topic'].tolist()
        old_name_list.sort()
        if ds_name_list == old_name_list:
            return

        # Add an empty row as a workaround to a Tabulator bug
        if len(ds_name_list) == 0:
            topic_list = ['']
            subscribe_list = [False]
        else:
            topic_list = []
            subscribe_list = []

            # Retain the previously running tasks
            for name in ds_name_list:
                topic_list.append(name)
                if name in old_name_list:
                    for index, row in old_df.iterrows():
                        old_name = row['Topic']
                        if old_name == name:
                            subscribe_list.append(old_df.iloc[index]['Subscribe'])
                            break
                else:
                    subscribe_list.append(False)

        # Stop and remove existing threads that are not part of the new topic list.
        # This cleans up the existing thread events in case the topics are destroyed
        # by other clients.
        DacBase.thread_pool.refresh(self.topic_type, ds_name_list)
        for name in DacBase.thread_pool.get_ds_names(self.topic_type):
            if name in ds_name_list == False:
                if name in self._listener_dict:
                    del self._listener_dict[name]

        new_df = pd.DataFrame({
            'Topic': topic_list,
            'Subscribe': subscribe_list,
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
        self.value =  self._df

    def get_topic_from_hz(self, topic_name):
        '''Returns the specified topic. The topic is created if it does not exist.
        '''
        if self.topic_type == 'ReliableTopic':
            topic = self.hazelcast_cluster.hazelcast_client.get_reliable_topic(topic_name)
        else:
            topic = self.hazelcast_cluster.hazelcast_client.get_topic(topic_name)
        return topic

    def __clear_status__(self):
        self._status_text.value = ''

    def __new_execute__(self, event):
        name = self._new_text.value.strip()
        if len(name) == 0:
            return
        if self.hazelcast_cluster != None and self.hazelcast_cluster.hazelcast_client != None:
            topic = self.get_topic_from_hz(name)
            self.hazelcast_cluster.refresh()
            # Select the new row and make it visible
            for index, row in self._df.iterrows():
                if name == row['Topic']:
                    self._blotter.selection = [index]
                    page = int(index / self._blotter.page_size) + 1
                    self._blotter.page = page
                    break
    
    def select(self, ds_name):
        self.__clear_status__()
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.ds_type, ds_name)
        else:
            ds = None
        self.execute_topic(ds)
        row_count = len(self._df.index)
        for row in range(row_count):
            if ds_name == self._df.iloc[row]['Topic']:
                self._blotter.selection = [row]
                break

    def __blotter_on_edit__(self, event):
        ds_name = self._df.iloc[event.row]['Topic']
        operation = event.column
        operation_value = event.value
        if self.hazelcast_cluster != None:
            ds = self.hazelcast_cluster.get_ds(self.topic_type, ds_name)
        else:
            ds = None
        if ds != None:
            if operation == 'Subscribe':
                if ds_name in self._listener_dict:
                    id = self._listener_dict[ds_name]
                else:
                    id = None
                if operation_value == True:
                    if id == None:
                        if self.topic_type == 'ReliableTopic':
                            future = ds.add_listener(self._reliable_topic_listener)
                            # future = topic.add_listener(self.__topic_listener__)
                        else:
                            future = ds.add_listener(self.__topic_listener__)
                        self._listener_dict[ds_name] = HazelcastUtil.get_future_value(future)
                else:
                    if id != None:
                        ds.remove_listener(id)
                        del self._listener_dict[ds_name]

    def __topic_listener__(self, tm):
        message_type = type(tm.message)
        self._terminal.write(f'{tm.name}: {message_type} {tm.message} {tm.publish_time} {tm.member}\n')

    def execute_topic(self, topic):
        data = {}
        self._blotter.page = 1
        if topic == None:
            ds_name = None
        else:
            ds_name = topic.name

from hazelcast.proxy.reliable_topic import ReliableMessageListener

class ReliableTopicListener(ReliableMessageListener):

    def __init__(self, dac_topic):
        self._dac_topic = dac_topic

    def on_message(self, tm):
        message_type = type(tm.message)
        self._dac_topic._terminal.write(f'{tm.name}: {message_type} {tm.message} {tm.publish_time} {tm.member}\n')

    def retrieve_initial_sequence(self):
        # -1 -> No initial sequence. Start from the next published message.
        return -1

    def store_sequence(self, sequence):
        pass

    def is_loss_tolerant(self):
        return False

    def is_terminal(self, error):
        print(error)
        return True
