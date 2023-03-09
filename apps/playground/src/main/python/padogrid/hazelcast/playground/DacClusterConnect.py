# %%
"""
DacClusterConnect connects to a Hazelcast cluster
"""

import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

import hazelcast
from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.HazelcastCluster import HazelcastCluster
from padogrid.hazelcast.playground.init_util import get_portable_factories

class DacClusterConnect(DacBase, Viewer):

    _callback = param.Callable(default=None)
    
    value = None
    cluster_url = param.String(default="dev@localhost:5701")
    portable_factories = param.Dict(default={})
        
    def __init__(self, **params):
        super().__init__(**params)

        if "portable_factories" in params:
            self.portable_factories = params["portable_factories"]
        else:
            self.portable_factories = get_portable_factories(DacBase.config)
        if "client_name" in params:
            self.client_name = params["client_name"]
        else:
            self.client_name = "Playground"
        if self.hazelcast_cluster == None:
            self.hazelcast_cluster = HazelcastCluster()

        self._connect_input = pn.widgets.TextInput(disabled=False, placeholder=self.cluster_url)
        self._connect_input.value = self.cluster_url
        self._connect_input.param.watch(self.__connect_execute__, 'value')
        self._connect_button = pn.widgets.Button(name='Connect', width=100, button_type='primary')
        self._connect_button.on_click(self.__connect_execute__)
        self._shutdown_button = pn.widgets.Button(name='Shutdown', width=100, button_type='primary')
        self._shutdown_button.on_click(self.__click_shutdown_button__)
        self._status_text = pn.widgets.TextInput(disabled=True, value='Not connected')
        self._layout = pn.Column(self._connect_input, pn.Row(self._connect_button, self._shutdown_button), self._status_text)
        self._sync_widgets()

    def __panel__(self):
        return self._layout
    
    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = self.cluster_url
         
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = self.cluster_url
    
    def connect(self):
        self.cluster_url = self._connect_input.value
        self._connect()

    def shutdown(self):
        if self.hazelcast_cluster != None:
            self.hazelcast_cluster.shutdown()

    def _connect(self):
        if self.hazelcast_cluster != None:
            cluster, member = self.cluster_url.split("@", 1)
            self.hazelcast_cluster.shutdown()
            hazelcast_client = hazelcast.HazelcastClient(
                client_name=self.client_name,
                labels=["playground"],
                cluster_name=cluster,
                cluster_members=[
                    member
                ],
                lifecycle_listeners=[
                    self.__state_changed__
                    # lambda state : self._status_text.value = state
                    # self._status_text.value = lambda state : state
                    # x = lambda a, b : a * b
                    # lambda state: self._status_text.value = state
                    # lambda state: print("Lifecycle event >>>", state)
                ],
                portable_factories=self.portable_factories,
                cluster_connect_timeout=20,
                statistics_enabled=True,
                statistics_period=5
            )
            # Connect requires resetting HazelcastClient
            self.hazelcast_cluster.reset(hazelcast_client, self.cluster_url)

    def __state_changed__(self, state):
        self._status_text.value = state + " [" + self.cluster_url + "]"
        if state == "SHUTDOWN":
            if self.hazelcast_cluster != None:
                self.hazelcast_cluster.refresh(is_reset=True)

        # if state == "STARTED":
        #     self._status_text.background = 'yellow'
        # elif state == "CONNECTED":
        #     self._status_text.background = 'green'
        # elif state == "SHUTDOWN":
        #     self._status_text.background = 'red'
        # else:
        #     self._status_text.background = 'white'

    def __connect_execute__(self, event):
        self.connect()

    def __click_shutdown_button__(self, event):
        self.shutdown()
        
    def on_click(self, callback=None):
        self._callback = callback

# %%
