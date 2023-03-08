# %%
"""
DacHelp displays help information.
"""

import os
import pandas as pd
import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.class_util import get_attributes
from padogrid.hazelcast.playground.class_util import get_class_name
from padogrid.hazelcast.playground.hazelcast_util import HazelcastUtil
from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.HazelcastCluster import HazelcastCluster

class DacHelp(DacBase, Viewer):

    value = None

    def __init__(self, **params):
        super().__init__(**params)

        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        file_path=f'{dir_path}/help.md'
        with open(file_path, 'r') as file:
            content = file.read()
        file.close()
        self._help_markdown = pn.pane.Markdown(content)
        self.reset(self.hazelcast_cluster)
        self._layout = pn.Column(pn.Row(self._help_markdown))
        self._sync_widgets()

    def refresh(self, is_reset=False):
        return
        
    def __panel__(self):
        return self._layout

    @param.depends('value', watch=True)
    def _sync_widgets(self):
        self.value = ''
            
    @param.depends('value', watch=True)
    def _sync_params(self):
        self.value = ''
