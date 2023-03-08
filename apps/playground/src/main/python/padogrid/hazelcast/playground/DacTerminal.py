# %%
"""
DacTerminal opens a PadoGrid terminal.
"""

import panel as pn
import param
from panel.viewable import Viewer

from padogrid.hazelcast.playground.DacBase import DacBase

pn.extension('terminal')

class DacTerminal(DacBase, Viewer):

    value = None
    
    def __init__(self, **params):
        super().__init__(**params)
        self._terminal = pn.widgets.Terminal(
            "\nWelcome to PadoGrid!\n\n",
            options={"cursorBlink": True},
            sizing_mode='stretch_both'
        )

        self._layout = pn.Column(self._terminal)
        self._terminal.subprocess.run("bash", "--init-file", ".bash_init")
        #self._sync_widgets()

    def __panel__(self):
        return self._layout