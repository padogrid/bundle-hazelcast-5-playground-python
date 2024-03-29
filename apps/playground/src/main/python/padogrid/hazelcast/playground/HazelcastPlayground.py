# %%
import os
import datetime as dt
import numpy as np
import pandas as pd
import panel as pn
pn.extension('tabulator', css_files=[pn.io.resources.CSS_URLS['font-awesome']])
pn.extension(sizing_mode="stretch_width")

# %%
from padogrid.hazelcast.playground.DacBase import DacBase
from padogrid.hazelcast.playground.HazelcastCluster import HazelcastCluster
from padogrid.hazelcast.playground.init_util import get_playground_config
from padogrid.hazelcast.playground.init_util import get_create_functions
from padogrid.hazelcast.playground.init_util import get_er
from padogrid.hazelcast.playground.init_util import get_components
# %% [markdown]
# ## DacClusterConnect

# %%
from padogrid.hazelcast.playground.DacClusterConnect import DacClusterConnect

def initialize():
    global playground_config
    global cluster
    global component_list

    playground_config = get_playground_config()
    cluster = HazelcastCluster()

    obj_creation_function_dict = get_create_functions(playground_config)
    er_dict = get_er(playground_config, obj_creation_function_dict)
    obj_type_list = list(obj_creation_function_dict.keys())
    er_name_list = list(er_dict.keys()) + ['N/A']
    component_list = get_components(playground_config, cluster)
    DacBase.config = playground_config
    DacBase.obj_creation_function_dict = obj_creation_function_dict
    DacBase.er_dict = er_dict
    DacBase.obj_type_list = obj_type_list
    DacBase.er_name_list = er_name_list
    DacBase.component_list = component_list

# Connect to cluster
def connect_click(hazelcast_cluster):
    global cluster
    cluster = hazelcast_cluster
    # desktop.open_modal()
    
# Initialize Dac component services including HazelcastCluster
initialize()

# Create ClusterConnect
dac_cluster_connect = DacClusterConnect(hazelcast_cluster=cluster, cluster_url='dev@localhost:5701')
dac_cluster_connect.on_click(connect_click)
client = cluster.hazelcast_client
dac_cluster_connect

# %% [markdown]
# ## DacDataStructureTable

# %%
from padogrid.hazelcast.playground.DacDataStructureTable import DacDataStructureTable

dac_ds_table = DacDataStructureTable(hazelcast_cluster=cluster)

def dac_ds_table_click(event):
    ds_type, ds_name = dac_ds_table.get_type_name(event.row)
    index = get_tab_index(ds_type)
    if index == -1:
        return
    
    main_tabs.active = index
    if ds_type == "Map":
        dac_map_query.execute_query("select * from \"" + ds_name + "\" limit 1000")
        card_map_blotter_title_text.value = truncateString(dac_map_query.title_text)
    elif ds_type == "MultiMap":
        map = cluster.get_ds(ds_type, ds_name)
        dac_multi_map.execute_map(map)
        card_multi_map_title_text.value = truncateString(dac_multi_map.title_text)   
    elif ds_type == "ReplicatedMap":
        map = cluster.get_ds(ds_type, ds_name)
        dac_replicated_map.execute_map(map)
        card_replicated_map_title_text.value = truncateString(dac_replicated_map.title_text)   
    elif ds_type == "Queue":
        dac_queue.select(ds_name)
    elif ds_type == "List":
        dac_list.select(ds_name)
    elif ds_type == "Set":
        dac_set.select(ds_name)
    elif ds_type == "Ringbuffer":
        dac_ringbuffer.select(ds_name)
    elif ds_type == "PNCounter":
        dac_pn_counter.select_pn_counter(ds_name)
    elif ds_type == "FlakeIdGenerator":
        dac_flake_id_generator.select(ds_name)
    elif ds_type == "Topic":
        dac_topic.select(ds_name)
    elif ds_type == "ReliableTopic":
        dac_reliable_topic.select(ds_name)
    elif ds_type == "MapKeySearch":
        dac_map_key_search.select(ds_name)
    elif ds_type == "ReplicatedMapKeySearch":
        replicated_dac_map_key_search.select(ds_name)
    elif ds_type == "Ingestors":
        dac_ingestor.select(ds_name)

dac_ds_table.on_click(dac_ds_table_click)
dac_ds_table


# %% [markdown]
# ## SQL

# %% [markdown]
# ## DacHelp

# %% [markdown]
# 

# %%
from padogrid.hazelcast.playground.DacHelp import DacHelp
dac_help = DacHelp(hazelcast_cluster = cluster)
dac_help

# %% [markdown]
# ## DacFlakeIdGenerator

# %%
from padogrid.hazelcast.playground.DacPNCounter import DacPNCounter
dac_pn_counter = DacPNCounter(hazelcast_cluster=cluster)
dac_pn_counter                           

# %% [markdown]
# ## DacIngestorProgress

# %%
from padogrid.hazelcast.playground.DacIngestorProgress import DacIngestorProgress
dac_ingestor_progress = DacIngestorProgress(hazelcast_cluster=cluster)
dac_ingestor_progress

# %% [markdown]
# ## DacIngestor

# %%
from padogrid.hazelcast.playground.DacIngestor import DacIngestor
dac_ingestor = DacIngestor(hazelcast_cluster=cluster, ingestor_progress=dac_ingestor_progress)
dac_ingestor

# %% [markdown]
# ## DacList

# %%
from padogrid.hazelcast.playground.DacList import DacList
dac_list = DacList(hazelcast_cluster=cluster)
dac_list

# %% [markdown]
# ## DacMapKeySearch

# %%
from padogrid.hazelcast.playground.DacMapKeySearch import DacMapKeySearch

# Map
dac_map_key_search = DacMapKeySearch(dac_id='MapKeySearch', hazelcast_cluster=cluster)
dac_map_key_search

# ReplicatedMap
replicated_dac_map_key_search = DacMapKeySearch(dac_id='ReplicatedMapKeySearch', hazelcast_cluster=cluster, map_type='ReplicatedMap')
replicated_dac_map_key_search

# %% [markdown]
# ## DacMapQuery

# %%
from padogrid.hazelcast.playground.DacMapQuery import DacMapQuery

# Max length of card title text
CARD_TITLE_MAX_LEN = 26

def truncateString(str):
    if len(str) > CARD_TITLE_MAX_LEN:
        str = str[0:CARD_TITLE_MAX_LEN] + "..."
    return str
        
def dac_map_query_click(query):
    card_map_blotter_title_text.value = query
    card_map_blotter_title_text.value = truncateString(dac_map_query.title_text)
    
dac_map_query = DacMapQuery(hazelcast_cluster=cluster)
dac_map_query.on_click(dac_map_query_click)
dac_map_query

# %% [markdown]
# ## DacMultiMap

# %%
from padogrid.hazelcast.playground.DacMultiMap import DacMultiMap

dac_multi_map = DacMultiMap(hazelcast_cluster=cluster)

def dac_multi_map_click(map):
    card_multi_map_title_text.value = map.name
    card_multi_map_title_text.value = truncateString(dac_multi_map.title_text)
    
dac_multi_map.reset(cluster)
dac_multi_map

# %% [markdown]
# ## DacPNCounter

# %%
from padogrid.hazelcast.playground.DacFlakeIdGenerator import DacFlakeIdGenerator
dac_flake_id_generator = DacFlakeIdGenerator(hazelcast_cluster=cluster)
dac_flake_id_generator

# %% [markdown]
# ## DacReplicatedMap

# %%
from padogrid.hazelcast.playground.DacReplicatedMap import DacReplicatedMap

dac_replicated_map = DacReplicatedMap(hazelcast_cluster=cluster)

def dac_replicated_map_click(map):
    card_replicated_map_title_text.value = map.name
    card_replicated_map_title_text.value = truncateString(dac_replicated_map.title_text)
    
dac_replicated_map.reset(cluster)
dac_replicated_map

# %% [markdown]
# ## DacRingbuffer

# %%
from padogrid.hazelcast.playground.DacRingbuffer import DacRingbuffer
dac_ringbuffer = DacRingbuffer(hazelcast_cluster=cluster)
dac_ringbuffer

# %% [markdown]
# ## DacQueue

# %%
from padogrid.hazelcast.playground.DacQueue import DacQueue

def dac_queue_click(obj, exception):
    pass
    
dac_queue = DacQueue(hazelcast_cluster=cluster)
dac_queue.on_click(dac_queue_click)
dac_queue

# %% [markdown]
# ## DacSet

# %%
from padogrid.hazelcast.playground.DacSet import DacSet
dac_set = DacSet(hazelcast_cluster=cluster)
dac_set

# %% [markdown]
# ## DacTopic - ReliableTopic

# %%
from padogrid.hazelcast.playground.DacTopic import DacTopic
dac_reliable_topic = DacTopic(dac_id='ReliableTopic', hazelcast_cluster = cluster, topic_type='ReliableTopic')
dac_reliable_topic

# %% [markdown
# ## DacTopic

# %%
from padogrid.hazelcast.playground.DacTopic import DacTopic
dac_topic = DacTopic(hazelcast_cluster = cluster)
dac_topic

# %% [markdown]
# ## Playground

# %%
def create_dac_help_card():
    global card_dac_help_title_text
    global dac_help
    card_dac_help_title_text = pn.widgets.StaticText(name='Help', value='')
    card_dac_help = pn.Card(dac_help, name='Help', header=pn.panel(card_dac_help_title_text))
    return card_dac_help

def create_dac_flake_id_generator_card():
    global card_dac_flake_id_generator_title_text
    global dac_flake_id_generator
    card_dac_flake_id_generator_title_text = pn.widgets.StaticText(name='FlakeIdGenerator', value='FlakeIdGenerator')
    card_dac_flake_id_generator = pn.Card(dac_flake_id_generator, name='FlakeIdGenerator', header=pn.panel(card_dac_flake_id_generator_title_text))
    return card_dac_flake_id_generator

def create_dac_ingestor_card():
    global card_ingestor_title_text
    global dac_ingestor
    card_ingestor_title_text = pn.widgets.StaticText(name='Ingestion', value='Ingestion')
    card_ingestor = pn.Card(dac_ingestor, name='Ingestion', header=pn.panel(card_ingestor_title_text))
    return card_ingestor

def create_dac_ingestor_progress_card():
    global card_ingestor_progress_title_text
    global dac_ingestor_progress
    card_ingestor_progress_title_text = pn.widgets.StaticText(name='Ingestion Progress', value='Ingestion Progress')
    card_ingestor_progress = pn.Card(dac_ingestor_progress, name='Ingestion Progress', header=pn.panel(card_ingestor_progress_title_text))
    return card_ingestor_progress 

def create_dac_list_card():
    global card_list_title_text
    global dac_list
    card_list_title_text = pn.widgets.StaticText(name='List', value='List')
    card_list = pn.Card(dac_list, name='List', header=pn.panel(card_list_title_text))
    return card_list

def create_dac_map_query_card():
    global card_map_blotter_title_text
    global dac_map_query
    card_map_blotter_title_text = pn.widgets.StaticText(name='Map', value='Map')
    card_map_blotter = pn.Card(dac_map_query, name='Map', header=pn.panel(card_map_blotter_title_text))
    return card_map_blotter

def create_dac_map_key_search_card():
    global dac_map_key_search
    card_dac_map_key_search = pn.Card(dac_map_key_search, name='MapKeySearch', title='Map Key Search')
    return card_dac_map_key_search

def create_dac_multi_map_card():
    global card_multi_map_title_text
    global dac_multi_map
    card_multi_map_title_text = pn.widgets.StaticText(name='MultiMap', value='MultiMap')
    card_dac_multi_map = pn.Card(dac_multi_map, name='MultiMap', header=pn.panel(card_multi_map_title_text))
    return card_dac_multi_map

def create_dac_pn_counter_card():
    global card_pn_counter_title_text
    global dac_pn_counter
    card_pn_counter_title_text = pn.widgets.StaticText(name='PNCounter', value='PNCounter')
    card_pn_counter = pn.Card(dac_pn_counter, name='PNCounter', header=pn.panel(card_pn_counter_title_text))
    return card_pn_counter

def create_dac_reliable_topic_card():
    global card_reliable_topic_title_text
    global dac_reliable_topic
    card_reliable_topic_title_text = pn.widgets.StaticText(name='ReliableTopic', value='ReliableTopic')
    card_reliable_topic = pn.Card(dac_reliable_topic, name='ReliableTopic', header=pn.panel(card_reliable_topic_title_text))
    return card_reliable_topic

def create_dac_replicated_map_card():
    global card_replicated_map_title_text
    global dac_replicated_map
    card_replicated_map_title_text = pn.widgets.StaticText(name='ReplicatedMap', value='ReplicatedMap')
    card_dac_replicated_map = pn.Card(dac_replicated_map, name='ReplicatedMap', header=pn.panel(card_replicated_map_title_text))
    return card_dac_replicated_map

def create_dac_replicated_map_key_search_card():
    global replicated_dac_map_key_search
    card_replicated_dac_map_key_search = pn.Card(replicated_dac_map_key_search, name='ReplicatedMapKeySearch', title='ReplicatedMap Key Search')
    return card_replicated_dac_map_key_search

def create_dac_ringbuffer_card():
    global card_ringbuffer_title_text
    global dac_ringbuffer
    card_ringbuffer_title_text = pn.widgets.StaticText(name='Ringbuffer', value='Ringbuffer')
    card_ringbuffer = pn.Card(dac_ringbuffer, name='Ringbuffer', header=pn.panel(card_ringbuffer_title_text))
    return card_ringbuffer

def create_dac_queue_card():
    global card_dac_queue_title_text
    global dac_queue
    card_dac_queue_title_text = pn.widgets.StaticText(name='Queue', value='Queue')
    card_dac_queue = pn.Card(dac_queue, name='Queue', header=pn.panel(card_dac_queue_title_text))
    return card_dac_queue

def create_dac_set_card():
    global card_set_title_text
    global dac_set
    card_set_title_text = pn.widgets.StaticText(name='Set', value='Set')
    card_set = pn.Card(dac_set, name='Set', header=pn.panel(card_set_title_text))
    return card_set

def create_dac_topic_card():
    global card_topic_title_text
    global dac_topic
    card_topic_title_text = pn.widgets.StaticText(name='Topic', value='Topic')
    card_topic = pn.Card(dac_topic, name='Topic', header=pn.panel(card_topic_title_text))
    return card_topic

# %%
card_dac_ingestor_progress = create_dac_ingestor_progress_card()
card_dac_ingestor_progress

# %%
# Template
desktop = pn.template.GoldenTemplate(title='Hazelcast Playground', 
                                    logo='https://avatars.githubusercontent.com/u/12813076?v=4')
#                                    logo='images/sorintlab-transparent.png')
#desktop = pn.template.BootstrapTemplate(title='Hazelcast Playground',
#                                        logo='https://avatars.githubusercontent.com/u/12813076?v=4')
#desktop = pn.template.MaterialTemplate(title='Hazelcast Playground',
#                                       logo='https://avatars.githubusercontent.com/u/12813076?v=4')

# Sidebar
card_cluster = pn.Card(dac_cluster_connect, name='ClusterConnect', title='Cluster')                 
card_dac_ds_table = pn.Card(dac_ds_table, name='Content', title='Content')

# card_queue_table = pn.Card(queue_table, name='DataStructureQueue', title='Queues')
sidebar_tabs = pn.Tabs(card_cluster, card_dac_ds_table, dynamic=True)
desktop.sidebar.append(pn.Column(sidebar_tabs))
# desktop.sidebar.append(pn.Column(card_cluster, card_dac_ds_table))
desktop.sidebar.sidebar_width = 600

# Main
card_dac_help = create_dac_help_card()
card_dac_flake_id_generator = create_dac_flake_id_generator_card()
card_dac_ingestor = create_dac_ingestor_card()
card_dac_ingestor_progress = create_dac_ingestor_progress_card()

card_dac_list = create_dac_list_card()
card_dac_map = create_dac_map_query_card()
card_dac_map_key_search = create_dac_map_key_search_card()
card_dac_multi_map = create_dac_multi_map_card()
card_dac_pn_counter = create_dac_pn_counter_card()
card_dac_replicated_map = create_dac_replicated_map_card()
card_dac_replicated_map_key_search = create_dac_replicated_map_key_search_card()
card_dac_queue = create_dac_queue_card()
card_dac_ringbuffer = create_dac_ringbuffer_card()
card_dac_set = create_dac_set_card()

card_dac_topic = create_dac_topic_card()
card_dac_reliable_topic = create_dac_reliable_topic_card()

main_tabs = pn.Tabs(card_dac_flake_id_generator,
                    card_dac_list,
                    card_dac_map,
                    card_dac_map_key_search, 
                    card_dac_multi_map,
                    card_dac_pn_counter,
                    card_dac_queue,
                    card_dac_reliable_topic,
                    card_dac_replicated_map,
                    card_dac_replicated_map_key_search, 
                    card_dac_ringbuffer,
                    card_dac_set, 
                    card_dac_topic,
                    name='Content',
                    dynamic=True)

def get_tab_index(tab_name):
    '''Returns the tab index of the specified tab name
    '''
    index = -1
    i = 0
    for obj in main_tabs.objects:
        if tab_name == obj.name:
            index = i
            break
        i = i + 1
    return index

desktop.main.append(
    main_tabs
)
desktop.main.append(
    card_dac_ingestor
)
desktop.main.append(card_dac_ingestor_progress)

# Additional components defined in playground.yaml
sidebar_root_dict = {}
main_root_dict = {}
for component in component_list:
    dac = component['component']
    card = pn.Card(dac, name=component['name'], title=component['title'])
    if component['panel'] == 'main':
        desktop_pane = desktop.main 
        root_dict = main_root_dict
    else:
        desktop_pane = desktop.sidebar 
        root_dict = sidebar_root_dict
    root = component['root']
    if root == None:
        desktop_pane.append(card)
    else:
        if root in root_dict:
            main_cards = root_dict[root]
        else:
            main_cards = []
            root_dict[root] = main_cards
        main_cards.append(card)

for name, cards in main_root_dict.items():
    new_tabs = pn.Tabs(name=name,
                    dynamic=True)
    for card in cards:
        new_tabs.append(card)
    desktop.main.append(new_tabs)

for name, cards in sidebar_root_dict.items():
    new_tabs = pn.Tabs(name=name,
                    dynamic=True)
    for card in cards:
        new_tabs.append(card)
    desktop.sidebar.append(new_tabs)
 
# Help component
desktop.main.append(card_dac_help)

# Use servable by default. Override it by setting the env variable
# PLAYGROUND_SERVABLE anything but 'true', case insensitive.
playground_servable = os.environ.get('PLAYGROUND_SERVABLE', 'true')
if playground_servable.lower() == 'false':
    desktop.show();
else:
    desktop.servable();