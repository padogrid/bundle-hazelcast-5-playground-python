![PadoGrid](https://github.com/padogrid/padogrid/raw/develop/images/padogrid-3d-16x16.png) [*PadoGrid*](https://github.com/padogrid) | [*Catalogs*](https://github.com/padogrid/catalog-bundles/blob/master/all-catalog.md) | [*Manual*](https://github.com/padogrid/padogrid/wiki) | [*FAQ*](https://github.com/padogrid/padogrid/wiki/faq) | [*Releases*](https://github.com/padogrid/padogrid/releases) | [*Templates*](https://github.com/padogrid/padogrid/wiki/Using-Bundle-Templates) | [*Pods*](https://github.com/padogrid/padogrid/wiki/Understanding-Padogrid-Pods) | [*Kubernetes*](https://github.com/padogrid/padogrid/wiki/Kubernetes) | [*Docker*](https://github.com/padogrid/padogrid/wiki/Docker) | [*Apps*](https://github.com/padogrid/padogrid/wiki/Apps) | [*Quick Start*](https://github.com/padogrid/padogrid/wiki/Quick-Start)

---

# Hazelcast Playground

Hazelcast Playground provides read/write access to Hazelcast data structures. You can periodically ingest mock data to any of the Hazelcast data structures and view updates at the same time.

## Connect and Play

There are eight (8) root tabs: *Sidebar*, *Content*, *Ingestion*, *Ingestion Progress*, *Query1*, *Query2*, *PadoGrid1*, *PadoGrid2*, and *Help*. You can move them by dragging your mouse to any where in the browser. For example, you might want to place *Ingestion* and *Ingestion Progress* side by side to start ingestion jobs and view their statuses in the same view.


### *Sidebar*

*Sidebar* contains *ClusterConnect* and *Content* tabs. Select *ClusterConnect* to connect to a Hazelcast cluster, *Content* to view a complete list of data structures in the cluster. 

#### *Sidebar/ClusterConnect*

Enter your cluster URL in the format of `cluster-name@host:port` and click the **Connect** button.

#### *Sidebar/DataStructureTable*

If your cluster contains no data, then *Sidebar/DataStructureTable* will also be empty. In that case, try ingesting mock data to each data structure type from the *Ingestion* tab.

Playground gathers data structure information from the Hazelcast cluster and refreshes its components every five (5) seconds. You can also click on the **Refresh** button to immediately refresh the components. This button is useful for `MapKeySearch` and `ReplicatedMapSearch`, which update the type-ahead keys with the latest key sets retrieved from the cluster.

If you select any of the data structures in the table, its content will be displayed in the main pane's *Content* tab. At this time, due to Panel limitations, selecting a data structure will not automatically make the *Content* pane visible. If the *Content* pane is not visible, then you must manually select the *Content* pane to view the data structure you selected from *Sidebar/DataStructureTable*.

### *Content*

*Content* contains data structure tabs for browsing and inserting data objects. Each tab represents a data structure type and allows you to create new instances, add mock data, and navigate data contents.

✏️  The `Map` tab requires Jet enabled in the cluster. By default, Hazelcast disables Jet. You can enable it by adding the following in the Hazelcast configuration file.

```xml
<hazelcast ...>
...
   <jet enabled="true">
   </jet>
...
</hazelcast>
```

### *Ingestion*

*Ingestion* allows you to create data structures and periodically ingest data into them. You can start up to 10 ingestion jobs.

To create a new data structure, enter a name in the text field next to the **New** button and hit the `Enter` key or click on the **New** button. This will create a new empty data structure in the cluster and update the ingestion table by inserting a new row for the new data structure.

### Ingestion Table Columns

#### *Name*

The *Name* column is not editable and contains the existing data structures in the cluster.

#### *Operation*

The *Operation* column is editable and provides a pulldown menu containing all the ingestion methods available to the data structure. For example, the `Map` data structures has the mothods, `put`, `put_all`, and `set`. Select the operation that you want to perform during ingestion.

#### *Key*

The *Key* column is editable and provides a pulldown menu containing the options, `random`, `uuid` and `custom`. This column applies only to `Map`, `MultiMap`, and `ReplicatedMap`. The `random` option creates string keys with random integer values. The `uuid` option creates UUID string keys in hex values. The `custom` option is reserved for your own custom object creation functions. If you do not wish to support the `custom` option in your creation functions, then you should default to `random` to be consistent with the Playground's default object creation functions, which default to `random` if the `custom` option is selected.

#### *Key Range*

The *Key Range* column is editable and only applicable to the `random` and `custom` *Key* options. This value sets the range of the integer value of the key. For example, setting this column to 10,000  generates keys between 0 and 9999 inclusive. All default `random` object integer values are padded with leaading 0's to create the fixed length of eight (8). Furthermore, the default `Customer` and `CustomerJson` object keys are prefixed with 'c', and the default `Order` and `OrderJson` object keys are prefixed with `o`. Given 10,000, the range of keys for `Customer` and `CustomerJson` is `c000000000` to `c00009999`. Like wise, the range of keys for `Order` and `OrderJson` is `o00000000` to `o00009999`.

By setting the key range low, you can generate updates to the existing keys. This is quite useful for playing with `MultiMap` in particular. `MultiMap` maps one or more objects to a single key.

#### *Object*

The *Object* column is editable and provides a pulldown menu containing the objects defined in the [`playground.yaml`](src/main/python/padogrid/hazelcast/playground/help.md) file. The default menu contains `Customer`, `Order`, `CustomerJson`, and `OrderJson`. Each object represents the object type that is used to create objects by ingestion jobs. For exaxmple, selecting `Customer` ingests `Customer` objects of the type, `hazelcast.serialization.api.Portable`. `CustomerJson` ingests customer objects of the type, `hazelcast.core.HazelcastJsonValue`.

#### *ER*

The *ER* column is editable and provides a pulldown menu containing the ER objects defined in the [`playground.yaml`](src/main/python/padogrid/hazelcast/playground/help.md) file. The default menu contains `Customer_Order`, `Customer_Order_Json`, and `N/A`.  Each object represents the ER object type that is used to establish entity relationships defined in the `playground.yaml` file as shown below. 
```yaml
er:
    # Customer_Order creates one Customer object with 0 to 5 Order objects.
    # Order objects are stored in the data structure named 'orders_er'.
    Customer_Order:
        orders_er:
            object: Order
            attributes:
                - attribute:
                    from: __key
                    to: customerId
            otm: 5
            otm-type: random
```

The `yaml` file snippet above defines `Customer_Order` which ingests up to five (5) `Order` objects referenced by the object selected in *Object* column. If the *Object* column has `Customer`, then the `orders_er` data structure would contain up to five (5) `Order` objects with their `customerId` attribute set to the `__key` value of the object selected in the *Object* column.

If `N/A` is elected in the *ER* column, then the ER operation is not performed.


#### *Count*

The *Count* column is editable. Enter the number of objects that you want to ingest in this column. This column represents the object type selected in the *Object* column. If you select an ER, then there will be additional ER objects ingested in its respective data structure.

#### *Batch Size*

The *Batch Size* column is editable. This column is meaningful only if the `put_all` operation selected in the *Operation* column. The `pull_all` method inserts a batch of entries as a single operation, providing high throughput. This column is displayed only for `Map` and `ReplicatedMap`. 

#### *Cell Delay (msec)*

The *Cell Delay (msec)* column is editable. Enter a delay in milliseconds between operation calls. Each operation call will be delayed by this column value. The lowest value that you can enter is 10 milliseconds.

#### *Ingest*

The *Ingest* column is editable. Selecting this column will immediately start the ingestion job for that row's data structure. You can monitor the ingestion statuses in the *Ingestion Progress* tab.

#### *Trashcan*

❗The *Trashcan* column destroys the data structure by removing it from the cluster. Be careful with this column. The destroyed data structures are not recoverable.

### *Ingest Progress*

*Ingest Progress* displays the ingestion status of each job. You can stop each job at any time by selecting the trashcan in the last column. Note that the trashcan in this table does not destroy the data structure. It only stops the ingestion job.

### *Query1* and *Query2*

*Query1* and Query2* are provided so that you can reposition them side by side to compare `Map` query results.

### *PadoGrid1* and *PadoGrid2*

*PadoGrid1* and *PadoGrid2* are terminals for remotely executing shell commands in the PadoGrid environment.

### *Help*

*Help* displays this content. 

## Panel Limitations

Playground is built using [Panel](https://panel.pyviz.org), a high-level GUI framework made for data scientists. Panel's easy-to-use API hides the web programming complexity, but unfortunately, it also comes with some basic limitations, which hindered the natural UI flow of Playground. The author hopes to resolve some of the issues as Panel lifts the limitations.

- Unable to layout the screen programmatically. Playground uses [GoldenTemplate](https://panel.holoviz.org/reference/templates/GoldenLayout.html), which has no API support for screen layout. The screen can only be arranged by user.
- If you close any of the root tabs, you will not be able to reinstate them. To display them again, you must restart the Playgroud session by refreshing the browser.
- Panel has no support for screen persistence. This means if you had rearranged the screen, that layout is lost and the new session will always reset to the default screen layout.
- Panel lacks support for trapping key events other than th 'Enter' key. For example, `TextAreaInput` has no support for selecting text and trapping Ctrl-Enter. This is evident for the `DacMapQuery` widget which requires you to click on the **Execute Query** button to execute your query statement entered in the SQL input area. Furthermore, `TextAreaInput` has no API for getting text selection.
- Panel's Tabulator widget does not render properly if `DataFrame` is initially empty and populated thereafter. This bug forces Playground to initialize every `Tabulator` instance with an empty row.
- There are several other Panel limitations that required workarounds.

## Playground Limitations

- Playground currently supports string keys only. All other types are silently ignored.

## Playground Watchouts

Due to Hazelcast limitations, there are some watchouts that can potentially impact Hazelcast performance.

- Selecting `MultiMap`, `ReplicatedMap`, or `Ringbuffer` retrieves the entire entries. This is a very expensive call if the data structure has a large number of entries.
- Similarly, selecting `MapKeySearch` or `ReplicatedMapKeySearch` retrieves the entire keys. Although not as expensive as retrieving the entire entries, this is still an expensive call.
- In the *Ingestion* tab, for each data structure, it makes one or more remote calls to determine the object type. Unfortunately, Hazelcast has no API support for retrieving a single entry or a limited number of entries.
    - `Set` - Retrieves the entire entries.
    - `Map`, `MultiMap`, `ReplicatedMap` - Retrieves the entire keys and uses one key from the key list to determine its object type.

## SQL Mapping

Playground automatically executes a `MAPPING` statement for `Portable` objects. At this time, however, `MAPPING` is not executed for JSON objects. This may change later. 

You can manually execute `MAPPING` statements from the *Query1* and *Query2* tabs. The `MAPPING` statements for the default Playground objects are shown below.

### Portable Mapping

**Customer**

```sql
CREATE OR REPLACE MAPPING "customers"
TYPE IMap 
OPTIONS ('keyFormat'='java',
         'keyJavaClass'='java.lang.String',
         'valueFormat'='portable',
         'valuePortableFactoryId'='1',
         'valuePortableClassId'='101',
         'valuePortableClassVersion'='0')
```

**Order**

```sql
CREATE OR REPLACE MAPPING "orders"
TYPE IMap 
OPTIONS ('keyFormat'='java',
         'keyJavaClass'='java.lang.String',
         'valueFormat'='portable',
         'valuePortableFactoryId'='1',
         'valuePortableClassId'='109',
         'valuePortableClassVersion'='0')
```

### JSON Mapping

**Customer**

```sql
CREATE OR REPLACE MAPPING customers_json
TYPE IMap
OPTIONS (
    'keyFormat' = 'varchar',
    'valueFormat' = 'json'
)
```

**Order**

```sql
CREATE OR REPLACE MAPPING orders_json
TYPE IMap
OPTIONS (
    'keyFormat' = 'varchar',
    'valueFormat' = 'json'
)
```

## References

1. Hazelcast Python Client, <https://hazelcast.readthedocs.io>
2. Panel, GUI framework, <https://panel.pyviz.org>
3. Faker, Mock data generator, <https://faker.readthedocs.io>

---

![PadoGrid](https://github.com/padogrid/padogrid/raw/develop/images/padogrid-3d-16x16.png) [*PadoGrid*](https://github.com/padogrid) | [*Catalogs*](https://github.com/padogrid/catalog-bundles/blob/master/all-catalog.md) | [*Manual*](https://github.com/padogrid/padogrid/wiki) | [*FAQ*](https://github.com/padogrid/padogrid/wiki/faq) | [*Releases*](https://github.com/padogrid/padogrid/releases) | [*Templates*](https://github.com/padogrid/padogrid/wiki/Using-Bundle-Templates) | [*Pods*](https://github.com/padogrid/padogrid/wiki/Understanding-Padogrid-Pods) | [*Kubernetes*](https://github.com/padogrid/padogrid/wiki/Kubernetes) | [*Docker*](https://github.com/padogrid/padogrid/wiki/Docker) | [*Apps*](https://github.com/padogrid/padogrid/wiki/Apps) | [*Quick Start*](https://github.com/padogrid/padogrid/wiki/Quick-Start)
