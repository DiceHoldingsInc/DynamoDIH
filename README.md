DynamoDB DataImportHandler
=========================
This is a data import handler for Solr.  It was originally developed for Dice.com / DHI Inc which they have kindly agreed to opensource.

Tested/Built against **Solr 6.3**, releases/tags will be added in the future for specific Solr Versions.

Features
------------
- many aws authentication options supported
- Supports using STS to assume an alternative role, useful for cross-account access.
- Automatic aws region setting, if hosted on an ec2 instance, or within the ec2 infrastructure, automatically selects the current region by default.
- Supports various form of explicit aws credentials using Java Properties or setting them directly in the DIH configuration.
- Support for Dynamo Query Features
	- key condition expression
	- filter expression
	- projection expression
	- KeyMap
	- ValueMap
- automatically chooses between a Query and a Scan based on the presence of condition expressions.

Configuration
-------------------
The Dynamo data-import-handler configuration is similar to other DIH configurations, you must use the ``DataSource`` with the ``EntityProcessor`` as shown in the configuration below.

All configuration parameters support Solr Variable injection.

### DIH solrconfig.xml configuration
Witin ``solrconfig.xml`` you must include the dynamo data import handler, along with solrs DIH libraries:
```xml
  <!-- You must include the standard DIH jars! -->
  <lib dir="${solr.install.dir:../../../..}/dist" regex="solr-dataimporthandler-\d.*\.jar" />
  <lib dir="${solr.install.dir:../../../..}/dist" regex="solr-dataimporthandler-extras-\d.*\.jar" />
  
  <!-- If you placed your dynamo DIH Jar in contrib/solr-dataimporthandler/dynamo/dynamo-dih-6.3.jar"-->
  <lib dir="${solr.install.dir:../../../..}/contrib/solr-dataimporthandler/dynamo/" regex=".*\.jar" />
```

Within your ``solrconfig.xml`` you must define a request handler, or customize the *default* one.
This is where you will specify the data hanlder configuration you will use with this request handler.
Note that when you use the default request handler endpoint of ``/dataimport`` you'll be able to use this
DIH from the solr admin interface.
```xml
    <requestHandler name="/dataimport" class="org.apache.solr.handler.dataimport.DataImportHandler">
      <lst name="defaults">
        <str name="config">dataimport/dynamo.xml</str>
      </lst>
    </requestHandler>
```

### dataimport.properties
Within the root directory of your core/collection configuration you must define a ``dataimport.properties`` file.
This is where the data-import-handler global settings are stored, but more importantly, each time your 
data-import-handler runs it will save the last import time for your collection to top of this file.

**Scheduling:** this file also enables you to schedule the data-import-hanlder to periodically run.
You can specify the url parameters to execute each time the dataimport runs.

Here is an example/default ``dataimport.properties`` file.
```bash
#################################################
#                                               #
#       dataimport scheduler properties         #
#                                               #
#################################################

#  to sync or not to sync
#  1 - active; anything else - inactive
syncEnabled=0

#  solr server name or IP address
#  [defaults to localhost if empty]
server=localhost

#  solr server port
#  [defaults to 80 if empty]
port=8983

#  URL params [mandatory]
#  remainder of URL
params=/select?qt=/dataimport&command=full-import&clean=false&commit=false
```

### DataSource Parameters
All ``<dataSource>`` parameters are optional

- ``endpoint`` - AWS Dynamo Endpoint (expert)
- ``region`` - Region name
- ``stsRoleARN`` - STS Role ARN to assume before connecting to Dynamo (will use credential settings).  This is typically required if you need to access a different AWS account. Perhaps your ``dev`` and ``production`` aws environments are separated into 2 different accounts.  The ``dev`` account might need to assume a role that is configured within ``production``.
- ``stsEndpoint`` - Custom endpoint to use for sts (expert)
- ``stsDuration`` - The duration of STS alternative credentials in seconds.
- ``accessKeyId`` - Explicitly set the acess key for AWS 
- ``secretKeyId`` - Explicitly set the secret key for AWS
- ``credentialProfilesFile`` - Explicitly specify an aws profiles file to use.
- ``credentialProfilename`` - Explicitly specify which profile name within the profile file to use.
- ``credentialUseProfileDefaults`` - (true/false) Use the default profiles file in the default location, use NO other form of AWS authentication.
- ``credentialUseJavaProperties`` - (true/false) Use java properties for authentication, use NO other form of AWS authentication.
- ``convertType`` - Convert field types to the explicitly defined type in each ``<field>`` element. Supported types are dynamo type names: ``N, I, S, L, BOOL``

Note that, whatever credentials you provide if ``stsRoleARN`` is specified, the credentials provided will be used to obtain
the sts role!  When we assume a role, using Amazons STS it provides us with temporary credentials.  This is most useful for
cross account authentication.

### Entity Parameters
The only required entity parameter is ``tableName``
- ``pk`` - used to compare records / duplicates, this should be the name of your solr field. It will automatically be mapped to the corresponding dynamo field for set comparison and handling deletes.
- ``tableName`` - (required) the dynamo table name to retrieve records from.
- ``keyConditionExpression`` - a key condition expression to use with your query (if not used the dynamo table will be scanned!)
- ``filterExpression`` -  a filter expression to use with your query, (applied after results are returned)
- ``projectionExpression`` - a projection express to use with your query/scan (controls what fields are returned)
- ``nameMap`` - (Field Prefix) When your query contains keyword values, you must use nameMap to provide alternative names, that arent' reserved. (see NameMap below)
- ``valueMap`` - (Field Prefix) If you wish to inject variables into your query, you can use a Value Map to (safely) achieve this. (see ValueMap below)

### Example DIH Configuration
```xml
<dataConfig>
  <dataSource type="com.dhi.solr.dataimporthandler.DynamoDataSource" 
              applicationName="DynamoImport" name="DynamoDataSource"/>
              
  <document>
    <entity name="DynamoEntity" processor="com.dhi.solr.dataimporthandler.DynamoEntityProcessor"
            dataSource="DynamoDataSource" 
            tableName="solr-records">
            
        <field column="id" name="id" />
        <field column="description" name="description" />
        <field column="title" name="title" />
    </entity>
  </document>
</dataConfig>
```

### Example DIH Configuration for Delta Import

#### Delta Entity Fields
Because we don't want to make any assumptions for you, the only shared value between **FULL IMPORT**
and **DELTA IMPORT** is the ``tableName`` entity value.

For the DELTA IMPORT at least one **Key Condition** or **Filter Conditon** must be specified.

These entity fields are for DELTA IMPORT:
- ``DELTAkeyConditionExpression``
- ``DELTAfilterExpression``
- ``DELTAprojectionExpression``
- ``DELTAnameMap``
- ``DELTAvalueMap``

#### Custom Variables
Dynamo does not support actual "DATE" objects by default, for this reason people often use epoch
numbers for comparison of dates if they want to search using a ``keyConditionExpression`` by creating a secondary
index on a dynamo field (*attribute*) containing an epoch date.  
We support this by helping you to build a custom **keyConditionExpression** using epoch seconds by providing custom variables.

For deltaimport these custom variables are provided for you:
- ``dataimport.dynamo.last_index_time_epoch_sec``
- ``dataimport.dynamo.last_index_time_epoch_ms``

    
The custom variables are created by reading the ``dataimport.last_index_time`` variable that Solr provides.
The DataImportHandler currently assumes the date format for ``dataimport.last_index_time`` is the default 
of: ``yyyy-MM-dd HH:mm:ss`` - Because this is the default, you shouldn't need to change anything to make
this work.

**Delta Import Configuration**
```xml
  <dataSource type="com.dhi.solr.dataimporthandler.DynamoDataSource"
              applicationName="DynamoImport" name="DynamoDataSource"/>

  <document>
    <entity name="DynamoEntity" processor="com.dhi.solr.dataimporthandler.DynamoEntityProcessor"
            dataSource="DynamoDataSource"
            tableName="solr-data-import"
            pk="id"
            DELTAkeyConditionExpression="#updated >= :lastupdate"
            DELTAnameMapUpdate="#updated, update_time"
            DELTAvalueMapUpdate="Long :lastupdate, ${dataimport.dynamo.last_index_time_epoch_ms}"
            transformers="HTMLStripTransformer,DateFormatTransformer">
            
        <!-- Note that column is the name of the field/attribute in DYNAMO, and name is the name of
             the SOLR field. -->
        <field column="id"                name="id" />
        <field column="title"             name="title" />
        <field column="summary"           name="summary" />
        <field column="brandName"         name="brand_name" />
        <field column="description"       name="description" />
        <field column="createdDate"       name="created_date"  dateTimeFormat="yyyy-MM-dd'T'HH:mm'Z'"/> <!-- ISO8601 Date format-->
        <field column="featured"          name="is_featured"/>
        
    </entity>
  </document>
</dataConfig>

```

Name Maps
---------
**NameMaps** provide a way to avoid conflicts within your query between column names, and reserved names.

**Syntax:** ``nameMap[Unique-Name]="[key],[value]"``

in the Dynamo query syntax. "year" is a reserved word, so if you want to use the dynamo field named "year" in a projection:
```xml
<entity processor="com.dhi.solr.dataimporthandler.DynamoEntityProcessor"
        projectionExpression="#yr"
        nameMapYear="#yr, year" />
```

You can specify as many ``<entity>`` attributes as you want that begin with **nameMap**.
But to make each attribute unique you must use a suffix, the suffix is arbitrary and ignored, it
simply provides a unique name.  Attributes like: ``nameMap1``, ``nameMap2``, ``nameMap3`` are
perfectly legal.

Value Maps
----------
**ValueMaps** provide a way to inject values into a ``filterExpression`` or ``keyConditionExpression`` at query time.

**Syntax:** ``valueMap[Unique-Name]="[Type] :[FieldName], [Value]"``

The dynamo documentation for Java has several examples of doing exactly this for example:
```java
 ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression("#yr, title, info.rating")
                .withFilterExpression("#yr between :start_yr and :end_yr")
                .withNameMap(new NameMap().with("#yr",  "year"))
                .withValueMap(new ValueMap().withNumber(":start_yr", 1950).withNumber(":end_yr", 1959));
```

To accomplish this same query using the DataImportHandler configuration you would provide
an ``<entity>`` configuration like this:
```xml
<entity processor="com.dhi.solr.dataimporthandler.DynamoEntityProcessor"
        filterExpression=""#yr between :start_yr and :end_yr"
        projectionExpression="#yr, title, info.rating"
        nameMapYear="#yr, year"
        valueMapStart="Int :start_yr, 1950"
        valueMapEnd="Int :end_yr, 1959" />
```

You can specify as many ``<entity>`` attributes as you want that begin with **valueMap**.
But to make each attribute unique you must use a suffix, the suffix is arbitrary and ignored, it
simply provides a unique name.  Attributes like: ``valueMap1``, ``valueMap2``, ``valueMap3`` are
perfectly legal.

#### Value Map Solr Variable Injection
Because we support solr template variables in all ValueMaps, you can inject solr variables into
your value that will be evaluated when the request is evaluated.

**Request variables** can be injected from the DataImportHandler request arguments using:

- ``${dataimporter.request.[url-argument]}``  where **url-argument** is the name of the url argument passed-into the DataImport request handler.

For example the argument: 
- ``custom_arg`` 
from the request: 
- ``params=/select?qt=/dataimport&command=full-import&clean=false&commit=false&custom_arg=hello``

can be used in a valueMap like this:
```xml
    <entity valueMapCustom="String :custom, ${request.custom_arg}" />
```

Other variables include:

- ``${handlerName}``
- ``${dataimporter.index_start_time}``
- ``${dataimporter.last_index_time}``
- ``dataimport.dynamo.last_index_time_epoch_sec``   *the last time import was run in epoch seconds since 1970*
- ``dataimport.dynamo.last_index_time_epoch_ms``    *the last time import was run in epoch milliseconds since 1970*

TODO
--------
- Needs more thorough tests, make use of solrs embedded server/testing framework
- Needs to add-to/support debug response so it can log usage after every import.
- Deletions support needs to be added so stale records are removed.
- OnError setting needs to be respected (if it isn't?)
- Map/Path expansion so sub-maps can be extracted as field values.

Feedback
-------------
If you like this module, would like to improve it, or have a feature suggestion please feel free to contact me ``ben.demott`` at ``gmail`` dot ``com``.
