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
	- key conditional expression
	- filter expression
	- projection expression
	- KeyMap
	- ValueMap
- automatically chooses between a Query and a Scan based on the presence of conditional expressions.

Configuration
-------------------
The Dynamo data-import-handler configuration is similar to other DIH configurations, you must use the ``DataSource`` with the ``EntityProcessor`` as shown in the configuration below.

All configuration parameters support Solr Variable injection.

#### DataSource Parameters
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
- ``convertType`` - Convert field types to the explicitly defined type in each ``<field>`` element.

#### Entity Parameters
The only required entity parameter is ``tableName``

- ``tableName`` - (required) the dynamo table name to retrieve records from.
- ``keyConditionalExpression`` - a key condition expression to use with your query (if not used the dynamo table will be scanned!)
- ``filterExpression`` -  a filter expression to use with your query, (applied after results are returned)
- ``projectionExpression`` - a projection express to use with your query/scan (controls what fields are returned)
- ``nameMap`` - (Field Prefix) When your query contains keyword values, you must use nameMap to provide alternative names, that arent' reserved. (see NameMap below)
- ``valueMap`` - (Field Prefix) If you wish to inject variables into your query, you can use a Value Map to (safely) achieve this. (see ValueMap below)

### Example DIH Configuration
```
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
TODO
--------
- Needs more thorough tests
- Needs better POM / Build (includes too many dependencies)
- Needs debug response so it can log usage after every import.
- Delta Import needs to be implemented, as well as Deleted rows set comparison
- OnError setting needs to be respected (if it isn't?)

Feedback
-------------
If you like this module, would like to improve it, or have a feature suggestion please feel free to contact me ``ben.demott`` at ``gmail`` dot ``com``.
