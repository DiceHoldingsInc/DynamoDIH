package com.dhi.solr.dataimporthandler;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.JdbcDataSource.CONVERT_TYPE;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient; // aws-java-sdk-sts
import com.amazonaws.services.securitytoken.model.*;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType; // Dynamo types
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;

import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * A custom DataSource to be used as a DataImportHandler that interacts with Amazons DynamoDB
 * 
 * Use this Data Source within your Data Import Handler configuration like so:
 * 
 *   <dataSource type="com.dhi.solr.dataimporthandler.DynamoDataSource"
 *            applicationName="DynamoImport" name="DynamoDataSource"/>
 * 
 * 
 * https://lucidworks.com/2013/08/26/notes-on-dih-architecture-solrs-data-import-handler-2/
 * 
 * SqlEntityProcessor.DELTA_QUERY in the entity attributes tells the DIH delta is supported
 * 
 * Dynamo API:
 * See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ScanJavaDocumentAPI.html
 * 
 * @author ben.demott
 */
public class DynamoDataSource extends DataSource<Iterator<Map<String, Object>>> {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    
    
    protected AmazonDynamoDB dynamoClient;
    protected boolean explicitTypeMapping = false;
    
    // --------------------------------------------
    // init properties (data source parameter keys)
    // All of these parameters are optional.
    public static final String ENDPOINT = "endpoint";
    public static final String REGION = "region";
    public static final String STS_ROLE = "stsRoleARN";
    public static final String STS_ENDPOINT = "stsEndpoint";
    public static final String STS_DURATION = "stsDuration";
    public static final String ACCESS_KEY = "accessKeyId";
    public static final String SECRET_KEY = "secretKeyId";
    public static final String CREDENTIALS_PROFILES_FILE = "credentialProfilesFile";
    public static final String CREDENTIALS_PROFILE_NAME = "credentialProfileName";
    public static final String USE_DEFAULT_PROFILES = "credentialUseProfileDefaults";
    public static final String USE_JAVA_PROPERTIES = "credentialUseJavaProperties";
    
    public static final String CONVERT_FIELD_TYPES = CONVERT_TYPE;
    
    public static final String STS_ROLE_SESSION_NAME = "Solr-DynamoDataImportHandler"; // TODO use class name?
    
    public static final String PROPERTY_TRUE = Boolean.TRUE.toString();
    public static final String PROPERTY_FALSE = Boolean.FALSE.toString();
    
    public static final String JAVA_PROPS_ACCESS_KEY = "aws.accessKeyId";
    public static final String JAVA_PROPS_SECRET_KEY = "aws.secretKey";
    
    public static final Regions DEFAULT_REGION = Regions.US_EAST_1;
    public static final int CONNECTION_TIMEOUT = 30000;
    
    public static final String ERROR_ACCESS_DENIED = "AccessDeniedException";

    /**
     * Called to setup the data import handler before use.  Any setup logic or validation logic 
     * can be placed here.
     * 
     * 
     * 
     * @param context Contains variables defined in the entity configuration, parses this part of
     *         the xml configuration for you and provides convenience functions to access them.
     * @param initProps A Properties instance that contains a mapping of xml attribute-name to 
     *         attribute-value.  Parameters defined inside of the <datasource> are automatically
     *         parsed and stored here as strings.
     */
    @Override
    public void init(Context context, Properties initProps) {
        try {
            validateInitProps(initProps);
        } catch (Exception e) {
            LOG.warn("Configuration error", e);
            wrapAndThrow(SEVERE, e, "Configuration error");
        }
        
        try {
            dynamoClient = getDynamoClient(context, initProps);
        } catch (Exception e) {
            LOG.warn("Configuration error", e);
            wrapAndThrow(SEVERE, e, "Error creating dynamo client");
        }
    }

    /**
     * Validate configuration properties, log warnings / errors if they are mis-configured.
     * 
     * @param initProps 
     * @throws java.lang.Exception 
     */
    protected void validateInitProps(Properties initProps) throws Exception {
        
        explicitTypeMapping = Boolean.parseBoolean(initProps.getProperty(CONVERT_FIELD_TYPES, PROPERTY_FALSE));
        
        final boolean hasJavaPropertyCreds = System.getProperty(JAVA_PROPS_ACCESS_KEY) != null && System.getProperty(JAVA_PROPS_SECRET_KEY) == null;
        final boolean useJavaPropertyCreds = Boolean.parseBoolean(initProps.getProperty(USE_JAVA_PROPERTIES, PROPERTY_FALSE));
        final boolean useDefaultProfilesFile = Boolean.parseBoolean(initProps.getProperty(USE_DEFAULT_PROFILES, PROPERTY_FALSE));
        final String profilesFile = initProps.getProperty(CREDENTIALS_PROFILES_FILE, "");
        final String regionName = initProps.getProperty(REGION, "");
        final String stsDuration = initProps.getProperty(STS_DURATION, "");
        
        // Ensure integer inputs are parseable
        if(!stsDuration.isEmpty()) {
            try {
                Integer.parseInt(stsDuration);
            } catch(NumberFormatException e) {
                throw new Exception(String.format("attribute [%s] must be an integer value, not '%s'... %s", STS_DURATION, stsDuration, e.toString()));
            }
        }
        
        // If access key is given, so too must secret key.
        final String secretKey = initProps.getProperty(SECRET_KEY, "");
        if(!initProps.getProperty(ACCESS_KEY, "").isEmpty() && initProps.getProperty(SECRET_KEY, "").isEmpty()) {
            throw new Exception(String.format("if attribute [%s] is set, attribute [%s] must also be set", ACCESS_KEY, SECRET_KEY));
        }
        
        // If you explicitly say to use java properties, the properties must be present.
        if(useJavaPropertyCreds && !hasJavaPropertyCreds) {
            throw new Exception(String.format("attribute [%s] is %s, but java properties are not defined: ('%s', '%s')", 
                    USE_JAVA_PROPERTIES, PROPERTY_TRUE, JAVA_PROPS_ACCESS_KEY, JAVA_PROPS_SECRET_KEY));
        }
        
        // you cannot use the default profile file, and also use a specific profile file.
        if(useDefaultProfilesFile && !profilesFile.isEmpty()) {
            throw new Exception(String.format("If attribute [%s] is set, attribute [%s] cannot also be set", 
                    USE_DEFAULT_PROFILES, CREDENTIALS_PROFILES_FILE));
        }
        
        // If you define a custom profile file, you must define the profile name to use (DEFAULT) for example.
        if(!profilesFile.isEmpty() && initProps.getProperty(CREDENTIALS_PROFILE_NAME, "").isEmpty()) {
            throw new Exception(String.format("If attribute [%s] is set, attribute [%s] must also be set", 
                    CREDENTIALS_PROFILES_FILE, CREDENTIALS_PROFILE_NAME));
        }
        
        // If a custom endpoint is provided, region MUST also be provided. (this one isn't completely neccesary)
        if(!initProps.getProperty(ENDPOINT, "").isEmpty() && initProps.getProperty(REGION, "").isEmpty()) {
            throw new Exception(String.format("If attribute [%s] is set, attribute [%s] must also be set", ENDPOINT, REGION));
        }
        
        // If a region is provided it must be a valid name.
        if(!regionName.isEmpty() && Regions.fromName(regionName) == null) {
            ArrayList regionNames = new ArrayList();
            for (Regions reg : Regions.values()) {
                regionNames.add(reg.getName());
            }
            throw new Exception(String.format("Invalid attribute [%s] value: '%s'... valid regions: %s", REGION, regionName, regionNames.toString()));
        }
    }
    
    
    /**
     * Resolve the region name to use
     * 
     * If a region is given in the properties, that region will be used.
     * Otherwise, If the program is running inside of an ec2 instances the current region will be used
     * Otherwise, The default region defined in the constant DEFAULT_REGION will be used.
     * 
     * @param regionName A value name of region as defined in the regions xml file, that can be
     *         resolved from the regions api using the fromName() method.
     * @return 
     */
    protected Regions getAwsRegion(String regionName) {
        Regions awsRegion;
        
        if(!regionName.isEmpty()) {
            awsRegion = Regions.fromName(regionName);
        } 
        else if (Regions.getCurrentRegion() != null) {
            String currentRegionName = Regions.getCurrentRegion().getName();
            awsRegion = Regions.fromName(currentRegionName);
        } 
        else {
            awsRegion = DEFAULT_REGION;
        }
        return awsRegion;
    }
    
    /**
     * client configuration / setup - any customization to the client / connection can go in here
     * 
     * retry limits
     * connection timeout
     * socket timeout
     * etc
     * 
     * @return 
     */
    protected ClientConfiguration getAwsClientConfig() {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setConnectionTimeout(CONNECTION_TIMEOUT);
        return clientConfig;
    }
    
    /**
     * Retrieves credentials in the fashion desired based on the arguments provided.
     * Supports defining an explicit profiles.credentials file, and an explicit profile-name.
     * 
     * Supports explicit credentials passed into the configuration directly.
     * 
     * If no valid options are provided for explicit credential use, the default 
     * DefaultAWSCredentialsProviderChain is used.
     * 
     * Todo consolidate exceptions into one CREDENTIAL exception... 
     * Todo add logging to indicate which credential type is being used.
     * 
     * @param useJavaProperties 
     * @param useDefaultProfilesFile 
     * @param profilesFile
     * @param profileName
     * @param accessKey
     * @param secretKey
     * @return credentials provider used for authentication.
     * @throws Exception 
     */
    protected AWSCredentialsProvider getAWSCredentials(
            boolean useJavaProperties, 
            boolean useDefaultProfilesFile, 
            String profilesFile, 
            String profileName, 
            String accessKey, 
            String secretKey ) throws Exception {

        AWSCredentialsProvider provider;
        if(useDefaultProfilesFile) {
            // Loads the default profile, for the default configuration, from the default file location
            provider = new ProfileCredentialsProvider();
        }
        else if(profilesFile != null && !profilesFile.isEmpty()) {
            // Use the custom profile configuration file and a specific profile name
            provider = new ProfileCredentialsProvider(profilesFile, profileName);
        }
        else if ((profilesFile == null || profilesFile.isEmpty()) && profileName != null && !profileName.isEmpty()) {
            // If the configuration has only specified the profileName, use that.
            provider = new ProfileCredentialsProvider(profileName);
        }
        else if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
            // Use excplicit credentials
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
            provider = new AWSStaticCredentialsProvider(awsCreds);
        } 
        else if(useJavaProperties) {
            // provides credentials by looking at the aws.accessKeyId and aws.secretKey Java system properties.
            provider = new SystemPropertiesCredentialsProvider();
        } 
        else {
            provider = new DefaultAWSCredentialsProviderChain();
        }
        
        LOG.info(String.format("using AWS credential provider: [%s]", provider.getClass().getName()));
        return provider;
    }
    
    
    protected AmazonDynamoDB getDynamoClient(final Context context, final Properties initProps) throws Exception {
        
        final String dynamoEndpoint = initProps.getProperty(ENDPOINT, "");
        final Regions dynamoRegion = getAwsRegion(initProps.getProperty(REGION, ""));
        final String dynamoRegionName = dynamoRegion.getName();
        final String stsRoleArn = initProps.getProperty(STS_ROLE, "");
        final boolean credentialsUseSts = !stsRoleArn.isEmpty();
        final String stsEndpoint = initProps.getProperty(STS_ENDPOINT, "");
        final int stsDuration = Integer.parseInt(initProps.getProperty(STS_DURATION, "0"));
        final boolean useJavaPropertyCreds = initProps.getProperty(USE_JAVA_PROPERTIES, "").toLowerCase().equals(PROPERTY_TRUE);
        final boolean useDefaultProfilesFile = initProps.getProperty(USE_DEFAULT_PROFILES, "").toLowerCase().equals(PROPERTY_TRUE);
        final String profilesFile = initProps.getProperty(CREDENTIALS_PROFILES_FILE, "");
        final String profileName = initProps.getProperty(CREDENTIALS_PROFILE_NAME, "");
        final String accessKey = initProps.getProperty(ACCESS_KEY, "");
        final String secretKey = initProps.getProperty(SECRET_KEY, "");

        // Get aws credentials based upon the options provided.
        AWSCredentialsProvider credProvider = getAWSCredentials(
                useJavaPropertyCreds, 
                useDefaultProfilesFile, 
                profilesFile, 
                profileName, 
                accessKey, 
                secretKey);
        // we'll use credProvider to connect to dynamo by default, unless STS overrides it
        AWSCredentialsProvider dynamoCredentials = credProvider;
        
        if(credentialsUseSts) {
            LOG.info(String.format("Property [%s] is set, assuming role: [%s]", STS_ROLE, stsRoleArn));
            // If we are being asked to assume a role using STS assume credentials provided should
            // be used to acquire STS role.
            
            AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(credProvider);

            AssumeRoleRequest stsAssumeRequest = new AssumeRoleRequest().withRoleArn(stsRoleArn);
                    
            // optional - set endpoint
            if(!stsEndpoint.isEmpty()) {
                stsClient.setEndpoint(stsEndpoint);
            }
            // optional - set token valid duration
            if(stsDuration != 0) {
                stsAssumeRequest.setDurationSeconds(stsDuration);
            }
            // set the session name
            stsAssumeRequest.setRoleSessionName(STS_ROLE_SESSION_NAME);
            
            AssumeRoleResult stsAssumeResult = stsClient.assumeRole(stsAssumeRequest);
            
            BasicSessionCredentials stsCredentials = new BasicSessionCredentials(
                        stsAssumeResult.getCredentials().getAccessKeyId(),
                        stsAssumeResult.getCredentials().getSecretAccessKey(),
                        stsAssumeResult.getCredentials().getSessionToken()
            );
            
            // override dynamoCredentials with the credentials provided by the now assumed role.
            dynamoCredentials = new AWSStaticCredentialsProvider(stsCredentials);
        }
        
        AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard();
        
        ClientConfiguration clientConfig = getAwsClientConfig();
        clientBuilder.setClientConfiguration(clientConfig);
        
        if(!dynamoEndpoint.isEmpty()) {
            clientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoEndpoint, dynamoRegionName));
        }
        else if(dynamoRegionName != null) {
            clientBuilder.setRegion(dynamoRegionName);
        }
        
        clientBuilder.setCredentials(dynamoCredentials);
        AmazonDynamoDB dynamo = clientBuilder.build();
     
        return dynamo;
    }
    

    /**
     * This method cannot be used with DynamoDataSource / DynamoEntityProcessor.  This is the default
     * method called from EntityProcessorBase, but dynamo needs a series of arguments, not a string 
     * to execute a query.
     * a query.
     * 
     * @param query
     * @return 
     */
    @Override
    public Iterator<Map<String, Object>> getData(String query) {
        wrapAndThrow(SEVERE, new Exception("This method signature getData(String) cannot be used, ensure you are using 'DynamoEntityProcessor'!"));
        return null;
    }
    
  /**
   * Get records for the given query.The return type depends on the
   * implementation .
   *
   * @param context 
   * @param tableName The name of the table we are querying
   * @param query  A dynamo QuerySpec object constructed in DynamoEntityProcessor
   * @return returns an Iterator<Map<String, Object>> mapping field-name to field-value.
   */
    
    public Iterator<Map<String, Object>> getData(Context context, String tableName, DynamoQueryParameters query) {
        // Check for the table, so a valuable error gets raised before we start iterating
        List<String> tableNames = new LinkedList<>();
        try {
             tableNames = dynamoClient.listTables().getTableNames();
        } catch (AmazonDynamoDBException e) {
            AmazonServiceException.ErrorType eType = e.getErrorType();
            if(e.getErrorCode().equals(ERROR_ACCESS_DENIED)) {
                LOG.debug("Permission denied to list tables, skipping this action.");
            } else {
                LOG.warn("Unexpectd error, trying to list tables... " + e.getMessage());
            }
        }
        
        if(!tableNames.isEmpty() && !tableNames.contains(tableName)) {
            List<String> shortened = tableNames.subList(0, Integer.min(tableNames.size() - 1, 20));
            wrapAndThrow(SEVERE, new Exception(String.format(
               "The dynamo table [%s] does not exist.  Valid tables: %s", tableName, shortened.toString()
            )));

            return new EmptyIterator<>();
        }
        
        Map<String, DynamoDBAttributeType> typeMap;
        if(explicitTypeMapping) {
            typeMap = getFieldTypeMapping(context);
            LOG.debug(String.format("Attribute [%s] is set, type map will be used, with %d map elements", CONVERT_FIELD_TYPES, typeMap.size()));
        } else {
            typeMap = new HashMap<>();
        }
        
        
        return new DynamoResultIterator<>(dynamoClient, tableName, query, typeMap);
    }

    
    /**
     * This sets up a mapping for the field, based on its name.
     * 
     * @param context the Entity context
     * @return 
     */
    protected Map<String, DynamoDBAttributeType> getFieldTypeMapping(Context context) {
    // This sets up a mapping for the field, based on its name.
        // Iterate through all enities that describe a column:
        //    

        // Create a mapping 
        Map<String, DynamoDBAttributeType> colNameToType = new HashMap<>();
        
        for (Map<String, String> map : context.getAllEntityFields()) {
            // name - the name (destination) of the Solr field
            String solrName = map.get(DataImporter.NAME);
            // column - "column" the (source) column from the query result
            String col = map.get(DataImporter.COLUMN);
            // type - "type" the string type of the data to convert to
            String colTypeStr = map.get(DataImporter.TYPE);

            //DynamoTypes: B, BOOL, BS, L, M, N, NS, NULL, S, SS
            DynamoDBAttributeType dynamoType = DynamoDBAttributeType.valueOf(colTypeStr);
            if(dynamoType != null) {
                colNameToType.put(col, dynamoType);
            } else {
                List<String> validTypesDesc = new LinkedList<String>();
                for (DynamoDBAttributeType theType : DynamoDBAttributeType.values()) {
                    validTypesDesc.add(theType.toString());
                }
                LOG.warn(String.format("entity field with name:[%s] invalid dynamo type: [%s], valid types are: %s", solrName, colTypeStr, validTypesDesc.toString()));
            }
        }
        
        return colNameToType;
    }
    
  /**
   * Cleans up resources of this DataSource after use.
   */
    @Override
    public void close() {
        LOG.debug("closing data handler");
    }
    
    
    /**
     * A templated concrete iterator class, that returns nothing
     * 
     * @param <T> Data return type
     */
    public class EmptyIterator<T> implements Iterator<T>  {
        @Override
        public boolean hasNext() {return false;}

        @Override
        public T next() {return null;}

        @Override
        public void remove() {}
    }
    
}
