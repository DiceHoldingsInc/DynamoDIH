package com.dhi.solr.dataimporthandler;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.config.ConfigNameConstants.IMPORTER_NS;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.EntityProcessorWrapper;
import org.apache.solr.handler.dataimport.SolrWriter;
import org.apache.solr.handler.dataimport.VariableResolver;
import org.apache.solr.handler.dataimport.config.Entity;


/**
 * A custom entity processor that works with Solr's Data Import Handler (DIH) infrastructure.
 * 
 * This entity processor is designed to be used only with DynamoDataSource
 * 
 * Entity processors are designed to read the entity configuration, generate the query, and
 * send that query to the DataImportHandler.  The DataImportHandler then executes the query
 * and returns an iterator.  In our case we've implemented a custom iterator "DynamoResultIterator"
 * to make our implementation more transparent and easier to read.
 * 
 * @author ben.demott
 */
public class DynamoEntityProcessor extends EntityProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // We must use DynamoDataSource directly, instead of org.apache.solr.handler.dataimport.DataSource
    // because DataSource.getData() needs to accept a custom data-type.
    protected DynamoDataSource dataSource;
    protected DynamoQueryParameters queryParams;
    protected String primaryKeySolr;
    protected String primaryKeyDynamo;
    
    public static final String TABLE_NAME = "tableName";
    public static final String VALUE_MAP = "valueMap";
    public static final String NAME_MAP = "nameMap";
    public static final String CONDITIONAL_EXPRESSION = "keyConditionExpression";
    public static final String FILTER_EXPRESSION = "filterExpression";
    public static final String PROJECTION_EXPRESSION = "projectionExpression";
    public static final String DELTA_NAME_ATTRIBUTE = "DELTA"; // fields starting with this value will be used for DELTA queries.
    public static final String NAME_ATTR_DELIMITER = ",";
    public static final String VALUE_TYPE_DELIMITER = ":";
    public static final String VALUE_ATTR_DELIMITER = ",";
    
    public static final String VARIABLE_LAST_IMPORT = IMPORTER_NS + "." + SolrWriter.LAST_INDEX_KEY;
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // no constant elsewhere for this unfortunately
    
    public static final String VARIABLE_CUSTOM_NAMESPACE = IMPORTER_NS + ".dynamo";
    

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) {
        
        // Note that the call to super (EntityProcessorBase) sets up quite a few things
        // 
        // context      is set to the Context passed to it.
        // rowIterator  is set to be an Iterator<Map<String, Object>> (not initialized)
        // query        is set to a null String (we won't use this because our query isn't a string)
        // entityName   is set to the 'name' of this entity processor
        super.init(context);
        
        LOG.info(String.format("Initializing DIH entity: [%s]", entityName));
        
        DataSource dataSourceGeneric = context.getDataSource();
        dataSource = (DynamoDataSource) dataSourceGeneric;
        
        String tableName = context.getResolvedEntityAttribute(TABLE_NAME);
        
        // Build custom variables (used by the query expression)
        buildCustomVariables();

        queryParams = getQueryExpression();
        
        // Get the primary key
        EntityProcessorWrapper epc = (EntityProcessorWrapper) context.getEntityProcessor();
        primaryKeySolr = epc.getEntity().getPk();
        primaryKeyDynamo = getSolrDynamoFieldMapping().getOrDefault(primaryKeySolr, primaryKeySolr);
        
        rowIterator = dataSource.getData(context, tableName, queryParams);
        
        // VALIDATION
        validateEntityAttributes();
    }
    
    /**
     * Know the list of allowable entity attributes and validate them
     * There are certain entity attributes that are required
     */
    protected void validateEntityAttributes() {
        String errMsg = null;
        
        boolean hasConditionExpression = queryParams.getKeyConditionExpression() != null;
        boolean hasValueMap = queryParams.getValueMap() != null && !queryParams.getValueMap().isEmpty();

        if(hasConditionExpression && !hasValueMap) {
            errMsg = String.format("Dynamo DIH Error: %s is specified, a ValueMap must also be specified", CONDITIONAL_EXPRESSION);
        }
        
        if(context.getEntityAttribute(TABLE_NAME) == null || context.getEntityAttribute(TABLE_NAME).isEmpty()) {
            errMsg = String.format("Entity Attribute [%s] is required, and cannot be empty", TABLE_NAME);
        }
        
        if(context.currentProcess().equals(Context.DELTA_DUMP)) {
            String conditionalExprField = CONDITIONAL_EXPRESSION + DELTA_NAME_ATTRIBUTE;
            String filterExprField = FILTER_EXPRESSION + DELTA_NAME_ATTRIBUTE;
            Boolean hasConditionExpr = context.getEntityAttribute(conditionalExprField) != null && !context.getEntityAttribute(conditionalExprField).isEmpty();
            Boolean hasFilterExpr = context.getEntityAttribute(conditionalExprField) != null && !context.getEntityAttribute(conditionalExprField).isEmpty();
            if(!hasConditionExpr && hasFilterExpr) {
                LOG.warn(String.format("Dynamo DIH Delta Import is using [%s] only. "
                        + "A FULL TABLE SCAN will be performed; which will be more expensive. "
                        + "Consider using [%s] to use less capacity from DynamoDB!",
                        filterExprField, conditionalExprField));
                
            } else if (!hasConditionExpr && hasFilterExpr) {
                errMsg = String.format("Dynamo DIH Delta Import needs a query to retrieve a subset "
                        + "of documents. Please add [%s] and/or [%s] to the entity configuration "
                        + "to retrieve a subset of documents. Note that the Solr Variable: "
                        + "${%s} will contain the last import time for use "
                        + "in your ValueMap!", conditionalExprField, filterExprField, VARIABLE_LAST_IMPORT);
            }
        }
        
        if(errMsg != null) {
            LOG.warn(errMsg);
            wrapAndThrow(DataImportHandlerException.WARN, new Exception(errMsg));
        }
    }
    
    protected void buildCustomVariables() {
        context.getVariableResolver().resolve("dataimporter.last_index_time");
        VariableResolver resolve = context.getVariableResolver();
        String lastImportStr = (String) resolve.resolve(VARIABLE_LAST_IMPORT);

        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_DATE_FORMAT, Locale.ROOT);
        // By default will import everything since 5 minutes ago
        Date lastImportDate = new Date(System.currentTimeMillis() - 3600 * 5);
        try {
            lastImportDate = dateFormat.parse(lastImportStr);
        } catch (ParseException e) {
            // TODO, when this happens if the import is a DELTA IMPORT, we really can't continue
            // without knowing the LAST DATE.  We could support a url argument such as "deltaImportDate"
            // and tell the user to specify this in their query if they would like to run a DeltaImport
            // with an explicit delta date.
            LOG.warn("Unable to parse the last import date, Note: custom date formats are not supported.", e);
            return;
        }
        
        
        /*
            Add custom variables to the DataImportHandler
        
            addNamespace() - So how this works, is variable resolver holds multiple Map<> objects
            that contain variables about different 'namespaces. so for example 
            `dataimport` is a namespace.  This is how variables are set within the VariableResolver.
            You can update to, and append to existing namespaces, as long as the keys in your map
            are unique.
        */
        Long lastIndexMs = lastImportDate.getTime();
        Long lastIndexSec  = lastIndexMs / 1000;
        Map<String, Object> customVars = new HashMap<>();
        customVars.put("last_index_time_epoch_ms", lastIndexMs.toString());
        customVars.put("last_index_time_epoch_sec", lastIndexSec.toString());
        resolve.addNamespace(VARIABLE_CUSTOM_NAMESPACE, customVars);
        
        String indexEpochMs = (String) resolve.resolve(VARIABLE_CUSTOM_NAMESPACE + "." + "last_index_time_epoch_ms");
        if(indexEpochMs == null || indexEpochMs.isEmpty()) {
            LOG.warn("Failed to properly set custom variable: last_index_time_epoch_ms");
        } 
    }
    
    /**
     * Full Import, Gets next data row from DataSource using DataSource.getData()
     * 
     * This method helps streaming the data for each row . The implementation
     * would fetch as many rows as needed and gives one 'row' at a time. Only this
     * method is used during a full import
     *
     * @return A 'row'.  The 'key' for the map is the column name and the 'value'
     *         is the value of that column. If there are no more rows to be
     *         returned, return 'null'
     */
    @Override
    public Map<String, Object> nextRow() {
        if(rowIterator == null || !rowIterator.hasNext()) {
            rowIterator = null;
            return null;
        }
        
        return rowIterator.next();
    }

    
    @Override
    public Map<String, Object> nextModifiedRowKey() {
        if(rowIterator == null || !rowIterator.hasNext()) {
            rowIterator = null;
            return null;
        }
        
        return rowIterator.next();
    }
    
    @Override
    public Map<String, Object> nextDeletedRowKey() {
        // TODO, you must perform a set comparison in order to know what keys have been deleted.
        // this may simply not be worthwhile to support as you will need to do a full-table-scan
        return null;
    }


    /**
     * Construct a dynamo query.
     * We will use information from the entity configuration to construct the Dynamo Query.
     * 
     * @return A dynamo QuerySpec
     */
    protected DynamoQueryParameters getQueryExpression() {
        // TODO ... optionally if no projection expression is set, generate our own projection
        // expression to filter out uneeded (unmapped fields)
        
        DynamoQueryParameters queryParams = new DynamoQueryParameters();
        String currentProcessType = context.currentProcess();
        
        
        // Field names
        String nameMapField = NAME_MAP;
        String valueMapField = VALUE_MAP;
        String conditionalExprField = CONDITIONAL_EXPRESSION;
        String filterExprField = FILTER_EXPRESSION;
        String projectionExprField = PROJECTION_EXPRESSION;
        
        
        if (currentProcessType.equals(Context.FULL_DUMP)) {
            // any specifics for FULL_DUMP
        } else if (currentProcessType.equals(Context.DELTA_DUMP)) {
            // '${dataimporter.last_index_time}'
            // also you need to read "pk" attribute
            // This can be used to automatically determine the set difference between Dynamo and Solr
            // and perform a DELETE of removed documents
            
            nameMapField = DELTA_NAME_ATTRIBUTE + nameMapField;
            valueMapField = DELTA_NAME_ATTRIBUTE + valueMapField;
            conditionalExprField = DELTA_NAME_ATTRIBUTE + conditionalExprField;
            filterExprField = DELTA_NAME_ATTRIBUTE + filterExprField;
            projectionExprField = DELTA_NAME_ATTRIBUTE + projectionExprField;
        }
        
        /**
         * Get string filter, projection, and key expression from attributes specified in the 
         * <entity> element in the DataImportHandler configuration.
         * getResolvedEntityAttribute, automatically resolves variables specified within the 
         * attribute string for us.
         */
        String conditionalExpr = context.getResolvedEntityAttribute(conditionalExprField);
        String filterExpr = context.getResolvedEntityAttribute(filterExprField);
        String projectionExpr = context.getResolvedEntityAttribute(projectionExprField);
        
        
        if(conditionalExpr != null && !conditionalExpr.isEmpty()) {
            LOG.debug(String.format("Using %s: %s", conditionalExprField, conditionalExpr));
            queryParams.setKeyConditionExpression(conditionalExpr);
        } else {
            LOG.debug(String.format("No key condition specified in entity attribute: [%s]", conditionalExprField));
        }
        
        if(filterExpr != null && !filterExpr.isEmpty()) {
            LOG.debug(String.format("Using %s: %s", filterExprField, filterExpr));
            queryParams.setKeyConditionExpression(filterExpr);
        } else {
            LOG.debug(String.format("No filter expression specified in entity attribute: [%s]", filterExprField));
        }
        
        if(projectionExpr != null && !projectionExpr.isEmpty()) {
            LOG.debug(String.format("Using %s: %s", projectionExprField, projectionExpr));
            queryParams.setProjectionExpression(projectionExpr);
        } else {
            LOG.debug(String.format("No projection specified in entity attribute: [%s]", projectionExprField));
        }
        
        queryParams.setNameMap(getQueryNameMap(nameMapField));
        queryParams.setValueMap(getQueryValueMap(valueMapField));
        
        return queryParams;
    }
    
    /**
     * Retrieve all entity attributes that are specified, we need all possible entity attributes
     * to iterate through them to search for string prefixes matching different patterns.
     * 
     * @return returns a Map of attribute name to value pairs
     */
    protected Map<String,String> getAllEntityAttributes() {
        EntityProcessorWrapper epc = (EntityProcessorWrapper) context.getEntityProcessor();
        Entity entity = epc.getEntity();
        Map<String, String> attributes = entity.getAllAttributes();
        
        return attributes;
    }
    
    
    /**
     * Filters and returns a map based on a key prefix.  The map returned contains keys and values
     * from the input Map, but only keys that begin with 'prefix'
     * 
     * @param prefixMap A map that contains more keys than you need
     * @param prefix String prefix, this is the key that the Map key starts with that you'd like
     *         included in the returned Map.
     * @return 
     */
    protected Map<String, String> getPrefixedMapKeys(Map<String, String> prefixMap, String prefix) {
        TreeMap<String, String> treeMap = new TreeMap<>();
        treeMap.putAll(prefixMap);
        SortedMap<String, String> filtered = treeMap.subMap(prefix, prefix + Character.MAX_VALUE);
        return filtered;
    }
    
    
    /**
     * Return a mapping from dynamo to solr fields, only explicitly defined mappings in the 
     * entity configuration will be returned.
     * 
     * The Solr field name is referenced in the "name" attribute. and the dynamo field is referenced
     * in the "column" attribute of each <field> element within the entity configuration.
     * 
     * @return 
     */
    protected Map<String,String> getDynamoSolrFieldMapping() {

        Map<String, String> nameMap = new HashMap<>();        

        for (Map<String, String> map : context.getAllEntityFields()) {
            String dynamoField = map.get(DataImporter.COLUMN);
            String solrField = map.get(DataImporter.NAME);
            nameMap.put(dynamoField, solrField);
        }
        
        return nameMap;
    }
    
    /**
     * Return a mapping from solr to dynamo fields, only explicitly defined mappings in the
     * entity configuration will be returned.
     * 
     * @return 
     */
    protected Map<String, String> getSolrDynamoFieldMapping() {
        Map<String, String> map = getDynamoSolrFieldMapping();

        // Reverse the mapping
        Map<String, String> mapInversed = map.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        
        return mapInversed;
    }
    
    
    /**
     * Returns a NameMap that can be used to populate the nameMap of a dynamo query expression
     * 
     * The NameMap is parsed from xml configuration attributes in the entity config.
     * Any entity attribute that begins with the string (fieldPrefix) passed into this method
     * will be added to the NameMap.
     * 
     * This XML statement:
     *     <entity nameMapYear="#yr,year">
     * is equivelant to the statement:
     *     new NameMap().with("#yr",  "year")
     * 
     * Each attribute must be named uniquely, if you want more than one NameMap
     *    <entity nameMapYear="#start_yr,year" nameMapEnd="#end_yr,end">
     * This is the same as:
     *    new NameMap().with("#yr",  "year").with("#end_yr", "end")
     * 
     * @param fieldPrefix The map keys that begin with this string will be kept.
     * @return
     */
    protected NameMap getQueryNameMap(String fieldPrefix) {
        NameMap nameMap = new NameMap();
        
        // We want to get all parameters of the <entity> configuration that begin with `fieldPrefix`
        // So the first step is to get every single field=value pair from the entity configuration
        Map<String, String> attributes = getAllEntityAttributes();
        Map<String, String> nameAttributes = getPrefixedMapKeys(attributes, fieldPrefix);
        

        
        for (Map.Entry<String, String> entry : nameAttributes.entrySet()) {
            LOG.info("NameMap  Key = " + entry.getKey() + ", Value = " + entry.getValue()); // XXX
            
            String entryVal = entry.getValue().trim();
            int idxDelimiter = entryVal.indexOf(NAME_ATTR_DELIMITER);
            
            if(idxDelimiter == -1 || entryVal.length() -1 == idxDelimiter) {
                LOG.warn(String.format("NameMap attribute [%s] value [%s] is malformed, must contain 2 values delimited by: %s",
                        entry.getKey(), 
                        entry.getValue(), 
                        NAME_ATTR_DELIMITER));
                continue;
            }
            
            String placeHolder = entryVal.substring(0, idxDelimiter);
            String fieldName = entryVal.substring(idxDelimiter+1, entryVal.length());
            
            // Key the string components to pass into NameMap, and insert 'replaceTokens' 
            // which inserts any solr variables referenced in the string.
            placeHolder = context.replaceTokens(placeHolder).trim();
            fieldName = context.replaceTokens(fieldName).trim();

            nameMap.with(placeHolder, fieldName);
        }
        
        if(nameMap.isEmpty()) {
            LOG.debug(String.format("no NameMap fields configured (prefix: %s)", fieldPrefix));
            return null;
        } else {
            return nameMap;
        }
        
    }
    
    /**
     * Parse xml entity attributes to create a strongly-typed ValueMap to be used to inject values
     * into the dynamo query conditions.
     * 
     * An entity configuration such as:
     *     <entity valueMapVer="Int :ver, 4">
     * Becomes the statement:
     *     new ValueMap().withInt(":ver", 4)
     * 
     * An entity configuration such as:
     *     <entity valueMapUpdated="Bool :isupdated, true" valueMapScore="Float :score, 2.54"
     * Becomes the statement:
     *     new ValueMap().withBool(":isupdated", true).withNumber(":score", 5.1)
     * 
     * The part of the string that appears after the prefix "valueMap" is arbitrary, but is required
     * to give the attribute a unique name.  This also identifies the attribute uniquely in logs
     * and errors.
     * 
     * ---------------------------------------------------------------------------------------------
     * 
     * The fields referenced inside of ValueMap are arbitrary, they only mean something 
     * inside of the condition expression.
     * 
     * A ValueMap that uses the field-name ":yyyy"
     *     valueMap.put(":yyyy", 1985)
     * is referenced in a condition expression like so:
     *     withKeyConditionExpression("#yr = :yyyy")
     * 
     * The ValueMap field name can be anything, but must start with ":"
     * 
     * @param fieldPrefix
     * @return 
     */
    protected ValueMap getQueryValueMap(String fieldPrefix) {
        
        ValueMap valueMap = new ValueMap();
        Map<String, String> attributes = getAllEntityAttributes();
        Map<String, String> nameAttributes = getPrefixedMapKeys(attributes, fieldPrefix);
        
        for (Map.Entry<String, String> entry : nameAttributes.entrySet()) {
            LOG.info("ValueMap  Key = " + entry.getKey() + ", Value = " + entry.getValue()); // XXX
            
            // Given the string "Int:field,value" return position of ':'
            String entryVal = entry.getValue();
            int typeDelimIdx = entryVal.indexOf(VALUE_TYPE_DELIMITER);
            
            if(typeDelimIdx == -1 || entryVal.length() -1 == typeDelimIdx) {
                LOG.error(String.format("ValueMap attribute [%s] value [%s] is malformed, must contain delimitor between type and field/value: '%s'", 
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_TYPE_DELIMITER));
                continue;
            }
            
            // Given the string "Int:field,value" return 'Int'
            String typeName = entryVal.substring(0, typeDelimIdx);
            // Given the string "Int:field,value" return 'field,value'
            String fields = entryVal.substring(typeDelimIdx, entryVal.length());
            
            // replaceTokens, insert any solr variables if the string is templated.
            typeName = context.replaceTokens(typeName).trim().toLowerCase();
            
            fields = fields.trim();
            
            
            // Ensure the typeName isn't empty or invalid
            if(typeName == null || typeName.isEmpty()) {
                LOG.error(String.format("ValueMap attribute [%s] value [%s] does not contain a type, before '%s'", 
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_TYPE_DELIMITER));
                continue;
            }
            
            // Given the string "field,value" return return index of ','
            int fieldValueIdx = fields.indexOf(VALUE_ATTR_DELIMITER);
            
            if(fieldValueIdx == -1 || fields.length() -1 == fieldValueIdx) {
                LOG.error(String.format("ValueMap attribute [%s] value [%s] is malformed, must contain delimiter: '%s' between field and value",
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_ATTR_DELIMITER));
                continue;
            }
            
            // Given the string "field,value" return 'field'
            String fieldNameRaw = fields.substring(0, fieldValueIdx);
            // Given the string "field,value" return 'value'
            String fieldValueRaw = fields.substring(fieldValueIdx+1, fields.length());
            
            // Fill field and Value with solr variables if they are template strings
            String fieldName = context.replaceTokens(fieldNameRaw).trim();
            String fieldValue = context.replaceTokens(fieldValueRaw).trim();
            
            if(fieldValue == null || fieldValue.isEmpty()) {
                LOG.error(String.format("ValueMap attribute [%s] value [%s] is empty, Value before replaceTokens: [%s]",
                        entry.getKey(),
                        entry.getValue(),
                        fieldValueRaw));
                continue;
            }
            
            try {
                // ValueMap expects strongly formed types to be added, based on what type-string the user
                // specified we will parse the value and add it to the ValueMap
                switch (typeName) {
                    case "int":
                    case "integer":
                        int intVal = Integer.parseInt(fieldValue);
                        valueMap.withInt(fieldName, intVal);
                        break;
                    case "l":
                    case "long":
                        long longVal = Long.parseLong(fieldValue);
                        valueMap.withLong(fieldName, longVal);
                        break;
                    case "bool":
                    case "boolean":
                        boolean boolVal = Boolean.parseBoolean(fieldValue);
                        valueMap.withBoolean(fieldName, boolVal);
                        break;
                    case "n":
                    case "float":
                    case "decimal":
                    case "number":
                    case "double":
                        double doubleVal = Double.parseDouble(fieldValue);
                        valueMap.withNumber(fieldName, doubleVal);
                        break;
                    case "s":
                    case "string":
                        valueMap.withString(fieldName, fieldValue);
                        break;
                    default:
                        LOG.error(String.format("ValueMap attribute [%s] with value [%s] contains invalid type string: '%s'",
                                entry.getKey(),
                                entry.getValue(),
                                typeName));
                        continue;
                }
            } catch (Exception e) {
                LOG.error(String.format("ValueMap attribute [%s] with value [%s], parsing value [%s] exception: %s",
                        entry.getKey(),
                        entry.getValue(),
                        fieldValue,
                        e.getMessage()));
                continue;
            }

            
            LOG.debug(String.format("ValueMap type:%s field:%s value:%s added", typeName, fieldName, fieldValue));
        }
        return valueMap;
    }
}