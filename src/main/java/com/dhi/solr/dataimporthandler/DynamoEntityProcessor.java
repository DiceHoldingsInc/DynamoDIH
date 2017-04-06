/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dhi.solr.dataimporthandler;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.EntityProcessorWrapper;
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
    
    public static final String TABLE_NAME = "tableName";
    public static final String VALUE_MAP = "valueMap";
    public static final String NAME_MAP = "nameMap";
    public static final String CONDITIONAL_EXPRESSION = "keyConditionalExpression";
    public static final String FILTER_EXPRESSION = "filterExpression";
    public static final String PROJECTION_EXPRESSION = "projectionExpression";
    public static final String DELTA_NAME_ATTRIBUTE = "DELTA"; // fields ending with this value will be used for DELTA queries.
    public static final String NAME_ATTR_DELIMITER = ",";
    public static final String VALUE_TYPE_DELIMITER = ":";
    public static final String VALUE_ATTR_DELIMITER = ",";

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
        if(!dataSourceGeneric.getClass().isInstance(DynamoDataSource.class)) {
            LOG.error("datasource does not appear to be an instance of DynamoDataSource");
        }
        dataSource = (DynamoDataSource) dataSourceGeneric;
        String tableName = context.getResolvedEntityAttribute(TABLE_NAME);

        DynamoQueryParameters queryParams = getQueryExpression();
        
        rowIterator = dataSource.getData(context, tableName, queryParams);
    }
    
    /**
     * Know the list of allowable entity attributes and validate them
     * There are certain entity attributes that are required
     */
    protected void validateEntityAttributes() {
        // TODO!!! FINISH ME
        String entityName = TABLE_NAME;
        List<Map<String, String>> entityAttributes;
        String attrVal = context.getResolvedEntityAttribute(entityName);
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
     * @return 
     */
    @Override
    public Map<String, Object> nextRow() {
        /*
        context.replaceTokens(q) - replaces tokens in any string, that are variables such as 
        the data import handler last modified date.
        */
      if(rowIterator == null) {
          return null;
      }
      
      if(rowIterator.hasNext()) {
          try {
              return rowIterator.next();
          } catch (Exception e) {
              LOG.warn(String.format("DataImport iterator [%s] exception", rowIterator.getClass().getName()), e);
              wrapAndThrow(DataImportHandlerException.WARN, e);
              return null;
          }
          
      } else {
          rowIterator = null;
          return null;
      }
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
            
            nameMapField = nameMapField + DELTA_NAME_ATTRIBUTE;
            valueMapField = valueMapField + DELTA_NAME_ATTRIBUTE;
            conditionalExprField = conditionalExprField + DELTA_NAME_ATTRIBUTE;
            filterExprField = filterExprField + DELTA_NAME_ATTRIBUTE;
            projectionExprField = projectionExprField + DELTA_NAME_ATTRIBUTE;
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
        
        
        if(conditionalExpr != null) {
            LOG.debug(String.format("Using %s: %s", conditionalExprField, conditionalExpr));
            queryParams.setKeyConditionExpression(conditionalExpr);
        } else {
            LOG.debug(String.format("No key condition specified in entity attribute: [%s]", conditionalExprField));
        }
        
        if(filterExpr != null) {
            LOG.debug(String.format("Using %s: %s", filterExprField, filterExpr));
            queryParams.setKeyConditionExpression(filterExpr);
        } else {
            LOG.debug(String.format("No filter expression specified in entity attribute: [%s]", filterExprField));
        }
        
        if(projectionExpr != null) {
            LOG.debug(String.format("Using %s: %s", projectionExprField, projectionExpr));
            queryParams.setProjectionExpression(projectionExpr);
        } else {
            LOG.debug(String.format("No projection specified in entity attribute: [%s]", projectionExprField));
        }
        
        queryParams.setNameMap(getyQueryNameMap(nameMapField));
        queryParams.setValueMap(getQueryValueMap(valueMapField));
        
        return queryParams;
    }
    
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
        SortedMap<String, String> filtered = treeMap.tailMap(prefix);
        return filtered;
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
     * @return
     */
    protected NameMap getyQueryNameMap(String fieldPrefix) {
        NameMap nameMap = new NameMap();
        
        Map<String, String> attributes = getAllEntityAttributes();
        Map<String, String> nameAttributes = getPrefixedMapKeys(attributes, fieldPrefix);
        
        if(nameAttributes.isEmpty()) {
            LOG.debug(String.format("no NameMap fields configured (prefix: %s)", fieldPrefix));
            return null;
        }
        
        for (Map.Entry<String, String> entry : nameAttributes.entrySet()) {
            LOG.info("NameMap  Key = " + entry.getKey() + ", Value = " + entry.getValue()); // XXX
            
            String[] parts = entry.getValue().split(NAME_ATTR_DELIMITER, 1);
            
            if(parts.length != 2) {
                LOG.warn(String.format("NameMap attribute [%s] value [%s] is malformed, must contain 2 values delimited by: %s", 
                        entry.getKey(), 
                        entry.getValue(), 
                        NAME_ATTR_DELIMITER));
                continue;
            }
            
            // Key the string components to pass into NameMap, and insert 'replaceTokens' 
            // which inserts any solr variables referenced in the string.
            String placeHolder = context.replaceTokens(parts[0]).trim();
            String fieldName = context.replaceTokens(parts[1]).trim();
            
            nameMap.with(placeHolder, fieldName);
        }
        return nameMap;
    }
    
    
    /**
     * Return a mapping between solr and dynamo fields, only explicitly defined mappings in the 
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
            
            // Given the string "Int:field,value" return String[] ["Int", "field,value"]
            String[] typeAndFields = entry.getValue().split(VALUE_TYPE_DELIMITER, 1);
            
            if(typeAndFields.length != 2) {
                LOG.warn(String.format("ValueMap attribute [%s] value [%s] is malformed, must contain delimitor between type and field/value: '%s'", 
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_TYPE_DELIMITER));
                continue;
            }
            
            // Key the string components to pass into NameMap, and insert 'replaceTokens' 
            // which inserts any solr variables referenced in the string.
            String typeName = context.replaceTokens(typeAndFields[0]).trim().toLowerCase();
            String fields = context.replaceTokens(typeAndFields[1]).trim();
            
            // Ensure the typeName isn't empty or invalid
            if(typeName == null || typeName.isEmpty()) {
                LOG.warn(String.format("ValueMap attribute [%s] value [%s] does not contain a type, before '%s'", 
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_TYPE_DELIMITER));
                continue;
            }
            
            // Given the string "field,value" return String[] ["field", "value"]
            String[] fieldAndValue = fields.split(VALUE_ATTR_DELIMITER, 1);
            
            if(fieldAndValue.length != 2) {
                LOG.warn(String.format("ValueMap attribute [%s] value [%s] is malformed, must contain delimiter: '%s' between field and value", 
                        entry.getKey(), 
                        entry.getValue(), 
                        VALUE_ATTR_DELIMITER));
                continue;
            }
            
            String fieldName = context.replaceTokens(fieldAndValue[0]).trim();
            String fieldValue = context.replaceTokens(fieldAndValue[0]).trim();
            
            // When we split on the ':' we got rid of the field prefix, we must restore it.
            fieldName = VALUE_TYPE_DELIMITER + fieldName;
            
            // ValueMap expects strongly formed types to be added, based on what type-string the user
            // specified we will parse the value and add it to the ValueMap
            switch (typeName) {
                case "int":
                case "integer":
                    int intVal = Integer.parseInt(fieldValue);
                    valueMap.withInt(fieldName, intVal);
                    break;
                case "bool":
                case "boolean":
                    boolean boolVal = Boolean.parseBoolean(fieldValue);
                    valueMap.withBoolean(fieldName, boolVal);
                    break;
                case "float":
                case "decimal":
                case "number":
                case "double":
                    double doubleVal = Double.parseDouble(fieldValue);
                    valueMap.withNumber(fieldName, doubleVal);
                    break;
                case "string":
                    valueMap.withString(fieldName, fieldValue);
                    break;
                default:
                    LOG.warn(String.format("ValueMap attribute [%s] with value [%s] contains invalid type string: '%s'",
                            entry.getKey(),
                            entry.getValue(),
                            typeName));
                    continue;
            }
            
            LOG.debug(String.format("ValueMap type:%s field:%s value:%s added", typeName, fieldName, fieldValue));
        }
        return valueMap;
    }

    protected Map<String, Object> getNext() {
        return null;
    }
    
}