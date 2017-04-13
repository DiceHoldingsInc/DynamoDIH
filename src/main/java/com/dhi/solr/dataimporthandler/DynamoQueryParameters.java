package com.dhi.solr.dataimporthandler;

import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

/**
 * Dynamo provides QueryExpressionSpec, but this, or ExpressionSpecBuilder does not provide
 * the ability to set a projection expression, filter expression, or key condition expression
 * as a raw string.
 * 
 * Note that QuerySpec provides:
 *  withProjectionExpression(String)
 *  withFilterExpression(String)
 *  withKeyConditionExpression(String)
 * 
 * In addition to not providing a way to set these strings explicitly, the QueryExpressionSpec
 * class is FINAL so we cannot override it to provide methods to set these values explicitly
 * similar to how QuerySpec works.
 * 
 * This class is designed to carry around values and options for querying, without needing to
 * construct a QuerySpec or ScanSpec object first.  This simplifies representing a query through
 * an object before you construct the Query/Scan Spec.
 * 
 * @author ben.demott
 */
public class DynamoQueryParameters {
    protected NameMap nameMap;
    protected ValueMap valueMap;
    protected String filterExpression;
    protected String projectionExpression;
    protected String keyConditionExpression;
    
    ///// SET /////////////////////////////////////////////
    public void setNameMap(NameMap nameMap) {
        if(nameMap != null && !nameMap.isEmpty()) {
            this.nameMap = nameMap;
        }
    }
    
    public void setValueMap(ValueMap valueMap) {
        if(valueMap != null && !valueMap.isEmpty()) {
            this.valueMap = valueMap;
        }
    }
     
    public void setFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
    }
    
    public void setProjectionExpression(String projectionExpression) {
        this.projectionExpression = projectionExpression;
    }
    
    public void setKeyConditionExpression(String keyConditionExpression) {
        this.keyConditionExpression = keyConditionExpression;
    }
    
    ///// GET ////////////////////////////////////////////
    public NameMap getNameMap() {
        return this.nameMap;
    }
    
    public ValueMap getValueMap() {
        return this.valueMap;
    }
     
    public String getFilterExpression() {
        return this.filterExpression;
    }
    
    public String getProjectionExpression() {
        return this.projectionExpression;
    }
    
    public String getKeyConditionExpression() {
        return this.keyConditionExpression;
    }
    
    @Override
    public String toString() {
        
        String nameMapDebug = null;
        if(nameMap != null) {
            nameMapDebug = nameMap.toString();
        }
        String valueMapDebug = null;
        if(valueMap != null) {
            valueMapDebug = valueMap.toString();
        }
        
        return String.format("Key Condition: %s"
                           + "%nFilter: %s"
                           + "%nProjection: %s"
                           + "%nName Map: %s"
                           + "%nValue Map: %s",
                            keyConditionExpression,
                            filterExpression,
                            projectionExpression,
                            nameMapDebug,
                            valueMapDebug);
    }
      
}
