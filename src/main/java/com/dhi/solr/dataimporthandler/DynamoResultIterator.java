/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dhi.solr.dataimporthandler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import java.util.Iterator;
import java.util.Map;

/**
 * Used by DynamoDataSource to present an iterator from the getData method
 * 
 * 
 * @author ben.demott
 */
public class DynamoResultIterator<T> implements Iterator<T> {
    DynamoDB dynamoDB;
    Table dynamoTable;
    QuerySpec dynamoQuery;
    ScanSpec dynamoScan;
    Iterator<Item> dynamoIter;
    Map<String, DynamoDBAttributeType> fieldToType;
    
    
    public DynamoResultIterator(AmazonDynamoDB dynamoClient, String tableName, DynamoQueryParameters queryParams, Map<String, DynamoDBAttributeType> dataTypeMap) {
        super();
        fieldToType = dataTypeMap;
        
        dynamoDB = new DynamoDB(dynamoClient);
        dynamoTable = dynamoDB.getTable(tableName);
        
        // If there isn't a query condition we should do a scan.
        if(hasConditionExpression(queryParams)) {
            dynamoQuery = new QuerySpec()
                    .withProjectionExpression(queryParams.getProjectionExpression())
                    .withKeyConditionExpression(queryParams.getKeyConditionExpression())
                    .withFilterExpression(queryParams.getFilterExpression())
                    .withNameMap(queryParams.getNameMap())
                    .withValueMap(queryParams.getValueMap());
            

            ItemCollection<QueryOutcome> items = dynamoTable.query(dynamoQuery);
            dynamoIter = items.iterator();
        } else {
            dynamoScan = new ScanSpec()
                    .withProjectionExpression(queryParams.getProjectionExpression());
            ItemCollection<ScanOutcome> items = dynamoTable.scan(dynamoScan);
            dynamoIter = items.iterator();
        }
    }
    
    private boolean hasConditionExpression(DynamoQueryParameters query) {
        boolean noCondition = query.getKeyConditionExpression() == null;
        boolean noFilter = query.getFilterExpression() == null;
        
        return noCondition && noFilter;
    }
    
    @Override
    public boolean hasNext() {
         return dynamoIter.hasNext();
    }
    
    @Override
    public T next() {
        //get the next value
        // TODO need to make use of dataTypeMap
        Item itemData = null;
        itemData = dynamoIter.next();
        return (T) itemData.asMap();
    }

    @Override
    public void remove() {
        // do nothing.
    }
}

