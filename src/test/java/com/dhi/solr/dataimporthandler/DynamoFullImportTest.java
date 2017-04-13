/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dhi.solr.dataimporthandler;

import org.apache.solr.SolrTestCaseJ4;

import org.junit.Before;

/**
 * 
 * 
 * @author ben.demott
 */
public class DynamoFullImportTest extends SolrTestCaseJ4 {
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        File home = createTempDir("dih-properties").toFile();
        System.setProperty("solr.solr.home", home.getAbsolutePath());    
    }
}
