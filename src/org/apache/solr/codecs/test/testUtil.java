/**
 * Copyright Andrey Prokopenko
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.codecs.test;

import org.apache.solr.codecs.onsql.ONSQLKVstoreHandler;

import java.io.FileInputStream;

import java.util.Properties;

import oracle.kv.KVStore;

public class testUtil {
    public static final String TEST_CONFIG_DIR = "C:\\work\\search_engine\\codec\\solr410\\solr_codectest\\codec\\core.properties";
    public static  KVStore getDebugKVstore() throws Exception{        
        Properties props = new Properties();
        FileInputStream fstream = new FileInputStream(TEST_CONFIG_DIR);
        props.load(fstream);
         fstream.close();
        ONSQLKVstoreHandler.getInstance().setKVStore("omega",props);  
         return ONSQLKVstoreHandler.getInstance().getKVStore("omega");
            
    }
    public static  void initPropsONSQL() throws Exception{        
        Properties props = new Properties();
        FileInputStream fstream = new FileInputStream(TEST_CONFIG_DIR);
        props.load(fstream);
         fstream.close();
        ONSQLKVstoreHandler.getInstance().setKVStore("omega",props); 
    }
}
