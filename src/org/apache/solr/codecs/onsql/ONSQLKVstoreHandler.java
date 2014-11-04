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

package org.apache.solr.codecs.onsql;

import java.util.HashMap;
import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import org.apache.solr.common.cloud.ClusterState;

public class ONSQLKVstoreHandler {
    public static final String KVSTORE_NAME_PROP = "kvstore.name";
    public static final String KVSTORE_HOSTS_PROP = "kvstore.hosts";
    public static final String KVSTORE_PRKEY_PROP = "kvstore.doc_primary_key_fields";
    
    static ONSQLKVstoreHandler instance;
    HashMap<String,KVStore> stores_map = null;  
    HashMap<String,String> stores_pkey_map = null;
    HashMap<String,Boolean> stores_awriting_map = null;
    HashMap<String,String> stores_shardids_map = null;
    private ONSQLKVstoreHandler() {    
        stores_map = new HashMap<String,KVStore>();
        stores_pkey_map = new HashMap<String,String>();
        stores_awriting_map = new HashMap<String,Boolean>();
        stores_shardids_map = new HashMap<String,String>();
    }
    public static ONSQLKVstoreHandler getInstance() {
        if (instance == null) instance = new ONSQLKVstoreHandler();    
       return instance;
    }
    public void setKVStore(String dir_key, Properties props) {
        if (props.getProperty(KVSTORE_NAME_PROP) == null) 
            throw new IllegalStateException("mandatory parameter "+KVSTORE_NAME_PROP+" with NoSQL store name is missing");
        if (props.getProperty(KVSTORE_HOSTS_PROP) == null) 
            throw new IllegalStateException("mandatory parameter "+KVSTORE_HOSTS_PROP+" with hosts list for NoSQL store is missing");
        if (props.getProperty(KVSTORE_PRKEY_PROP) == null)
            throw new IllegalStateException("mandatory parameter "+KVSTORE_PRKEY_PROP+" with fields list defining primary key for NoSQL storage is missing");
        KVStoreConfig kconfig = new KVStoreConfig(props.getProperty(KVSTORE_NAME_PROP), props.getProperty(KVSTORE_HOSTS_PROP).split(","));
        KVStore kvstore = KVStoreFactory.getStore(kconfig);
        if (kvstore != null) this.stores_map.put(dir_key, kvstore);
        else throw new IllegalStateException("KVStore handler is null, probably there was an error while initializing NOSQL store");        
        stores_pkey_map.put(dir_key, props.getProperty(KVSTORE_PRKEY_PROP));
    }
    
    public synchronized void setAllowWriting(String dir, boolean aw) {
        this.stores_awriting_map.put(dir, Boolean.valueOf(aw));
    }
    public boolean getAllowWriting(String dir) {
        Boolean val = this.stores_awriting_map.get(dir);
        if (val == null) return false;
        return val.booleanValue();
    }
    
    public synchronized void setShardId(String dir, String shardid) {
        this.stores_shardids_map.put(dir, shardid);
    }

    public String getShardId(String name) {
        return this.stores_shardids_map.get(name);
    }

    public KVStore getKVStore(String name) {
        return this.stores_map.get(name);
    }
   
    public String getDocPKFieldsList(String name) {
        return this.stores_pkey_map.get(name);
    }
    
    
}
