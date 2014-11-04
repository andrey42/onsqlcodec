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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.common.cloud.Replica;


import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CodecFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.SolrCoreAware;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class ONSQLCodecFactory extends CodecFactory implements SolrCoreAware { 
  private ONSQLCodec onsqlcodec;
  private static Logger log = LoggerFactory.getLogger(ONSQLCodecFactory.class);

  @Override
  public void init(NamedList args) {
    super.init(args);
    onsqlcodec =  new ONSQLCodec();     
  }
  
    @Override
    public void inform(SolrCore core) {
        log.info("=>>>>>>>>>>>>>codecfactory inform was called");        
        final String dir = Util.tidyIndexDir(core.getIndexDir());       
        log.info("inform.dir="+dir);
        //Util.printStackTrace();
        final CloudDescriptor cldesc = core.getCoreDescriptor().getCloudDescriptor();         
        if (cldesc != null) {
        log.info("cloud config available");
            ONSQLKVstoreHandler.getInstance().setAllowWriting(dir,cldesc.isLeader());
            ONSQLKVstoreHandler.getInstance().setShardId(dir,cldesc.getShardId());        
            final SolrZkClient zkclient  = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
            ZkSolrResourceLoader loader = (ZkSolrResourceLoader) core.getResourceLoader();
            String zkconfigpath = loader.getCollectionZkPath();
            try {
                // load NoSQL config from ZKregistry 
                byte[] content = zkclient.getData(zkconfigpath+"/kvstore.properties", null, null, true);
                ByteArrayInputStream is = new ByteArrayInputStream(content);
                Properties kvstore_props = new Properties();
                kvstore_props.load(is);
                is.close();
                ONSQLKVstoreHandler.getInstance().setKVStore(dir, kvstore_props);
                // hook watcher to the cluster update
                zkclient.exists(ZkStateReader.CLUSTER_STATE, new Watcher() {

                    @Override
                    public void process(WatchedEvent event) {
                        if (EventType.None.equals(event.getType()))
                            return;
                        log.info("got event type=" + event.getType());
                        try {                            
                            final Watcher thisWatch = this;
                            zkclient.exists(ZkStateReader.CLUSTER_STATE, thisWatch,true);                            
                            //clstate.getReplica(arg0, arg1)
                            ONSQLKVstoreHandler.getInstance().setAllowWriting(dir,cldesc.isLeader());
                        } catch (KeeperException e) {
                            if (e.code() == KeeperException.Code.SESSIONEXPIRED ||
                                e.code() == KeeperException.Code.CONNECTIONLOSS) {
                                log.warn("we have been disconnected from registry");
                                // TODO add code for stopping all the jobs in case it was a network failure in the cluster
                                return;
                            }
                            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
                        } catch (InterruptedException e) {
                            log.error(" we have been interrupted", e);
                            // Restore the interrupted status
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                }, true);
        } catch (IOException e) {
            log.error("error while reading key-value store properties",e);
            throw new IllegalStateException("error while reading key-value store properties");
        }
        catch (KeeperException e) {
                log.error("we have been disconnected from Zookeeper registry", e);
                throw new IllegalStateException("error while reading key-value store properties");
        } catch (InterruptedException e) {
            log.error(" we have been interrupted", e);
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        }
        }
        else {
            log.info("no cloud available, using local configs from filesystem, collection name="+core.getName());
            ONSQLKVstoreHandler.getInstance().setAllowWriting(dir,false);
            ONSQLKVstoreHandler.getInstance().setShardId(dir,"shard1");
            Properties props = new Properties();    
            try {
            FileInputStream propstream = new FileInputStream(core.getResourceLoader().getConfigDir().concat("/kvstore.properties"));
            props.load(propstream);
            propstream.close();
            }
            catch (FileNotFoundException fnex) {
                throw new IllegalStateException("kvstore.properties file is missing or non-readable in core config directory");
            }
            catch (IOException fnex) {
                throw new IllegalStateException("kvstore.properties file is missing or non-readable in core config directory");
            }
              ONSQLKVstoreHandler.getInstance().setKVStore(dir,props);

        }
  
    }
    
  @Override 
  public Codec getCodec() { 
   return onsqlcodec; 
  } 
} 