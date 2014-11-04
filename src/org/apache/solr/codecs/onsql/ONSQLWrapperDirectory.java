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

import java.io.File;
import java.io.IOException;

import java.nio.file.NoSuchFileException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import java.util.List;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;

import oracle.kv.KeyRange;

import org.apache.lucene.store.MMapDirectory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// here we pass through everything except deletion of the files
// in case fo delete we check whether segment  file extension belongs to store fields segment 
// and do deletion of the NOSQL content instead
// 
// need to figure out what to do in case of merge
// we have to keep mapping segment ID + doc ID => NOSQL doc primary key
// this mapping shoudl be preconfigurable
// for merge we have to recognise the merging context and do not delete actual docs until the end of merge procedure
// delete of actual stored fields entries for docs in NOSQL database should be done only if there are no entries
// left of given primary key for these docs
// deleted docs are marked on per-segment basic
// do not forget that Solr does not do the update, it deletes, then inserts new doc version
// unless we have all fields stored, we have to adhere to this option
// because there are no version info for individual fields, only for documetn itself
public class ONSQLWrapperDirectory extends MMapDirectory {
    private transient static Logger log = LoggerFactory.getLogger(ONSQLWrapperDirectory.class);
    KVStore kvstore;
    private ArrayList<String> dirSubkey = null;
    String tidydir = null;
    
    public ONSQLWrapperDirectory(File path) throws IOException {
      super(path, null);
      tidydir = Util.tidyIndexDir(path.getAbsolutePath());
      log.info("dir.tidydir="+tidydir);
    }

    
    @Override
    public void deleteFile(String nameIn) throws IOException, NoSuchFileException {
        log.debug("deleting file="+nameIn);
        //log.debug("nameIn.indexOf(\".si\")="+nameIn.indexOf(".si"));
        if (nameIn.indexOf(".si") > 0) {
            if (kvstore == null) {
                kvstore = ONSQLKVstoreHandler.getInstance().getKVStore(tidydir);
                this.dirSubkey = new ArrayList<String>();
                this.dirSubkey.add(ONSQLKVstoreHandler.getInstance().getShardId(tidydir));
                if (kvstore == null) throw new IllegalStateException("kvstore is null, error while deleting file="+nameIn);
            }    
            
            log.debug("we've got segment info file");
            ArrayList<String> fkey = new ArrayList<String>(this.dirSubkey);
            fkey.add(Base62Converter.fromBase10(nameIn.substring(0, 
                            nameIn.indexOf(".si")).concat(ONSQLStoredFieldsReader.STORED_FIELDS_EXTENSION).hashCode()));
            // we have major key defined for stored fields as dir ID + segment ID + doc id
            // so we have to use storeIterator to find & delete segment data, because it's a partial major key 
            Key dkey = Key.createKey(fkey);
            log.debug("deleting all descendants of the partial parent key="+dkey.toString());
            
            Iterator<Key> it_keys = kvstore.storeKeysIterator(Direction.UNORDERED, 10, dkey, null, Depth.PARENT_AND_DESCENDANTS);
            //    kvstore.multiGetKeysIterator(Direction.FORWARD, 1, dkey, null, Depth.PARENT_AND_DESCENDANTS);
            log.debug("got kvstore iterator");
            if (!it_keys.hasNext()){ 
                log.debug("entries for "+dkey.toString()+" do not exist in NOSQL");
                throw new NoSuchFileException("entries for "+dkey.toString()+" do not exist in NOSQL");
            } 
            while (it_keys.hasNext()) {
                Key entry_del_key = it_keys.next();
                log.debug("deleting entry="+entry_del_key.toString());
                kvstore.delete(entry_del_key);
                ArrayList<String> key_for_backref = new ArrayList<String>(entry_del_key.getMajorPath());                
                key_for_backref.add(0,"_1");
                // get custom pk
                ArrayList<String> custom_pk = new ArrayList<String>(entry_del_key.getMinorPath());               
                
                // delete backref from our custom key storage               
                Key backref_key = Key.createKey(custom_pk,// major part
                                            key_for_backref);// minor part, essentially a fields id set to -1 plus 
                                                  // full pk of the segment key plus doc id
                log.debug("backref key for deletion="+backref_key.toString());
                kvstore.delete(backref_key);
                
                // now let's retrieve the remaining backref, if they are available for our segment key
                // if not, we will delete the document completely
                Key custom_pk_key = Key.createKey(custom_pk);
                log.debug("checking for remaining backrefs for the key="+custom_pk_key.toString());
                Iterator<Key> custom_key_backrefs = kvstore.multiGetKeysIterator(Direction.FORWARD, 1,
                            custom_pk_key, 
                            new KeyRange("_",true,"_",true), Depth.DESCENDANTS_ONLY);
                if (!custom_key_backrefs.hasNext()) {
                    log.debug("none found, deleting key="+Key.createKey(custom_pk).toString());
                    kvstore.multiDelete(Key.createKey(custom_pk), null, Depth.PARENT_AND_DESCENDANTS);
                } else {
                    log.debug("some backref are still present, do not delete document");
                }
                
             }            
        }         
        super.deleteFile(nameIn);  // delegate deletion to original procedure        
    }
  
}

