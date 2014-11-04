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

import java.util.Arrays;
import java.util.Iterator;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Value;

public class testkeysSort {
    private static KVStore kvstore = null;  
    public static void main(String[] args) throws Exception {
        kvstore = testUtil.getDebugKVstore();
        clearKeys();
        initStore();        
        Iterator<Key> it_keys = kvstore.multiGetKeysIterator(Direction.REVERSE, 1, 
                                Key.createKey(Arrays.asList("1","1")),new KeyRange("2",true,"2",true), Depth.PARENT_AND_DESCENDANTS);
        while (it_keys.hasNext()) {            
            Key key = it_keys.next();
            System.out.println("key="+key.toString());
        }
  }
    
    private static void initStore() {
         kvstore.put(Key.createKey(Arrays.asList("1","1"), Arrays.asList("1","2")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","1"), Arrays.asList("1","1")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","1"), Arrays.asList("2","100")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","1"), Arrays.asList("2","200")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","1"), Arrays.asList("3","4")), Value.EMPTY_VALUE);        
         kvstore.put(Key.createKey(Arrays.asList("1","2"), Arrays.asList("1","2")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","2"), Arrays.asList("3","4")), Value.EMPTY_VALUE);
         kvstore.put(Key.createKey(Arrays.asList("1","2"), Arrays.asList("5","6")), Value.EMPTY_VALUE);        
     }
    private static void clearKeys() {
        kvstore.multiDelete(Key.createKey(Arrays.asList("1","1")), null, Depth.PARENT_AND_DESCENDANTS);
        kvstore.multiDelete(Key.createKey(Arrays.asList("1","2")), null, Depth.PARENT_AND_DESCENDANTS);
    }
}
