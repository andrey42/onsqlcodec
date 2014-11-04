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

import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;

import java.util.Arrays;

import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;

import oracle.kv.KVStoreFactory;

import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
    private transient static Logger log = LoggerFactory.getLogger(Util.class);
    public static final String DEFAULT_ROOT_PREFIX = "lucene";
    private static KVStore kvstore = null;
    
    
    public static byte[] copyRange(BytesRef ref) {
        if(ref == null) {
            return null;
        }
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }
    
    public static String copyRangeAsUTF8String(BytesRef ref) {
        if(ref == null) {
            return null;
        }
        try {
         return  new String(Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length),"UTF-8");            
        }
        catch (UnsupportedEncodingException e) {
            return null;
        }
        
    }
    public static String tidyIndexDir(String in) {
        String out = in.replaceAll("[\\/\\\\]+", "/");
        if (out.endsWith("/")) out = out.substring(0,out.length()-1);        
        return out.toLowerCase();
    }
    public static void printStackTrace() {
        StackTraceElement[] cause = Thread.currentThread().getStackTrace();
        for (int i=0;i<cause.length;i++)
        log.info(cause[i].toString());
    }

}
