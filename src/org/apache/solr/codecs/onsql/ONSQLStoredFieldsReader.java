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

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.RamUsageEstimator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ONSQLStoredFieldsReader extends StoredFieldsReader {
    private transient static Logger log = LoggerFactory.getLogger(ONSQLStoredFieldsReader.class);
    private int numDocsWritten = 0;  
    
    public final static String STORED_FIELDS_EXTENSION = "fld";
    private final static String TYPE_STRING = "S";
    private final static String TYPE_BINARY = "B";
    private final static String TYPE_INT = "I";
    private final static String TYPE_LONG = "L";
    private final static String TYPE_FLOAT = "F";
    private final static String TYPE_DOUBLE = "D";

    private KVStore kvstore = null;
    
    private final FSDirectory dir;
    private final String shardid_key_part;
    private  String segment_key_part; 
    private  ArrayList<String> doc_key = null;  

  private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(ONSQLStoredFieldsReader.class)
      + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)
      + RamUsageEstimator.shallowSizeOfInstance(CharsRef.class);
 
  private final FieldInfos fieldInfos;

  public ONSQLStoredFieldsReader(Directory dirIn, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
      
          if (dirIn instanceof TrackingDirectoryWrapper && ((TrackingDirectoryWrapper)dirIn).getDelegate() instanceof FSDirectory)
            this.dir = (FSDirectory)((TrackingDirectoryWrapper)dirIn).getDelegate();
          else if (dirIn instanceof FSDirectory)
              this.dir = (FSDirectory) dirIn;              
          else {
              throw new IllegalStateException("only file-based Directory is supported for now, this instance is "+dirIn.getClass().getName());
          }
       
      String tdir = Util.tidyIndexDir(dir.getDirectory().getAbsolutePath());
      log.info("fields reader tdir="+tdir);
      kvstore = ONSQLKVstoreHandler.getInstance().getKVStore(tdir);
      this.shardid_key_part = ONSQLKVstoreHandler.getInstance().getShardId(tdir);
      this.segment_key_part = Base62Converter.fromBase10(si.name.concat(STORED_FIELDS_EXTENSION).hashCode());
      this.fieldInfos = fn;
  }
  
  // used by clone
    ONSQLStoredFieldsReader(FSDirectory dirIn, String p_shardid_key, String p_segment_key, FieldInfos fn, KVStore kvstoreIn) throws IOException {
          this.kvstore = kvstoreIn;
          this.dir = dirIn;
          this.shardid_key_part = p_shardid_key;
          this.segment_key_part = p_segment_key;
          this.fieldInfos = fn;
      }
  
  @Override
  public void visitDocument(int doc_id, StoredFieldVisitor visitor) throws IOException {
    
    
    // first we retrieve our custom primary key
    Key key_to_get = Key.createKey(Arrays.asList(this.shardid_key_part,
                                                                                          this.segment_key_part,
                                        Base62Converter.fromBase10(doc_id)));
    Iterator<Key> segment_keys = kvstore.multiGetKeysIterator(Direction.FORWARD, 1, 
                                                              key_to_get,
                                                              null,
                                                              Depth.DESCENDANTS_ONLY);
    List<String> custom_pk = null;
    if (segment_keys.hasNext()) {
        Key segment_key = segment_keys.next();
        custom_pk = new ArrayList<String>(segment_key.getMinorPath());       
    }   
    if (custom_pk == null) throw new IllegalStateException("non-existing document id,search key used="+key_to_get.toString());
    // now let's get the main storage keys and values
    Iterator<KeyValueVersion> keyvals_iterator = kvstore.multiGetIterator(Direction.FORWARD,
                                         10, // batchsize
                                         Key.createKey(custom_pk) , // parentKey
                                         null, // subRange
                                         Depth.DESCENDANTS_ONLY
                                         ); //  depth      
    
    while (keyvals_iterator.hasNext()) {
      KeyValueVersion store_entry = keyvals_iterator.next();
      Key data_key = store_entry.getKey();
      // we need to get two last pieces of minor key parts, these will be field number and type
      List<String> minor_part = data_key.getMinorPath();
      String field_number = minor_part.get(0);
      // we have backrefs to segment keys present as a fields with negative field id, 
      // which is started as _ sign with out Base62Converter, we skip them as they are key-only fields, 
      // holding primary key for the segment as a minor key part, we skip them, 
      // as they are nesessary only for segment deletion time
      if (field_number.startsWith("_")) continue;
      String field_type = minor_part.get(1);
      FieldInfo currentfield_info = this.fieldInfos.fieldInfo(Base62Converter.toBase10(field_number));

      switch (visitor.needsField(currentfield_info)) {
        case YES:
          if (field_type.equals(TYPE_STRING)) visitor.stringField(currentfield_info, new String(store_entry.getValue().getValue(),StandardCharsets.UTF_8));
          else if (field_type.equals(TYPE_BINARY)) visitor.binaryField(currentfield_info, store_entry.getValue().getValue());
          else if (field_type.equals(TYPE_INT)) visitor.intField(currentfield_info,ByteBuffer.wrap(store_entry.getValue().getValue()).getInt());
          else if (field_type.equals(TYPE_LONG)) visitor.longField(currentfield_info,ByteBuffer.wrap(store_entry.getValue().getValue()).getLong());
          else if (field_type.equals(TYPE_FLOAT)) visitor.floatField(currentfield_info,ByteBuffer.wrap(store_entry.getValue().getValue()).getFloat());
          else if (field_type.equals(TYPE_DOUBLE)) visitor.doubleField(currentfield_info,ByteBuffer.wrap(store_entry.getValue().getValue()).getDouble());
          else throw new RuntimeException("unknown field type");
          break;
        case NO: continue;          
        case STOP: return;
      }
    }
  }
  
  private static int fromByteArray(byte[] bytes) {
         return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }
  

  @Override
  public StoredFieldsReader clone() {
    try {
        return new ONSQLStoredFieldsReader(
          this.dir, this.shardid_key_part, this.segment_key_part, this.fieldInfos,this.kvstore);   
    } 
    catch (IOException ex) {
        throw new RuntimeException(ex);
    }
   
  }
  
  @Override
  public void close() throws IOException {
  }
 
  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED;
  }

  @Override
  public void checkIntegrity() throws IOException {}
}
