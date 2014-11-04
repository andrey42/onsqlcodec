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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ONSQLStoredFieldsWriter extends StoredFieldsWriter {
  private transient static Logger log = LoggerFactory.getLogger(ONSQLStoredFieldsWriter.class);
  private int numDocsWritten = 0;
  
  private final static String STORED_FIELDS_EXTENSION = "fld";
  private final static String TYPE_STRING = "S";
  private final static String TYPE_BINARY = "B";
  private final static String TYPE_INT = "I";
  private final static String TYPE_LONG = "L";
  private final static String TYPE_FLOAT = "F";
  private final static String TYPE_DOUBLE = "D";  

  private KVStore kvstore = null;
  
  private final FSDirectory dir;
  private ArrayList<String> doc_key;
  private String tdir;
  private final String shardid_key_part;
  private  String segment_key_part;   
  private ArrayList<FieldEntry> doc_prep_cache = null;
  private LinkedHashMap<String,String> doc_kvstore_pkey = null;
  private boolean can_write_doc = true;

  
  public ONSQLStoredFieldsWriter(Directory dirIn, String segment, IOContext context) throws IOException {            
      if (dirIn instanceof TrackingDirectoryWrapper && ((TrackingDirectoryWrapper)dirIn).getDelegate() instanceof FSDirectory)
      this.dir = (FSDirectory)((TrackingDirectoryWrapper)dirIn).getDelegate();
    else if (dirIn instanceof FSDirectory) this.dir = (FSDirectory) dirIn;    
    else {
        throw new IllegalStateException("only file-based Directory is supported for now, this instance is "+((TrackingDirectoryWrapper)dirIn).getDelegate().getClass().getName());
    }
      this.tdir = Util.tidyIndexDir(dir.getDirectory().getAbsolutePath());
      log.info("fields writer tdir="+tdir);
      kvstore = ONSQLKVstoreHandler.getInstance().getKVStore(tdir);
      if (ONSQLKVstoreHandler.getInstance().getDocPKFieldsList(tdir) == null) throw new IllegalStateException("can't get nosql store primary key field names list");
      String[] arr = ONSQLKVstoreHandler.getInstance().getDocPKFieldsList(tdir).split(",");
      doc_kvstore_pkey = new LinkedHashMap<String,String>();
      for (int i=0;i<arr.length;i++) doc_kvstore_pkey.put(arr[i], null);      

      
    this.shardid_key_part = ONSQLKVstoreHandler.getInstance().getShardId(tdir);
    log.debug("dir path="+dir.getDirectory().getAbsolutePath());
    log.debug("path hashcode="+dir.getDirectory().getAbsolutePath().hashCode());
    log.debug("shardid_key_part="+this.shardid_key_part);
    this.segment_key_part = Base62Converter.fromBase10(segment.concat(STORED_FIELDS_EXTENSION).hashCode());
    log.debug("fieldswriter segment_key_part="+this.segment_key_part);
  }
  
  @Override
  public void startDocument() throws IOException {   
    this.can_write_doc = ONSQLKVstoreHandler.getInstance().getAllowWriting(this.tdir);
    if (can_write_doc) {
    this.doc_key = new ArrayList<String>();
    doc_key.add(this.shardid_key_part);
    doc_key.add(this.segment_key_part);
    doc_key.add(Base62Converter.fromBase10(numDocsWritten));    
    doc_prep_cache = new ArrayList<FieldEntry>();
    }
    numDocsWritten++;
  }

  @Override
  public void writeField(FieldInfo info, IndexableField field) throws IOException {
      if (!can_write_doc) return;
      final String type;
      final Value value;
      if(field.numericValue() != null) {          
          final Number n = field.numericValue();
          if(n instanceof Byte || n instanceof Short || n instanceof Integer) {
              type = TYPE_INT;
              value =  Value.createValue(ByteBuffer.allocate(4).putInt(n.intValue()).array());
              if (doc_kvstore_pkey.containsKey(info.name)) doc_kvstore_pkey.put(info.name, SortableNumbers.intToPrefixCoded(n.intValue())
                                                                                );
          } else if(n instanceof Long) {
              type = TYPE_LONG;
              value = Value.createValue(ByteBuffer.allocate(8).putLong(n.longValue()).array());
              if (doc_kvstore_pkey.containsKey(info.name)) doc_kvstore_pkey.put(info.name, SortableNumbers.longToPrefixCoded(n.longValue())
                                                                                );
          } else if(n instanceof Float) {
              type = TYPE_FLOAT;
              value =  Value.createValue(ByteBuffer.allocate(4).putFloat(n.floatValue()).array());
              if (doc_kvstore_pkey.containsKey(info.name)) doc_kvstore_pkey.put(info.name, SortableNumbers.floatToPrefixCoded(n.floatValue())
                                                                                );        
          } else if(n instanceof Double) {
              type = TYPE_DOUBLE;
              value = Value.createValue(ByteBuffer.allocate(8).putDouble(n.doubleValue()).array());
              if (doc_kvstore_pkey.containsKey(info.name)) doc_kvstore_pkey.put(info.name, SortableNumbers.doubleToPrefixCoded(n.doubleValue())
                                                                                );
          } else {
              throw new IllegalArgumentException("cannot store numeric type " + n.getClass());
          }
      } else if(field.binaryValue() != null) {
          type = TYPE_BINARY;
          BytesRef bytesRef = field.binaryValue();
          value =  Value.createValue(Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length));
          if (doc_kvstore_pkey.containsKey(info.name)) throw new IllegalArgumentException("field "+info.name+" assigned as a primary key cannot be of binary type");
      } else if(field.stringValue() != null) {
          type = TYPE_STRING;
          value = Value.createValue(field.stringValue().getBytes());
          if (doc_kvstore_pkey.containsKey(info.name)) doc_kvstore_pkey.put(info.name,
                                                                            field.stringValue()
                                                                            );
      } else {
          throw new IllegalArgumentException(
                  String.format(
                          "Unsupported type for IndexableField %s: %s",
                          field.name(),
                          field.fieldType().docValueType()
                  )
          );
      }
      
    doc_prep_cache.add(new FieldEntry(Base62Converter.fromBase10(info.number),type,value));
    
    /*  
    kvstore.put(Key.createKey(doc_key, 
                              Arrays.asList(Base62Converter.fromBase10(info.number), // field number
                                            type // field type code 
                                            )),
                value);
    */
      
  }
  
  @Override  
  public void finishDocument() throws IOException {  
      if (!can_write_doc) return;  
      ArrayList<String> doc_custom_key_buf = new ArrayList<String>();
      for (Map.Entry<String,String> entry : doc_kvstore_pkey.entrySet()) {
          if (entry.getValue() == null) throw new IllegalStateException("field "+entry.getKey()+" in primary key is null, key is not complete, can't save the document into kvstore");
          doc_custom_key_buf.add(entry.getValue());
      }
      
      for (FieldEntry fentry: doc_prep_cache) {
          kvstore.put(Key.createKey(doc_custom_key_buf, 
                                    Arrays.asList(fentry.getFieldid(),fentry.getType())), fentry.getValue());          
      }
      
      // we have also to store our segment backref, to identify link back to the segment
      // this backref has to be deleted on segment deletion, and if there are no segment backrefs left
      // we delete fields entryset 
      
      // put the main key for the segment
     
      kvstore.put(Key.createKey(doc_key, doc_custom_key_buf), Value.EMPTY_VALUE);
      
      
      //put backref to the primary key of the segment, we will delete it, when segment will be deleted
      // since we have field number coming before field type, we need to assign field number -1 to all the backrefs to distingush among them.
      doc_key.add(0, Base62Converter.fromBase10(-1));          
      kvstore.put(Key.createKey(doc_custom_key_buf, doc_key), Value.EMPTY_VALUE);
      
      // cleanup of our kvstore pkey for newt document   
      for (String mkey:doc_kvstore_pkey.keySet()) doc_kvstore_pkey.put(mkey, null);       
  } 

  @Override
  public void abort() {
    try {
            Iterator<KeyValueVersion> iterator = kvstore.storeIterator
                (Direction.UNORDERED, 0, Key.createKey(Arrays.asList(this.shardid_key_part,this.segment_key_part)), null, null);
            while (iterator.hasNext()) {
                KeyValueVersion kvv = iterator.next();
                kvstore.delete(kvv.getKey());
            }
           
        } catch (FaultException e) {
            /*
             * TODO: develop functionality to mark segment as a non-ready to fight connection failures with store 
             */
            log.debug("error while deleting entire segment "+this.segment_key_part+" with shardid="+this.shardid_key_part+"; some parts of segment may remain in the store!"); 
        }
  }

  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (numDocsWritten != numDocs) {
      throw new RuntimeException("mergeFields produced an invalid result: docCount is " + numDocs 
          + " but only saw " + numDocsWritten + "; now aborting this merge to prevent index corruption");
    }   
  }

  @Override
  public void close() throws IOException {
     
  }
  
  @Override
  public int merge(MergeState mergeState) throws IOException {
      log.debug("merge has been called");      
      // check for our primary key completeness
      /*
       * instead of copying stored fields we need to update mapping table and remove non-existing entries   
       * several cases: doc may be deleted and non existing in new segment
       * doc may be deleted, yet similar doc might be found
       * thing is, these fields might be updated, so using linkup is not so productive
       * because in fact there can be two diffirent versions of documents present after the update
       * one, old version, deleted now, and one new version 
       * hmmm, since we're using custom primary key, it means on the update entry with our fields will be overwritten, 
       * also, worth considering, that merge procedure only creates new segment based on previous ones, 
       * deletion of old segments is happening later, via Directory.deletefile API
       * have to check on this.
       * 
       * first, since it's merge, we assume entries in kvstore already existing
       * so, first we search for our segment key: segID-docID->customPK, then copy it
       * all other fields will be left unchanged
       * 
       * have to consider the failure case, when merge might be aborted in the middle of it
       * since merge first copies data to new segment, we are safe here, as in worst case we will lose just the links
       * for this new segment
       * */
      
      int docCount = 0;
      int idx = 0;
      String new_segment_kvstore_key_part =
            Base62Converter.fromBase10(mergeState.segmentInfo.name.concat(STORED_FIELDS_EXTENSION).hashCode());      
      for (AtomicReader reader : mergeState.readers) {
        final SegmentReader seg_reader = mergeState.matchingSegmentReaders[idx++];
        ONSQLStoredFieldsReader fields_reader = null;
        if (seg_reader != null) {
          final StoredFieldsReader fieldsReader = seg_reader.getFieldsReader();
          // we can do the merge only if the matching reader is also a ONSQLStoredFieldsReader
          if (fieldsReader != null && fieldsReader instanceof ONSQLStoredFieldsReader) {
            fields_reader = (ONSQLStoredFieldsReader) fieldsReader;
          }
          else throw new IllegalStateException("incorrect fieldsreader class at merge procedure, is "
                         +fieldsReader.getClass().getName() + ", only ONSQLStoredFieldsReader is accepted");
        }
          String current_segment =  seg_reader.getSegmentName().concat(STORED_FIELDS_EXTENSION);
          log.debug("current segment name = "+seg_reader.getSegmentName());
          // we assume reader always uses the instance of FSDirectory, so that we can extract directory path
          String dir = ((FSDirectory) seg_reader.directory()).getDirectory().getAbsolutePath();
          final int maxDoc = reader.maxDoc();
          final Bits liveDocs = reader.getLiveDocs();
          boolean canmerge = ONSQLKVstoreHandler.getInstance().getAllowWriting(this.tdir);
          for (int i = nextLiveDoc(0, liveDocs, maxDoc); i < maxDoc; i = nextLiveDoc(i + 1, liveDocs, maxDoc)) {
                ++docCount;
              if (canmerge) {
              // retrieve link using our doc id
              Key doc_key = Key.createKey(Arrays.asList(Base62Converter.fromBase10(dir.hashCode()),
                                                    Base62Converter.fromBase10(current_segment.hashCode()),
                                                    Base62Converter.fromBase10(i)
                                                                 ));
              Iterator<Key> kv_it =  kvstore.multiGetKeysIterator(Direction.FORWARD, 1, doc_key,
                                     null, Depth.PARENT_AND_DESCENDANTS);
              if (!kv_it.hasNext())
                  throw new IllegalStateException("unable to get doc segment key using key id="+doc_key.toString());
              Key entry_key =  kv_it.next();
              // create link to doc id for new segment 
              Key link_key = Key.createKey(
                                   Arrays.asList(Base62Converter.fromBase10(dir.hashCode()),
                                   new_segment_kvstore_key_part,
                                                    Base62Converter.fromBase10(numDocsWritten))
                                   , entry_key.getMinorPath());
              log.debug("putting link key="+link_key.toString());
              kvstore.put(link_key, Value.EMPTY_VALUE);
              // next add backref
              Key backref_key = Key.createKey(entry_key.getMinorPath(),                                   
                                Arrays.asList("_1", Base62Converter.fromBase10(dir.hashCode()),
                                   new_segment_kvstore_key_part,
                                                    Base62Converter.fromBase10(numDocsWritten)));
              kvstore.put(backref_key, Value.EMPTY_VALUE);
              log.debug("putting backref key="+backref_key.toString());
              //addDocument(doc, mergeState.fieldInfos);
              } else log.debug("merging is not allowed, skipping doc with internal id="+i);
              ++numDocsWritten;
              mergeState.checkAbort.work(300);
            }

      }

      finish(mergeState.fieldInfos, docCount);
      return docCount;
  }
  
    private static int nextLiveDoc(int doc, Bits liveDocs, int maxDoc) {
      if (liveDocs == null) {
        return doc;
      }
      while (doc < maxDoc && !liveDocs.get(doc)) {
        ++doc;
      }
      return doc;
    }
  
}
