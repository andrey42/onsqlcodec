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

import org.apache.solr.codecs.onsql.ONSQLCodec;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class testONSQLCodec {
    private static final String INDEX_ROOT_FOLDER = "C:\\work\\search_engine\\codec\\index-dir";
    private static File plaintextDir;

    public static void main(String[] args) {
        try {
            plaintextDir = assureDirectoryExists(new File(INDEX_ROOT_FOLDER));
            testUtil.initPropsONSQL();
            //----------- index documents -------
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_1);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
            // recreate the index on each execution
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            //config.setCodec(new SimpleTextCodec());            
            ONSQLCodec codec = new ONSQLCodec();
            config.setCodec(codec);
            config.setUseCompoundFile(false);
            Directory luceneDir = FSDirectory.open(plaintextDir);
            IndexWriter writer = new IndexWriter(luceneDir, config);
            writer.addDocument(Arrays.asList(new TextField("title", "The title of my first document", Store.YES),
                                             new TextField("content", "The content of the first document", Store.YES),
                                             new IntField("intval", 111111, Store.YES),
                                             new LongField("longval",1111111111L, Store.YES)
                                             ));

            writer.addDocument(Arrays.asList(new TextField("title", "The tAtle of the second document", Store.YES),
                                             new TextField("content", "The content of the second document",Store.YES),
                                             new IntField("intval", 222222, Store.YES),
                                             new LongField("longval",222222222L, Store.YES)
                                             ));
            writer.addDocument(Arrays.asList(new TextField("title", "The title of the third document", Store.YES),
                                             new TextField("content", "The content of the third document", Store.YES),
                                             new IntField("intval", 333333, Store.YES),
                                             new LongField("longval",3333333333L, Store.YES)                                             
                                             ));
            writer.commit();
            writer.close();
            IndexReader reader = DirectoryReader.open(luceneDir);             
            // now test for docs
            if (reader.numDocs() < 3)
                throw new IOException("amount of returned docs are less than indexed");
            else
                System.out.println("test passed");
            searchIndex("content", "third");
        } catch (Throwable te) {
            te.printStackTrace();
        }
    }


    private static File assureDirectoryExists(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
    
    public static void searchIndex(String searchField, String searchString) throws IOException, ParseException {
                    System.out.println("Searching for '" + searchString + "'");   
                    Directory luceneDir = FSDirectory.open(plaintextDir);
                    IndexReader indexReader = DirectoryReader.open(luceneDir); 
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    TotalHitCountCollector hitCountCollector = new  TotalHitCountCollector();
                    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_1);
                    QueryParser queryParser = new QueryParser(Version.LUCENE_4_10_1,searchField, analyzer);
                    Query query = queryParser.parse(searchString);
                    indexSearcher.search(query,hitCountCollector);
                    luceneDir.close();
                    
                    System.out.println("Word: "+searchString+ "; Number of hits: " + hitCountCollector.getTotalHits());

                   

            }


}
