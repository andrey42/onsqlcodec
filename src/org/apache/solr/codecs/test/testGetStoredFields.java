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

import org.apache.solr.codecs.onsql.ONSQLKVstoreHandler;
import org.apache.solr.codecs.onsql.ONSQLWrapperDirectory;

import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;

import java.util.Properties;

import oracle.kv.KVStore;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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

public class testGetStoredFields {
    private static final String INDEX_ROOT_FOLDER = "C:\\work\\search_engine\\codec\\index-dir";
    private static File plaintextDir;
    public static void main(String[] args) {
        try {
            testUtil.initPropsONSQL();
            plaintextDir = assureDirectoryExists(new File(INDEX_ROOT_FOLDER)); 
            getDoc("content", "fourth");
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
    public static void getDoc(String searchField, String searchString) throws IOException, ParseException {
                    
                    System.out.println("Searching for '" + searchString + "'");   
                    Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
                    IndexReader indexReader = DirectoryReader.open(luceneDir);                     
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);                
                    TotalHitCountCollector hitCountCollector = new  TotalHitCountCollector();
                    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_1);
                    QueryParser queryParser = new QueryParser(Version.LUCENE_4_10_1,searchField, analyzer);
                    Query query = queryParser.parse(searchString);               
                    indexSearcher.search(query,hitCountCollector);                    
                    System.out.println("Word: "+searchString+ "; Number of hits: " + hitCountCollector.getTotalHits());
                    System.out.println("maxdocs="+indexReader.maxDoc());
                    org.apache.lucene.search.TopDocs docs = indexSearcher.search(query, 100);
                    for (int i=0;i<docs.scoreDocs.length;i++) {
                      Document doc1 = indexReader.document(docs.scoreDocs[i].doc);
                      System.out.println("title="+doc1.get("title"));
                      System.out.println("content="+doc1.get("content"));
                      System.out.println("global_bu_id="+doc1.get("global_bu_id"));
                      System.out.println("omega_order_num="+doc1.get("omega_order_num"));
                      System.out.println("------");
                    }
                    luceneDir.close();

            }


}
