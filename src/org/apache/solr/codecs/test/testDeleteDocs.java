
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

import java.util.Arrays;

import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Field.Store;
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

public class testDeleteDocs {
    private static final String INDEX_ROOT_FOLDER = "C:\\work\\search_engine\\codec\\index-dir";
    private static File plaintextDir;

    public static void main(String[] args) {
        try {
            plaintextDir = assureDirectoryExists(new File(INDEX_ROOT_FOLDER));
           
            //----------- index documents -------
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer);
            // recreate the index on each execution
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            //config.setCodec(new SimpleTextCodec()); 
            
            Properties props = new Properties();
            FileInputStream fstream = new FileInputStream("C:\\work\\search_engine\\codec\\solr410\\solr_codectest\\collection1\\conf\\kvstore.properties");
            props.load(fstream);
            fstream.close();
            ONSQLKVstoreHandler.getInstance().setKVStore("omega",props);
            ONSQLCodec codec = new ONSQLCodec();
            config.setCodec(codec);          
            config.setUseCompoundFile(false);
            Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
            IndexWriter writer = new IndexWriter(luceneDir, config);
            QueryParser queryParser = new QueryParser(Version.LUCENE_4_10_0,"title", analyzer);
            String search_word = "fourth";
            Query query = queryParser.parse(search_word);
            writer.deleteDocuments(query);
            writer.commit();
            writer.close();
            searchIndex("title", search_word);
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
                    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
                    QueryParser queryParser = new QueryParser(Version.LUCENE_4_10_0,searchField, analyzer);
                    Query query = queryParser.parse(searchString);
                    indexSearcher.search(query,hitCountCollector);
                    luceneDir.close();
                    
                    System.out.println("Word: "+searchString+ "; Number of hits: " + hitCountCollector.getTotalHits());

                   

            }


}
