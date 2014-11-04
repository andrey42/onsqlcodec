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
import org.apache.solr.codecs.onsql.ONSQLWrapperDirectory;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
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
import org.apache.lucene.util.Version;

public class testONSQLWrapperDirectory {
    public static final String INDEX_ROOT_FOLDER = "C:\\work\\search_engine\\codec\\index-dir";
    public static void main(String[] args) {
        try {
            testUtil.initPropsONSQL();
            //----------- index documents -------
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_1);
            //Codec cd = new ONSQLCodec("omega");
            //Codec.setDefault(cd);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
            // recreate the index on each execution
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            //config.setCodec(new SimpleTextCodec());            
            ONSQLCodec codec = new ONSQLCodec();
            config.setCodec(codec);
            config.setUseCompoundFile(false);
            Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
            String[] list = luceneDir.listAll();
            System.out.println("listall length="+list.length);
            for (int i=0;i<list.length;i++) {
                System.out.println(list[i]);
            }
            IndexWriter writer = new IndexWriter(luceneDir, config);
            writer.addDocument(Arrays.asList(new TextField("title", "The title of the first document", Store.YES),
                                             new TextField("content", "The content of the first document", Store.YES),
                                             new TextField("global_bu_id", "1b", Store.YES),
                                             new TextField("omega_order_num", "1n", Store.YES)
                                             ));

            writer.addDocument(Arrays.asList(new TextField("title", "The tAtle of the second document", Store.YES),
                                             new TextField("content", "The content of the second document",Store.YES),
                                             new TextField("global_bu_id", "1k", Store.YES),
                                             new TextField("omega_order_num", "2b", Store.YES)
                                             ));
            writer.addDocument(Arrays.asList(new TextField("title", "The title of the third document", Store.YES),
                                             new TextField("content", "The content of the third document", Store.YES),
                                             new TextField("global_bu_id", "2k", Store.YES),
                                             new TextField("omega_order_num", "3b", Store.YES)                                             
                                             ));

            writer.addDocument(Arrays.asList(new TextField("title", "The title of the fourth document", Store.YES),
                                             new TextField("content", "The content of the fourth document", Store.YES),
                                             new TextField("global_bu_id", "2k", Store.YES),
                                             new TextField("omega_order_num", "4b", Store.YES)
                                             ));           

            //writer.commit();
            writer.close();            
             /*
            IndexReader reader = DirectoryReader.open(luceneDir);   
            // now test for docs
            if (reader.numDocs() != 3)
                throw new IOException("amount of returned docs are less than indexed");
            else
                System.out.println("test passed");
            */

            searchIndex("content", "second");
            System.out.println("---- now we delete docs for second document----");
            deleteDocs("content", "second");
            System.out.println("--- and repeat search-----");
            searchIndex("content", "second");
        } catch (Throwable te) {
            te.printStackTrace();
        }
    }


    
    public static void searchIndex(String searchField, String searchString) throws IOException, ParseException {
                    System.out.println("Searching for '" + searchString + "'");   
                    Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
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
    
        public static void deleteDocs(String searchField, String searchString) throws IOException, ParseException {
                    System.out.println("deleting docs for '" + searchString + "'");   
                    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_1, new StandardAnalyzer(Version.LUCENE_4_10_1));
                    config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                    ONSQLCodec codec = new ONSQLCodec();

                    config.setCodec(codec);
                    config.setUseCompoundFile(false);
                    Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
                    IndexWriter writer = new IndexWriter(luceneDir, config);
                    QueryParser queryParser = new QueryParser(Version.LUCENE_4_10_1,searchField, new StandardAnalyzer(Version.LUCENE_4_10_1));
                    Query query = queryParser.parse(searchString);
                    writer.deleteDocuments(query);
                    writer.commit();
                    writer.close();
                    luceneDir.close();                        
                    System.out.println("docs were deleted");
                }



    }


