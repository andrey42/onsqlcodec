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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

public class testMergeSegments {
    private static final String INDEX_ROOT_FOLDER = "C:\\work\\search_engine\\codec\\index-dir";  
    public static void main(String[] args) {
        try {
            testUtil.initPropsONSQL();
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_1);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            ONSQLCodec codec = new ONSQLCodec();
            config.setCodec(codec);
            config.setUseCompoundFile(false);
            Directory luceneDir = new ONSQLWrapperDirectory(new File(INDEX_ROOT_FOLDER));
            IndexWriter writer = new IndexWriter(luceneDir, config);
            writer.forceMerge(1);
            writer.close();

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


}
