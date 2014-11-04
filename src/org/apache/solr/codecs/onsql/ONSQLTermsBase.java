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

import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;

import java.util.Comparator;

public abstract class ONSQLTermsBase extends Terms
{
    private final boolean hasOffsets;
    private final boolean hasPositions;
    private final boolean hasPayloads;
    private final long size;
    private final long sumDocFreq;
    private final int docCount;

    public ONSQLTermsBase(boolean hasPositions,
                        boolean hasOffsets,
                        boolean hasPayloads,
                        long size,
                        long sumDocFeq,
                        int docCount) {
        this.hasOffsets = hasOffsets;
        this.hasPositions = hasPositions;
        this.hasPayloads = hasPayloads;
        this.size = size;
        this.sumDocFreq = sumDocFeq;
        this.docCount = docCount;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long getSumTotalTermFreq() {
        return -1;
    }

    @Override
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    @Override
    public int getDocCount() {
        return docCount;
    }

    @Override
    public boolean hasOffsets() {
        return hasOffsets;
    }

    @Override
    public boolean hasPositions() {
        return hasPositions;
    }

    @Override
    public boolean hasPayloads() {
        return hasPayloads;
    }
}
