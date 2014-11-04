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

import oracle.kv.Value;

public class FieldEntry {
    private String field_id;
    private String type;
    private Value value;
    public FieldEntry(String p_field_id,String p_type,Value p_val) {
        this.field_id = p_field_id;
        this.type = p_type;
        this.value = p_val;
    }
    public String getFieldid() {
        return this.field_id;
    }
    public String getType() {
        return this.type;
    }
    public Value getValue() {
        return this.value;
    }
}
