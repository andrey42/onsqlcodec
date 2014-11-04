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

import java.util.LinkedHashMap;
import java.util.Map;

public class testLinkedHashMap {
    public static void main(String[] args) {
       LinkedHashMap<String,String> testmap = new LinkedHashMap<String,String>();
       testmap.put("key1", null);
       testmap.put("key2", "22");
       testmap.put("key3", "333");
       
       if (testmap.containsKey("key1")) System.out.println("key1=true");       
       
       for (Map.Entry<String,String> entry: testmap.entrySet()) {
           System.out.println("key="+entry.getKey()+";value="+(entry.getValue()==null?"NULL":entry.getValue()));
       }
    }
}
