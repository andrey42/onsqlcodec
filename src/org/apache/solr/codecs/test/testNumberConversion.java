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

import org.apache.solr.codecs.onsql.Base62Converter;
import org.apache.solr.codecs.onsql.SortableNumbers;

public class testNumberConversion {
    
    public static void main(String[] args) {
        ls(-3);
        ls(-2);
        ls(-1);
        ls(1);
        ls(2);
        ls(3);
        System.out.println("--------------");
        lt(-1);
        lt(1);
        lt(100);
        
    }
    
    private static void ls(int in) { 
        String str = SortableNumbers.intToPrefixCoded(in);
        System.out.println("++" + SortableNumbers.intToPrefixCoded(in)+"++");
        for (int i=0;i<str.length();i++) {
            System.out.print((int)str.charAt(i)+"//");
        }
        System.out.println();
    }
    
    private static void lt(int in) { 
        String str = Base62Converter.fromBase10(in);
        System.out.println(str);
    }
}
