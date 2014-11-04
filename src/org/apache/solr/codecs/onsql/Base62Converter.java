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

  
public class Base62Converter {
 
	public static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
 
	public static final int BASE = ALPHABET.length();
 
	private Base62Converter() {}
 
	public static String fromBase10(int i) {
                if (i == 0) return "0";
		StringBuilder sb = new StringBuilder("");
	        boolean negative = (i < 0);
                if (negative) i= -i;
		while (i > 0) {
			i = fromBase10(i, sb);
		}
                //if (negative) sb.insert(0, "_");
                sb = sb.reverse();
                if (negative) sb.insert(0,'_');
		return sb.toString();
	}
 
	private static int fromBase10(int i, final StringBuilder sb) {                
		int rem = i % BASE;
		sb.append(ALPHABET.charAt(rem));
		return i / BASE;
	}
 
	public static int toBase10(String str) {
                StringBuilder sb = new StringBuilder(str);
                boolean negative = false;
                if (sb.indexOf("_")==0) {negative = true; sb.deleteCharAt(0); }
                int retval = toBase10(sb.reverse().toString().toCharArray());
                if (negative) retval = -retval;
		return retval;
	}
 
	private static int toBase10(char[] chars) {
		int n = 0;
		for (int i = chars.length - 1; i >= 0; i--) {
			n += toBase10(ALPHABET.indexOf(chars[i]), i);
		}
		return n;
	}
 
	private static int toBase10(int n, int pow) {
		return n * (int) Math.pow(BASE, pow);
	}
        
    public static void main(String[] args) {
        int val = 0;        
        String base62str = Base62Converter.fromBase10(val);
        System.out.println(val);
        System.out.println(base62str);
        System.out.println(Base62Converter.toBase10(base62str));
    }
}