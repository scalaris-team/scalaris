/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.storage.apachelog;

import java.util.regex.Pattern;

import org.apache.pig.piggybank.storage.RegExLoader;

/**
 * CombinedLogLoader is used to load logs based on Apache's combined log format, based on a format like
 * 
 * LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
 * 
 * The log filename ends up being access_log from a line like
 * 
 * CustomLog logs/combined_log combined
 * 
 * Example:
 * 
 * raw = LOAD 'combined_log' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader AS
 * (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referer, userAgent);
 * 
 */

public class ContrailLogLoader extends RegExLoader {
    //::1 - - [04/Aug/2011:10:28:59 +0200] "GET /scalaris-wiki/?title=Sports HTTP/1.1" 200 32658
    private final static Pattern combinedLogPattern = Pattern
        .compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+\"(\\S+)\\s+([^\\?]+)\\?title=(\\S+)\\s+(HTTP[^\"]+)\"\\s+(\\S+)\\s+(\\S+)$");

    public Pattern getPattern() {
        return combinedLogPattern;
    }
}
