/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.urlfilter.exactduplicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.*;

import org.apache.nutch.protocol.ProtocolFactory;                                                                                                     
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.SuffixStringMatcher;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;

import java.lang.Thread;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashMap;

import java.net.URL;
import java.net.MalformedURLException;

public class ExactDuplicateURLFilter implements URLFilter {

    private String attributeFile = null;

    private SuffixStringMatcher suffixes;
    private boolean modeAccept = false;
    private boolean filterFromPath = false;
    private boolean ignoreCase = false;

    private Configuration conf;
    private static IdDuplicates exact_duplicate = new IdDuplicates();

    private static final Logger LOG = LoggerFactory
        .getLogger(ExactDuplicateURLFilter.class);


    public ExactDuplicateURLFilter() throws IOException {
        LOG.info("current thread ID " + Thread.currentThread().getId());

    }

    public String filter(String url) {
        String urlString = url;
        Protocol protocol;
        Content content;

        Configuration conf = NutchConfiguration.create();
        try {
            protocol = new ProtocolFactory(conf).getProtocol(urlString);                                                                                
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        content = protocol.getProtocolOutput(new Text(urlString),
                new CrawlDatum()).getContent();

        String pageText = "";
        try {
            pageText = new String(content.getContent(), "UTF-8");
        } catch (Exception e) {
            LOG.error("excpetion while string encoding");
        }

        if(!exact_duplicate.checkForExactDuplicates(pageText, urlString))
        {
            return null;
        }

        return url;
    }

    public static void main(String args[]) throws IOException {
        ExactDuplicateURLFilter obj = new ExactDuplicateURLFilter();
        obj.filter("http://www.google.com");
    }


    public void setConf(Configuration conf) {
        this.conf = conf;

        String pluginName = "urlfilter-suffix";
        Extension[] extensions = PluginRepository.get(conf)
            .getExtensionPoint(URLFilter.class.getName()).getExtensions();
        for (int i = 0; i < extensions.length; i++) {
            Extension extension = extensions[i];
            if (extension.getDescriptor().getPluginId().equals(pluginName)) {
                attributeFile = extension.getAttribute("file");
                break;
            }
        }
        if (attributeFile != null && attributeFile.trim().equals(""))
            attributeFile = null;
        if (attributeFile != null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Attribute \"file\" is defined for plugin " + pluginName
                        + " as " + attributeFile);
            }
        } 

        String file = conf.get("urlfilter.suffix.file");
        String stringRules = conf.get("urlfilter.suffix.rules");
        // attribute "file" takes precedence if defined
        if (attributeFile != null)
            file = attributeFile;
        Reader reader = null;
        if (stringRules != null) { // takes precedence over files
            reader = new StringReader(stringRules);
        } else {
            reader = conf.getConfResourceAsReader(file);
        }

    }

    public Configuration getConf() {
        return this.conf;
    }

    public boolean isModeAccept() {
        return modeAccept;
    }

    public void setModeAccept(boolean modeAccept) {
        //    this.modeAccept = modeAccept;
    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean ignoreCase) {
        //    this.ignoreCase = ignoreCase;
    }

    public void setFilterFromPath(boolean filterFromPath) {
        //    this.filterFromPath = filterFromPath;
    }
}
