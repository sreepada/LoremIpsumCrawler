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

package org.apache.nutch.urlfilter.nearduplicate;

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
import java.util.HashSet;

import java.net.URL;
import java.net.MalformedURLException;

public class NearDuplicateURLFilter implements URLFilter {

    private String attributeFile = null;

    private SuffixStringMatcher suffixes;
    private boolean modeAccept = false;
    private boolean filterFromPath = false;
    private boolean ignoreCase = false;

    private Configuration conf;
    private static SimhashIndex simind = new SimhashIndex(new HashMap<String, Long>());

    private static final Logger LOG = LoggerFactory
        .getLogger(NearDuplicateURLFilter.class);


    public NearDuplicateURLFilter() throws IOException {
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

        Metadata metadata = content.getMetadata();
        LOG.info("this is the metadata: " + metadata.toString());

        Set<String> shingles = new HashSet<String>(); 

        for(String key: metadata.names())
        {
            if(key.equals("Date") || key.equals("Set-Cookie"))
            {
                continue;
            }
            shingles.add(key + ":" + metadata.get(key));
        }


        long simhash = SimHash.computeSimHashFromString(shingles); 

        Set<String> duplicates = simind.get_near_dups(simhash);

        if(duplicates.size() > 0)
        {
            LOG.info("found a duplicate: " + urlString);
            return null;
        }
        else
        {
            simind.add(Long.toString(System.nanoTime()), simhash);
        }

        /*try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/sreepada/Documents/CSCI_572/nutch/runtime/local/myfile.txt", true)))) {
            out.println("\n{\n\turl:'" + urlString + "',\n\t metadata: '" + metadata.toString() + "'\n}");
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
            LOG.error("file bardilla");
        }*/
        return url;
    }

    public static void main(String args[]) throws IOException {
        NearDuplicateURLFilter obj = new NearDuplicateURLFilter();
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
        } else {
            // if (LOG.isWarnEnabled()) {
            // LOG.warn("Attribute \"file\" is not defined in plugin.xml for
            // plugin "+pluginName);
            // }
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

        /*try {
        //readConfiguration(reader);
        } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
        LOG.error(e.getMessage());
        }
        throw new RuntimeException(e.getMessage(), e);
        }*/
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
