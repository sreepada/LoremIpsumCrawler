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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.net.*;

import org.apache.nutch.protocol.ProtocolFactory;                                                                                                     
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.segment.SegmentReader.SegmentReaderStats;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
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
import java.io.StringWriter;
import java.io.BufferedWriter;
import java.io.Writer;

import java.lang.Thread;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import java.net.URL;
import java.net.MalformedURLException;

/*
 *   This plugin accepts a URLFilter extension.
 *   At every instantiation it gets the latest metadata from current segment
 *   and stores them into a simhash, and compares the current URL's metadata
 *   simhash for near duplicates, within hamming distance of 3.
 *   It returns null if it is a near duplicate else returns the urls string.
 *
 *   Authors - LoremIpsumCrawler Team
 *             CSCI 575 Spring 2015
   */
public class NearDuplicateURLFilter implements URLFilter {

    private String attributeFile = null;

    private SuffixStringMatcher suffixes;
    private boolean modeAccept = false;
    private boolean filterFromPath = false;
    private boolean ignoreCase = false;

    private Configuration conf;

    private static final Logger LOG = LoggerFactory
        .getLogger(NearDuplicateURLFilter.class);


    public NearDuplicateURLFilter() throws IOException {
        LOG.info("start aagide");
        LOG.info("current thread ID " + Thread.currentThread().getId());

    }

    public void logExError(Exception e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        LOG.error(errors.toString());
    }

    //Override the filter function from interface URLFilter
    public String filter(String urlString) {
        LOG.debug("inside the function");
        LOG.debug("checking the url " + urlString);
        LOG.debug("current thread ID inside function " + Thread.currentThread().getId());

        JobConf job = new NutchJob(getConf());
        String path = job.get("mapred.work.output.dir", "dintGetAnything");
        LOG.debug("this is the current path: " + path);
        //Check to see if hadoop configuration has updated output dir to crawl
        //folder from temporary inject folder.
        if (path.contains("segment")) {
            SimhashIndex simind = new SimhashIndex(new HashMap<String, Long>());
            //trim to segment path from output.dir
            String segmentPath = path.split("/_temporary/")[0];
            LOG.info("this is the path of the current nutch job segments " + segmentPath);

            try {
                Configuration confForReader = NutchConfiguration.create();       
                FileSystem fs = FileSystem.get(confForReader);
                Path segmentFile = new Path(segmentPath + "/content/part-00000/data");
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, segmentFile, confForReader);
                Text segmentKey = new Text();
                Content segmentContent = new Content();
                // Loop through sequence files to get Content metadata of already
                // fetched urls and store into simhash
                while (reader.next(segmentKey, segmentContent)) {
                    Metadata segmentMetadata = segmentContent.getMetadata();
                    LOG.info("this is the key: " + segmentKey + " with the metadata: " + segmentMetadata.toString());
                    Set<String> shingles = new HashSet<String>();
                    for(String key: segmentMetadata.names())
                    {
                        if(key.equals("Date") || key.equals("Set-Cookie"))
                        {
                            continue;
                        }
                        shingles.add(key + ":" + segmentMetadata.get(key));
                    }
                    long simhash = SimHash.computeSimHashFromString(shingles); 
                    simind.add(Long.toString(System.nanoTime()), simhash);
                }
            } catch (Exception e) {
                logExError(e);
            }
            
            // Get content of current URL (sent as argument)
            Configuration conf = NutchConfiguration.create();
            Protocol protocol;
            Content content;
            try {
                protocol = new ProtocolFactory(conf).getProtocol(urlString);                                                                                
            } catch (Exception e) {
                logExError(e);
                return urlString;
            }
            content = protocol.getProtocolOutput(new Text(urlString),
                    new CrawlDatum()).getContent();

            //get metadata from content object
            Metadata metadata = content.getMetadata();
            LOG.info("this is the metadata: " + metadata.toString());

            Set<String> shingles = new HashSet<String>();

            //Loop through metadata and add to shingle only if 
            //it is not Date and Set-Cookie
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
            //return null if the url is a duplicate or add to simhash
            if(duplicates.size() > 0)
            {
                LOG.info("found a duplicate: " + urlString);
                return null;
            }
            else
            {
                simind.add(Long.toString(System.nanoTime()), simhash);
            }
        }
        return urlString;
    }

    public static void main(String args[]) throws IOException {
        NearDuplicateURLFilter obj = new NearDuplicateURLFilter();
        obj.filter("http://www.google.com");
    }


    public void setConf(Configuration conf) {
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
