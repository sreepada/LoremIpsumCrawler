# LoremIpsumCrawler
================================
CSCI 572 Spring 2015 Assignemnts

1. Mime types: (get_mime_types.py)
----------------------------------
Command to get mime types
python get_mime_types.py <path>/crawldb
    where <path> - project name used in crawl

2. Getting stats: (get_crawl_stats.py)
--------------------------------------
Command to get stats
python get_crawl_stats.py <path>/crawldb
    where <path> - project name used in crawl

3. Mime types found: (mime_types)
------------------------------
Various mime types found during crawling are listed in mime_types file

4. First crawl errors: (crawl_errors.txt)
----------------------------------------
Errors during first crawl (without selenium) can be found in crawl_errors.txt

5. Crawl errors with selenium:(crawl_errors_selenium.txt)
---------------------------------------------------------
Crawl errors with selenium are listed in crawl_errors_selenium.txt

6. Failed URLs:(failed_urls.txt)
--------------------------------
URLs which were failed during first crawl can be found in failed_urls.txt

7. Failed URLs during Selenium:(failed_urls_selenium.txt)
--------------------------------------------------------- 
URLs which failed during second crawl (with selenium) which can be found in failed_urls_selenium.txt
 
8. Include plugin into nutch:
--------------------------------
  1. Copy urlfilter-nearduplcate and urlfilter-exactdupliate to folder $nutch_root/src/plugin/.
  2. Add
      `<ant dir="urlfilter-nearduplicate" target="deploy"/>`
      `<ant dir="urlfilter-exactduplicate" target="deploy"/>`
       under deploy targets to file $nutch_root/src/plugin/build.xml as showin __plugin\_build.xml__
  3. _(Optional)_ Add
        `<ant dir="urlfilter-nearduplicate" target="clean"/>`
        `<ant dir="urlfilter-exactduplicate" target="clean"/>`
        under clean targets to file $nutch_root/src/plugin/build.xml as showin __plugin\_build.xml__
  4. Copy __nutch-site.xml__ to your preferred nutch-site.xml conf.
  5. run `ant runtime` in you $nutch_root
