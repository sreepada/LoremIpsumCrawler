import os
import sys
import re
class Stats:
    def __init__(self):
        self.fetched = 0
        self.unfetched = 0
        self.failed = 0

    def __str__(self):
        return "fetched: %d\nunfetched: %d\nfailed: %d"%(self.fetched, self.unfetched, self.failed)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "python get_mime_types.py <path>/crawldb"
        sys.exit(-1)
    path = sys.argv[1]
    if not os.path.exists(path):
        print "%s does not exists. Verify the path"
        sys.exit(-1)
    path  = path + "/current/part-00000/data"
    if not os.path.exists(path):
        print "No data"
        sys.exit(0)

    stats = Stats()

    import nutchpy
    data = nutchpy.sequence_reader.read(path)
    
    for each_file in data:
        file_name, parameters = each_file[0], each_file[1]
        parameters = parameters.strip()
        tokens = parameters.split("\n")
        for token in tokens:
            token = token.strip()
            try:
                k,v = token.split(":", 1)
                k = k.strip()
                v = v.strip()
            except Exception, e:
                continue
            else:
                if k == 'Status':
                    if v.find("db_fetched") != -1:
                        stats.fetched += 1
                    elif v.find("db_unfetched") != -1:
                        stats.unfetched += 1
                    else:
                        stats.failed += 1
                    break
    
    print "TOTAL urls: ",len(data) 
    print stats
