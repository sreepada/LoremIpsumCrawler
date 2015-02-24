import os
import sys
import re
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

    import nutchpy
    data = nutchpy.sequence_reader.read(path)
    
    mime_types = set()
    
    for each_file in data:
        file_name, parameters = each_file[0], each_file[1]
        parameters = parameters.strip()
        tokens = parameters.split("\n")
        for token in tokens:
            token = token.strip()
            if token.find("Content-Type=") != -1:
                k, v = token.split('=', 1)
                mime_types.add(v)
                break
    
    
    print "Mime types found\n"
    print "\n".join(mime_types)
