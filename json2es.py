#coding: utf-8
import os
import sys
import json
import gzip
import threading
import Queue
import getopt
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

class logRecode(object):
    def __init__(self):
        self.begin_time = datetime.now()
        self.end_time = datetime.now()
        self.total = 0
        self.success = 0 
        self.failed = 0   

class ImportElasticIndex(logRecode):    
    def __init__(self, es_host, es_index, src_file, 
                 b_gzip=False, scroll_size=1000, queue_size=10000):
        logRecode.__init__(self)        
        self.elastic = Elasticsearch(es_host)
        self.es_index = es_index  
        self.doc_type = 'json' 
        self.src_file = src_file
        self.b_gzip = b_gzip
        self.scroll_size = scroll_size     
        self.queue = Queue.Queue(queue_size)
        self.fquit = object()
        self.start()      
        
    def is_gzip(self, filename):
        try:
            fp = open(filename, 'rb') 
            if os.path.getsize(filename) >=2:
                magic = self.fileobj.read(2)
                if magic == '\037\213':
                    return True                    
        finally:
            fp.close()                              
        
    def read_file(self):        
        try:
            if self.b_gzip:
                fp = gzip.open(self.src_file, 'rb')
            else:
                fp = open(self.src_file, 'rb')
            for record in fp: 
                try:               
                    record = record.strip()
                    self.queue.put(record) 
                    self.total += 1             
                except Exception, e:
                    print e                               
        finally:
            fp.close()
    
    def write_elastic(self):
        actions = []                
        while True:
            record = self.queue.get()                  
            if record == self.fquit:
                self.queue.put(self.fquit)
                if len(actions):
                    try:                            
                        success, failed = helpers.bulk(self.elastic, actions, True)
                        self.success += success
                        self.failed += failed
                        actions[:] = [] 
                    except Exception, e:
                        print e                                        
                break                     
            actions.append({
                   "_index": self.es_index,
                    "_type": self.doc_type,
                    "_id": None,
                    "_source": record                            
            })                        
            if len(actions) >= self.scroll_size:
                try:
                    success, failed = helpers.bulk(self.elastic, actions, True)
                    self.success += success
                    self.failed += failed 
                    actions[:] = [] 
                except Exception, e:                  
                    print e            
            self.queue.task_done()     
        self.end_time = datetime.now()
            
    def __str__(self):
        return json.dumps({
            "begin_time": str(self.begin_time),
            "end_time:": str(self.end_time),
            "total:": self.total,
            "success:": self.success,
            "failed:":  self.failed}, indent=4)        
    
    def start(self):
        if not os.path.exists(os.path.abspath(self.src_file)):
            print "Error: Source file %s not exists." % self.src_file
            sys.exit(-1)                               
        self.elastic.indices.create(self.es_index, 
                                    body=json.loads(open('mapping.json').read()),
                                    ignore=[400, 404])        
        read_thread  = threading.Thread(target=self.read_file)
        read_thread.start()           
         
        write_thread = threading.Thread(target=self.write_elastic)
        write_thread.start()
        
        read_thread.join()
        self.queue.join()
        self.queue.put(self.fquit)
        write_thread.join()                 
        
def usage():      
    print "Example:"
    print "%s -e 192.168.1.121:9200 -i indexname -f result.json -g" %sys.argv[0]    
    sys.exit(1)
    
def main():    
    if len(sys.argv[1:]) < 1:
        usage()
    try: 
        opts, args = getopt.getopt(sys.argv[1:], 'hge:i:f:', 
                                   ["help", "gzip", "elastic", "index", "file"])        
    except getopt.GetoptError, e:
        print str(e)
        usage()        
        
    b_gzip = False
    es_host = '' 
    es_index = ''
    src_file = ''  
    encoding = sys.getfilesystemencoding()     
    
    for o, a in opts:
        a = a.decode(encoding)
        if o in ("-h", "--help"):
            usage()
        elif o in ("-g", "gzip"):
            b_gzip  = True        
        elif o in ("-e", "--elastic"): 
            es_host = a
        elif o in ("-i", "--index"):
            es_index = a
        elif o in ("-f", "--file"):
            src_file = a
        else:
            assert False, "Unhandled Option" 
            
    if not (es_host and es_index and src_file):
        usage()           
    sample = ImportElasticIndex(es_host, es_index, src_file, b_gzip)
    print sample
    
if __name__ == "__main__":
    main()
