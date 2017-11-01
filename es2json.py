
#coding: utf-8
import sys
import json
import gzip
import getopt
import threading
import Queue
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

class logRecode(object):
    def __init__(self):
        self.begin_time = datetime.now()
        self.end_time = datetime.now()
        self.total = 0
        self.success = 0 
        self.failed = 0   

class ElasticExportIndex(logRecode):
    
    def __init__(self, es_host, index_name, save_name,
                  b_gzip=False, count=False, scroll_size=1000,queue_size=10000):  
        logRecode.__init__(self)  
        self.elastic = Elasticsearch(es_host)
        self.index_name = index_name
        self.save_name = save_name
        self.b_gzip = b_gzip
        self.count = int(count) if count else False # select size
        self.res_count = 0
        self.scroll_size = scroll_size                
        self.queue = Queue.Queue(queue_size)
        self.fquit = object()          
        self.start()                        
        
    def read_elastic(self):                                            
        body = {'query': {'match_all':{}}}            
        res = self.elastic.search(index=self.index_name, 
                        body=body, 
                        scroll='3m',
                        search_type='scan',
                        size=self.scroll_size,
                        request_timeout=60)             
        scroll_size = res['hits']['total']              
        self.total = scroll_size
        # print self.total
        
        while (scroll_size > 0):
            
            try:                   
                scroll_id = res['_scroll_id']
                res = self.elastic.scroll(scroll_id=scroll_id, scroll='3m')                  
                for record in res['hits']['hits']:
                    try:                                                                                                       
                        self.queue.put(record['_source'])
                    except Exception as e:                  
                        print e                                                                            
                scroll_size = len(res['hits']['hits'])
                self.res_count += scroll_size # count check
                if self.count and (self.count <= self.res_count): break
                # print self.res_count,self.count
                print('[+] fetch {}...'.format(self.res_count))
            except Exception as e:                              
                pass                              
    
    def write_file(self):       
        try:
            if self.b_gzip:
                fp = gzip.open(self.save_name + '.gz', 'wb')
            else:
                fp = open(self.save_name, "wb")                
            while True:                
                record = self.queue.get()
                if record == self.fquit:
                    self.queue.put(self.fquit)
                    break
                try:                 
                    fp.write(json.dumps(record) + '\n')
                    self.success += 1
                except Exception, e:
                    self.failed += 1
                    print e
                self.queue.task_done()           
        finally:
            fp.close()
        self.end_time = datetime.now()
    
    def __str__(self):
        return json.dumps({
            "begin_time": str(self.begin_time),
            "end_time:": str(self.end_time),
            "total:": self.total,
            "success:": self.success,
            "failed:":  self.failed}, indent=4)      
    
    def start(self):
        if not self.elastic.indices.exists(self.index_name):
            print  "Error: Elasticsearch index %s not exists." % self.index_name
            sys.exit(-1)
            
        read_thread  = threading.Thread(target=self.read_elastic)
        read_thread.start()           
         
        write_thread = threading.Thread(target=self.write_file)
        write_thread.start()
        
        read_thread.join()
        self.queue.join()
        self.queue.put(self.fquit)
        write_thread.join()                      
    
def usage():      
    print "Example:"
    print "%s -e 192.168.1.121:9200 -i indexname -f result.json -c 10000 -g " %sys.argv[0]    
    sys.exit(1)
    
def main():    
    if len(sys.argv[1:]) < 1:
        usage()
    try: 
        opts, args = getopt.getopt(sys.argv[1:], 'hge:i:f:c:', ["help", "gzip", "elastic", "index", "file","count"])        
    except getopt.GetoptError, e:
        print str(e)
        usage()        
        
    b_gzip = False
    es_host = '' 
    es_index = ''
    save_file = ''
    count = False
    encoding = sys.getfilesystemencoding()     
    
    for o, a in opts:
        a = a.decode(encoding)
        if o in ("-h", "--help"):
            usage()
        elif o in ("-g", "gzip"): #
            b_gzip  = True        
        elif o in ("-e", "--elastic"): 
            es_host = a
        elif o in ("-i", "--index"):
            es_index = a
        elif o in ("-f", "--file"):
            save_file = a
        elif o in ("-c", "--count"):
            count = a
        else:
            assert False, "Unhandled Option" 
            
    if not (es_host and es_index and save_file):
        usage()           
    sample = ElasticExportIndex(es_host, es_index, save_file, b_gzip, count)
    print sample
    
if __name__ == "__main__":
    main()    

print 'Exiting Main Thread...'