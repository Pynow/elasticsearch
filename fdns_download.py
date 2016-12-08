Skip to content
This repository
Search
Pull requests
Issues
Gist
 @Amua
 Unwatch 1
  Star 0
 Fork 0 Amua/FileDetect
 Code  Issues 0  Pull requests 0  Projects 0  Wiki  Pulse  Graphs  Settings
Branch: master Find file Copy pathFileDetect/fdns.py
19cf815  on 14 Oct
@Amua Amua Create fdns.py
1 contributor
RawBlameHistory     
197 lines (174 sloc)  7.55 KB
#coding: utf-8

import os
import sys    
import requests
import shlex
import subprocess
import hashlib
import gzip
import json
import chardet
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers


class Elastic(object):
        
    def __init__(self, elastic=None, url='http://172.16.1.25:9090'):        
      self.elastic = elastic or Elasticsearch(url)
      
    def exists(self, index):
        return  self.elastic.indices.exists(index)      
           
    def create(self, index, body=None):                  
        self.elastic.indices.create(index, body=body, ignore=[400])   
    
    def insert(self, index, type, datas, id=None):    
        actions = []           
        for data in datas:
            actions.append({
                    "_index": index,
                    "_type": type,
                    "_id": id,
                    "_source": data
            })
        try:      
            helpers.bulk(self.elastic, actions) 
        except Exception, e:
            print ("{0} start bulk error: {1}".format(datetime.now(), e))         

class Fdns(object):
        
    def __init__(self):        
        self.website = "https://scans.io/study/sonar.fdns"       
        self.downurl, self.code = self.get_download_link()                     
        
    def get_download_link(self):       
        resp = requests.get(self.website)
        if resp.status_code != 200:
            raise Exception("download page failed: %d" % resp.status_code)
        soup = BeautifulSoup(resp.text, 'lxml')             
        try:
            table = soup.findAll(name='table', attrs={'class':'table table-condensed'})[0]
            tr = table.findAll(name='tr')[-1]              
            href = tr.findAll(name='a')[0]['href']
            code = tr.findAll(name='code')[0].text
        except Exception, msg:
            raise Exception("get download link failed: {}".format(msg))               
        return (href, code.lower())    
        
    @staticmethod
    def sha1(filepath, block_size=64*1024):
        try:
            with open(filepath, 'rb') as fd:
                sha1obj = hashlib.sha1()
                while True:
                    data = fd.read(block_size)
                    if not data:
                        break
                    sha1obj.update(data)
                retsha1 = sha1obj.hexdigest()
                return retsha1
        except IOError:
            raise Exception('Invalid file path: {}'.format(filepath))
        
    @staticmethod
    def make_dirs(dirpath, default='./sample'):
        try:       
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            return dirpath.rstrip(os.sep)
        except Exception, e:
            if not os.path.exists(default):
                os.makedirs(default)
            return dirpath.rstrip(os.sep) 
        
    @staticmethod
    def download_file(downurl, localdir=None, showlog=False):    
        localdir = Fdns.make_dirs(localdir)
        command_line = "wget -c -t100 -P {0} {1}".format(localdir, downurl)
        tmp_cmdline = shlex.split(command_line)
        try:
            proc = subprocess.Popen(args=tmp_cmdline,
                                    stderr=subprocess.STDOUT,
                                    stdout=subprocess.PIPE,
                                    bufsize=0)
        except IOError:
             raise EnvironmentError(1, "wget is not installed or could "
                                      "not be found in system path") 
        while showlog and proc.poll() is None:
            for streamline in iter(proc.stdout.readline, ''):
                sys.stdout.write(streamline)
        proc.communicate()        
        return proc.returncode      
        
    @staticmethod
    def gzip_extract(gzpath, dstpath, block_size=64*1024):
        try:
            with gzip.open(gzpath, 'rb') as fr, open(dstpath, 'wb') as fw:
                while True:
                    data = fr.read(block_size)
                    if not data:
                        break
                    fw.write(data)
        except IOError:
            raise Exception('Invalid file path: {}'.format(gzpath))                    
        
    @staticmethod
    def import_elastic(elastic, gzfile, step=500, sep=','):        
        with gzip.open(gzfile) as fd_file_down:          
            lines = []
            type = 'json'                                                   
            index = os.path.basename(gzfile)                 
            elastic.create(index=index, body={ 
                'mappings': {
                    'json': {
                        'properties' : {
                            'domain' : {
                                'type' : 'string',
                                'index': 'not_analyzed'
                            },
                            'record_type' : {
                                'type' : 'string',
                                'index': 'not_analyzed'
                            },
                            'record_value' : {
                                'type' : 'string',
                                'index': 'not_analyzed'
                            }                                                  
                        }                
                    }                    
                }
            })
                               
            for line in fd_file_down:
                success = False
                if not success:
                    try:
                        fields = line.strip().split(sep)
                        data = dict(domain=fields[0], record_type=fields[1], record_value=fields[2])                                                                          
                        lines.append(json.dumps(data))
                        success = True
                    except Exception:
                        pass
                if not success:
                    try:
                        encoding = chardet.detect(line).get('encoding')
                        line = line.decode(encoding)
                        fields = line.strip().split(sep)
                        data = dict(domain=fields[0], record_type=fields[1], record_value=fields[2])                                                                          
                        lines.append(json.dumps(data))
                        success = True                      
                    except Exception:
                        pass                      
                if len(lines) >= step:
                    elastic.insert(index, type, lines)
                    lines[:] = []
            if len(lines):
                elastic.insert(index, type, lines)                  
                      
def main():    
    fdns = Fdns() 
    localdir = "./sample"   
    downfilename = os.path.basename(fdns.downurl)
    
    print ("{0} start download file: {1}".format(datetime.now(), downfilename))
    while True:
        returncode = fdns.download_file(fdns.downurl, localdir=localdir, showlog=False)
        if returncode == 0:
            break
    
    print ("{0} start sha1 file: {1}".format(datetime.now(), downfilename))    
    downfilepath = localdir + os.path.sep + downfilename
    if fdns.sha1(downfilepath) != fdns.code:
        print "downfile failed: {}".format(fdns.downurl)
        sys.exit()
        
    print ("{0} start import file: {1}".format(datetime.now(), downfilename)) 
    elastic = Elastic()
    if not elastic.exists(downfilename):
        fdns.import_elastic(elastic, downfilepath)    
    
if __name__ == "__main__":   
    main()
    
Contact GitHub API Training Shop Blog About
© 2016 GitHub, Inc. Terms Privacy Security Status Help
