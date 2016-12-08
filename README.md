#elasticsearch Python脚本

---

###es2json.py    
用于将数据从elasticsearch导出为json    
支持导出过程中将json文件压缩为gz格式

---   

###json2es.py    
用于将json格式文件的输入导入elasticsearch    
支持json文件的gz压缩包直接导入    
支持自定义mapping（在脚本所在目录加入mapping.json）

---

###fdns_download.py    
下载scans.io下的最新dnsrecords_all数据，自动解压入库    
网络数据连接：https://scans.io/study/sonar.fdns
