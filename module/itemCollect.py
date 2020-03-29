# -*- coding: utf-8 -*-
import sys,os,json,codecs,re,requests,random,threading,time,datetime
from urllib.request import urlretrieve

from scrapy.selector import Selector

from apscheduler.schedulers.background import BackgroundScheduler

#from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

_properties=None
_scheduler=None

class Collector(object):
    
    def __init__(self):
        self._workdir = _properties.getProperty('application.storage.filesystem.download.tempdir')
        self._contentdir = _properties.getProperty('application.storage.filesystem.download.tempdir') +"/html"
        self._staticdir = _properties.getProperty('application.storage.filesystem.download.staticdir')
        
    def collectCorpus(self, p_content, p_selector):
        None
    
    def collectTs(self, p_content, p_selector,p_materialId, p_url, p_seqno):
        s = Selector(text=p_content)
        yaxisXpath =  p_selector["yaxisXpath"]
        data={"yaxisXpath":[]}
        for idx, item in enumerate(s.xpath(yaxisXpath)):
            data["yaxisXpath"].append(item.xpath(".//text()").getall())
        
        if p_selector["timeXAxis"]:
            starttime = datetime.datetime.now()
            for t in range(len(data["yaxisXpath"])):
                data["xaxisXpath"].append(starttime + timedelta(seconds=t))    
        else:
            xaxisXpath =  p_selector["xaxisXpath"]
            data={"xaxisXpath":[]}
            for idx, item in enumerate(s.xpath(xaxisXpath)):
                data["xaxisXpath"].append(item.xpath(".//text()").getall())    
        return data
        
    def collectBanner(self, p_content, p_selector,p_materialId, p_url, p_seqno):
        s = Selector(text=p_content)
        itemXpath =  p_selector["imgXpath"]
        data=[]
        for idx, item in enumerate(s.xpath(itemXpath)):
            kv={}
            kv["no"] = idx
            if p_selector["needLink"]:
                href = item.xpath("@href")
                if href:
                    kv["href"] = href.get()
                img = item.xpath(".//img")
                if img:
                    imgsrc = img.xpath("@src").get()
            else:
                imgsrc = item.xpath("@src").get()
                
            imgname = imgsrc.split("/")[-1]
            kv["img"] = "bannerImg"+p_materialId+"-"+imgname
            urlretrieve(imgsrc, self._staticdir+"/"+kv["img"])
            data.append(kv)
        return data
        
    def collectBrowser(self, p_content, p_definition, p_seqno):
        None
        
    def collectSubscribe(self, p_content, p_selector,p_materialId, p_url, p_seqno):
        s = Selector(text=p_content)
        itemXpath =  p_selector["itemXpath"]
        data=[]
        for idx, item in enumerate(s.xpath(itemXpath)):
            kv={}
            kv["no"] = idx
            kv["value"] = item.xpath(".//text()").getall()
            if p_selector["needLink"]:
                href = item.xpath("@href")
                if href:
                    kv["href"] = href.get()
            data.append(kv)
        return data
            
    def write(self, p_material_id, p_time, p_data):
        None
        
    def __call__(self):
        for f in os.listdir(self._workdir):
                fp = open(self._workdir+"/"+f, "r")
                try:
                    fcntl.flock(fp, fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError as e:
                    print(e)
                    continue
                
                taskstr = fp.readline()
                if taskstr:
                    task = json.loads(taskstr)
                    seqno = task["seqno"]
                    definition = task["definition"]
                    charset = task["charset"]
                    url = task["url"]
                    content = open(self._contentdir+"/"+seqno, "r", encoding=charset)
                    
                fcntl.flock(fp, fcntl.LOCK_UN)
                fp.close()
                
class CollectorDispatch(object):     
    
    def __init__(self):
        self._scheduler = None
        
    def load(self,p_properties = None, q=None):
        _properties=p_properties
         #jobstores = {
        #    'mongo': {'type': 'mongodb'},
         #   'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        #}
        executors = {
            'default': {'type': 'threadpool', 'max_workers': 5}
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }
        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(Collector,'interval', seconds=p_properties.getProperty("application.collector.schedule.intervalSeconds "))
        self._scheduler.configure(executors=executors, job_defaults=job_defaults)
        self._scheduler.start()
        
if __name__ == '__main__':
    InfiniteScan().__call__(p_schedule=[{"pid":"p10000","tableName":"aaa","scannerId":"scannerP10000","intervalSecs":300,"startSeq":0,"endSeq":10000},
                                        {"pid":"p10000","tableName":"aaa","scannerId":"scannerP10000","intervalSecs":300,"startSeq":10000,"endSeq":20000},
                                        {"pid":"p10000","tableName":"aaa","scannerId":"scannerP10000","intervalSecs":300,"startSeq":20000,"endSeq":30000}], p_properties={"application.scanner.schedule.intervalSeconds":10,"application.exchange.server.host":"localhost","application.exchange.server.port":8888})
    