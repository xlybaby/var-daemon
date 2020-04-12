# -*- coding: utf-8 -*-
import sys,os,json,codecs,re,requests,random,threading,time,fcntl
from datetime import datetime
from datetime import date
from urllib.request import urlretrieve
from urllib.parse import urljoin, urldefrag

from scrapy.selector import Selector

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.tornado import TornadoScheduler

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
import tornado.ioloop
import tornado.web
from tornado.tcpclient import TCPClient
from tornado import queues

from fileBasedConfiguration import ApplicationProperties, PgConnectionPool
from constants import RequestMapping
inq = queues.Queue()

def geturl(url, newurl):
    if newurl.lower().startswith("https://") or newurl.lower().startswith("http://"):
        return newurl
    pure_url, frag = urldefrag(newurl)
    return urljoin(url, pure_url) 
    #return pure_url

def gettimepartition():
    return 'y20s1'

def getseconddomain(addr):
    secondDomain = re.search(r"[A-Za-z0-9\-]+\.com|[A-Za-z0-9\-]+\.edu.cn|[A-Za-z0-9\-]+\.cn|[A-Za-z0-9\-]+\.com.cn|[A-Za-z0-9\-]+\.org|[A-Za-z0-9\-]+\.net|[A-Za-z0-9\-]+\.tv|[A-Za-z0-9\-]+\.vip|[A-Za-z0-9\-]+\.cc|[A-Za-z0-9\-]+\.gov.cn|[A-Za-z0-9\-]+\.gov|[A-Za-z0-9\-]+\.edu|[A-Za-z0-9\-]+\.biz|[A-Za-z0-9\-]+\.net.cn|[A-Za-z0-9\-]+\.org.cn",addr.lower())
    return secondDomain.group()
    
async def asyncjob(workdir,contentdir,collector):
        print("job fired, collect dir: %s"%(workdir))
        for f in os.listdir(workdir):
                if not f.endswith(".ini"):
                    continue
                print("find task %s"%(f))
                fp = open(workdir+"/"+f, "r")
                
                try:
                    fcntl.flock(fp, fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError as e:
                    print("file locked, %s"%(f))
                    fp.close()
                    continue
                #finally:
                    
                    
                try:
                    taskstr = fp.readline()
                    print(taskstr)
                    if taskstr:
                        task = json.loads(taskstr)
                        mid = task["materialId"]
                        pid = task["pid"]
                        seqno = task["seqno"]
                        definition = task["definition"]
                        charset = task["charset"]
                        url = task["url"]
                        type = task["type"]
                        content = open(contentdir+"/"+seqno, "r", encoding=charset)
                        html = content.read()
                        data = None
                        if type == 1:
                            data = collector.collectBrowser(p_content=html, p_definition=definition, p_seqno=seqno)
                        elif type == 2:
                            data = collector.collectBanner(p_content=html, p_definition=definition, p_seqno=seqno)
                        elif type == 3:
                            data = collector.collectSubscribe(p_content=html, p_definition=definition, p_seqno=seqno)
                        elif type == 4:
                            data = collector.collectTs(p_content=html, p_definition=definition, p_seqno=seqno)
                        elif type == 5:
                            data = collector.collectCorpus(p_content=html, p_definition=definition, p_seqno=seqno)
                        print(data)
                        collector.write(p_task=task, p_data = data)
                        os.remove(contentdir+"/"+seqno)
                        os.remove(workdir+"/"+f)
                except Exception as e:
                    print(e)
                    fcntl.flock(fp, fcntl.LOCK_UN)
                    fp.close()
                finally:
                    None
                    
class DashBoardHandler(tornado.web.RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])


    async def post(self):
        event = self.args["event"]
        if event == "jobs":
            jobs={"jobs":[]}
            for job in CollectorDispatch.get_jobs():
                j={"jobId":job.id, "jobName":job.name}
                jobs["jobs"].append(j)
                self.write(json.dumps(jobs))
                
class JobHandler(tornado.web.RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])

    async def post(self):
        event = self.args["event"]
        if event == "add":
            None
        elif event == "remove":
            None
        elif event == "stop":
            None
        
class Collector(object):
    
    def __init__(self):
        self._workdir = ApplicationProperties.configure('application.storage.filesystem.download.tempdir')
        self._contentdir = ApplicationProperties.configure('application.storage.filesystem.download.tempdir') +"/html"
        self._staticdir = ApplicationProperties.configure('application.storage.filesystem.download.staticdir')
        
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
        
    def collectBanner(self, p_content, p_definition, p_seqno):
        s = Selector(text=p_content)
        selectors =  p_definition["selectors"]
        rooturl = p_definition["addr"]
        data=[]
        for selector in selectors:
            imgxpath = selector["xpath"]
            linkxpath = None
            if "linkselector" in selector:
                linkxpath = selector["linkselector"]["xpath"]
                for idx, link in enumerate(s.xpath(linkxpath)):
                    kv={}
                    kv["no"] = idx
                    kv["imgsrc"] = geturl(rooturl, link.xpath(imgxpath).xpath("@src").get()) 
                    kv["link"] = geturl(rooturl, link.xpath("@href").get()) 
                    imgname = kv["imgsrc"].split("/")[-1]
                    kv["imgname"] = imgname
                    kv["domain"] = getseconddomain(kv["imgsrc"])
                    if not os.path.exists(self._staticdir+"/"+kv["domain"]):
                        os.makedirs(self._staticdir+"/"+kv["domain"])
                    urlretrieve(kv["imgsrc"], self._staticdir+"/"+kv["domain"]+"/"+ imgname)
                    data.append(kv)
            else:
                for idx, img in enumerate(s.xpath(imgxpath)):
                    kv={}
                    kv["no"] = idx
                    kv["imgsrc"] = geturl(rooturl, img.xpath("@src").get()) 
                    imgname = kv["imgsrc"].split("/")[-1]
                    kv["imgname"] = imgname
                    kv["domain"] = getseconddomain(kv["imgsrc"])
                    if not os.path.exists(self._staticdir+"/"+kv["domain"]):
                        os.makedirs(self._staticdir+"/"+kv["domain"])
                    urlretrieve(kv["imgsrc"], self._staticdir+"/"+kv["domain"]+"/"+ imgname)
                    data.append(kv)
        return data
        
    def collectBrowser(self, p_content, p_definition, p_seqno):
        #Invoke mass collector
        None
        
    def collectSubscribe(self, p_content, p_definition, p_seqno):
        s = Selector(text=p_content)
        selectors =  p_definition["selectors"]
        rooturl = p_definition["addr"]
        limit = -1
        if "limit" in p_definition:
            limit = p_definition["limit"]
        data=[]
        for sel in selectors:
            xpath = sel["xpath"]
            for idx, item in enumerate(s.xpath(xpath)):
                if limit>=0 and idx>=limit:
                    break 
                kv={}
                kv["no"] = idx+1
                string = item.xpath(".//text()").getall()
                strings = ""
                if string:
                    for s in string:
                        strings = strings+s.strip()
                kv["value"] = strings
                kv["hashcode"] = hash(strings)
                if item.xpath("@href"):
                    href = item.xpath("@href").get()
                    kv["href"] = geturl(rooturl, href) 
                data.append(kv)
        return data
            
    def write(self, p_task, p_data):
        if p_data:
            data = {}
            data["charset"] = p_task["charset"]
            data["url"] = p_task["url"]
            data["value"] = p_data
            ttime = datetime.now()
            pt = gettimepartition()
            sql = "insert into t_uc_material_data__"+pt+"__"+p_task["pid"] + " (materialId,seqno,insertDay,contents,insertTime) values (%s,%s,%s,%s,%s) "
            sqldata = (p_task["materialId"],p_task["seqno"],date.today(),json.dumps(data),ttime)
            conn,cur = PgConnectionPool.getConn()
            try:
                cur.execute(sql, sqldata)
                conn.commit()
            except Exception as e:
                print(e)
            finally:
                PgConnectionPool.release(conn, cur)        
            
class CollectorDispatch(object):     
    instance=None
    
    @staticmethod
    def start():
        CollectorDispatch.instance=CollectorDispatch()
        CollectorDispatch.instance.load()
    
    @staticmethod
    def get_jobs():
        return CollectorDispatch.instance.getjobs()
        
    def __init__(self):
        self._scheduler = None
        
    def getjobs(self):
        return self._scheduler.get_jobs()
    
    async def send(self):
        try:
            stream = await TCPClient().connect(self._massServerHost, self._massServerPort)
            #stream = await TCPClient().connect('127.0.0.1',8888)
            async for record in inq:
                            try:
                                print("Got a task: %s"%(json.dumps(record)))
                                await stream.write((json.dumps(record) + "\n").encode())
                                print("Sent to server:")
                                reply = await stream.read_until(b"\n")
                                print("Response from server:", reply.decode().strip())
                            except Exception as e:
                                print(e)
                            finally:
                                inq.task_done()       
        except Exception as e:
            print(e)
            
    def load(self):
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
        self._massServerHost = ApplicationProperties.configure("application.mass.server.host")
        self._massServerPort = ApplicationProperties.configure("application.mass.server.port")
        
        print('Application listeming...[Port:%s]'%(ApplicationProperties.configure(p_key='application.collector.server.port')))
        application = tornado.web.Application([ (RequestMapping.adjust_collect_job, JobHandler),
                                                                                   (RequestMapping.fetch_collect_dashboard, DashBoardHandler), ])
        application.listen(ApplicationProperties.configure(p_key='application.collector.server.port'))
        
        self._scheduler = TornadoScheduler()    
        self._scheduler.add_job(asyncjob,'interval', seconds=int(ApplicationProperties.configure("application.collector.schedule.intervalSeconds")), args=(ApplicationProperties.configure('application.storage.filesystem.download.tempdir'),ApplicationProperties.configure('application.storage.filesystem.download.tempdir') +"/html",Collector()))
        #self._scheduler.configure(executors=executors, job_defaults=job_defaults)
        self._scheduler.start()
        
        tornado.ioloop.IOLoop.current().run_sync(self.send)
        #print(ioLoop)
        #ioLoop.start()
        
if __name__ == '__main__':
    ApplicationProperties.populate({"config":"/Users/apple/Documents/var/workspace/var-daemon"})
    PgConnectionPool.ini(host=ApplicationProperties.configure("application.storage.postgres.connection.host"),
                                                    port=ApplicationProperties.configure("application.storage.postgres.connection.port"),
                                                    user=ApplicationProperties.configure("application.storage.postgres.connection.user"),
                                                    password=ApplicationProperties.configure("application.storage.postgres.connection.password"),
                                                    database=ApplicationProperties.configure("application.storage.postgres.connection.database"))


    #CollectorDispatch().load()
    CollectorDispatch.start()