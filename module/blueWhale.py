# -*- coding: utf-8 -*-
import os,time,sys,json
from datetime import datetime, timedelta,date
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag
import urllib3.contrib.pyopenssl

from tornado import gen, httpclient, queues
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler
from tornado.web import Application

from scrapy.selector import Selector

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
    
from fileBasedConfiguration import ApplicationProperties, PgConnectionPool
from constants import RequestMapping
import varcommon

p_concurrency = 1
c_concurrency = 1

async def pagination(definition, srcq, distq,name):
            async for task in srcq:
                limit=definition["limit"]
                interim=definition["interim"]
                mode=definition["mode"]
                print("pagination[%s] got task:%s"%(name,task))
                print("pagination configure:%d %d"%(limit, interim))
                
                if task is None:
                    continue
                try:
                    action = task["action"]
                    rooturl = url = task["url"]
                    while limit >0:
                        print("current page %d"%(limit))
                        if action == "fetch":
                            response = await varcommon.async_fetch_url(url)
                            #if response == None:
                            #    break
                        elif action == "crawl":
                            response = task["response"]
                        if response:
                            print("pagination got response")
                            #print(response)
                            nexttask={}
                            nexttask["action"]="crawl"
                            nexttask["response"]=response
                            nexttask["url"]=rooturl
                            await distq.put(nexttask)
                            print("pagination put next task: %s"%(nexttask))
                            s = Selector(text=response)
                            next = s.xpath(definition["selector"]["xpath"])
                            print(definition["selector"]["xpath"])
                            print(next.xpath("@href"))
                            if next and next.xpath("@href"):
                                url = varcommon.get_url(rooturl, next.xpath("@href").get()) 
                                print("next page's url: %s"%(url))
                            else:
                                break;
                            limit=limit-1
                            print("pagination starts to sleep for %d seconds"%(interim))
                            await gen.sleep(interim)
                            print("pagination wakes")
                        else:
                            print("pagination[%s] missed one page: %s"%(name, url))
                            
                    print("all pages had done!")
                    
                except Exception as e:
                    print("Exception: %s %s" % (e, url))
                #finally:
                #    srcq.task_done()

async def worker(executor,  srcq, distq, name, fp, definition=None):
            async for task in srcq:
                print("worker[%s] got task, action %s"%(name, task["action"]))
                if task is None:
                    continue
                try:
                    action = task["action"]
                    url = task["url"]
                    if action == "fetch":
                        response = await varcommon.async_fetch_url(url)
                        print("sleep for 3 seconds after url fetching......")
                        await gen.sleep(3)
                        #if response == None:
                        #    continue
                    elif action == "crawl":
                        response = task["response"]
                    results, nexttasks = executor.do(response,url)
                    print(results)
                    if nexttasks and len(nexttasks) > 0:
                            print(nexttasks)
                            #await distq.put(nexttasks[0])
                            for nt in nexttasks:
                                await distq.put(nt)
                    #write(results)
                    fp.write(json.dumps(results,ensure_ascii=False))
                    fp.flush()
                except Exception as e:
                    print("Exception: %s %s" % (e, url))
                #finally:
                #    srcq.task_done()
                    
def write(resutls):
    if "phase"+str(resutls["phase"]) in WorkEnv.current_result:
        phase = WorkEnv.current_result["phase"+str(resutls["phase"])]
    else:
        phase = WorkEnv.current_result["phase"+str(resutls["phase"])] = {}
        
    srcurl = resutls["srcurl"]
    if srcurl not in phase:
        phase[srcurl] = {"data":[]}
    phase[srcurl]["data"].append(resutls["data"])
    #phase[srcurl]["phase"] = resutls["phase"]
    #print("WorkEnv.current_result")
    #print(WorkEnv.current_result)
    
class Crawler(object):
    def __init__(self,definition):
        self._definition = definition
        self._phase = definition["phase"]
        self._selectors = definition["selectors"]
        
    def do(self, response, srcurl):
        results={"srcurl":srcurl, "phase":self._phase, "data":[]}
        if response == None:
            return results, None
        
        selector = Selector(text=response)
        nexttask=[]
        data=results["data"]
        for sel in self._selectors:
            xpath = sel["xpath"]
            extract = sel["extract"]
            subxpaths = None
            if "subselectors" in sel:
                subxpaths = sel["subselectors"]
            for idx, item in enumerate(selector.xpath(xpath)):
                kv={}
                kv["no"] = idx+1
                string = item.xpath(".//text()").getall()
                strings = ""
                if string:
                    for s in string:
                        strings = strings+s.strip()
                kv["value"] = varcommon.get_nonblank_words(strings)
                kv["hashcode"] = hash(strings)
                if subxpaths:
                    kv["subvalues"]=[]
                    for subxpath in subxpaths:
                        substrings=item.xpath(subxpath["xpath"]).xpath(".//text()").getall()
                        kv["subvalues"].append(varcommon.get_nonblank_words(substrings))
                        
                if extract==1 and item.xpath("@href"):
                    href = item.xpath("@href").get()
                    kv["href"] = varcommon.get_url(srcurl, href) 
                    nexttask.append({"action":"fetch","url":kv["href"]})
                data.append(kv)
        
        return results, nexttask
    
async def startWorkEnv(addr):
        queue = WorkEnv.Environment["l1"]["srcq"]
        task = {"action":"fetch", "url":addr}
        await queue.put(task)
        
class WorkEnv(object):
    Environment={}
    current_result={}
    
    @staticmethod
    def prepare_env(p_definition, level=1, queue=None):
        #level = 1
        WorkEnv.Environment["l"+str(level)]={}
        WorkEnv.Environment["l"+str(level)]["selectors"]=p_definition["selectors"]
        if "pagination" in p_definition:
            WorkEnv.Environment["l"+str(level)]["pagination"]=p_definition["pagination"]
            
        if queue:
            WorkEnv.Environment["l"+str(level)]["srcq"]=queue
        else:
            WorkEnv.Environment["l"+str(level)]["srcq"]=queues.Queue()

        if "child" in p_definition:
            child = p_definition["child"]
            WorkEnv.Environment["l"+str(level)]["distq"]=queues.Queue()
            WorkEnv.prepare_env(child, level+1, WorkEnv.Environment["l"+str(level)]["distq"])
        print("Prepared work env")
        print(WorkEnv.Environment)
        
    @staticmethod
    def initialize_env():
        phases = WorkEnv.Environment.keys()
        #scheduler = TornadoScheduler()
        for idx, p in enumerate(phases):
            defi = WorkEnv.Environment[p]
            ini={}
            ini["phase"]=idx+1
            fp = open(ApplicationProperties.configure("application.storage.filesystem.uc.rootdir")+"/temp_data/phase"+str(ini["phase"]), 'w+',encoding="utf-8")
            if "pagination" in defi:
                bridgeq = queues.Queue()
                ini["pagination"]=defi["pagination"]
                ini["selectors"]=defi["selectors"]
                interim=ini["pagination"]["interim"]
                print("gen pworkers")
                pworkers = gen.multi([pagination(ini["pagination"], defi["srcq"], bridgeq, "phase["+str(ini["phase"])+"]-pagination["+str(_)+"]") for _ in range(p_concurrency)])
                #print(pworkers)
                print(defi["srcq"])
                print(bridgeq)
                #scheduler.add_job(pagination,'interval', seconds=interim, args=(ini, defi["srcq"], bridgeq))
                cworkers = gen.multi([worker(Crawler(ini), bridgeq, defi["distq"],"phase["+str(ini["phase"])+"]-crawler["+str(_)+"]",fp) for _ in range(c_concurrency)])
                print("gen cworkers")
                print(cworkers)
                print(bridgeq)
                print(defi["distq"])
            else:
                ini["selectors"]=defi["selectors"]
                dq = None
                if "distq" in defi:
                    dq = defi["distq"]
                    print(defi["distq"])
                cworkers = gen.multi([worker(Crawler(ini), defi["srcq"], dq,"phase["+str(ini["phase"])+"]-crawler["+str(_)+"]",fp) for _ in range(c_concurrency)])
                print("gen cworkers")
                print(cworkers)
                print(defi["srcq"])
                
                
class WhaleHandler(RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])

    async def post(self):
        addr = self.args["addr"]
        print("recieve work address: %s"%(addr))
        WorkEnv.prepare_env(self.args)
        WorkEnv.initialize_env()
        await startWorkEnv(addr)

class WhaleWriter(RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self.args = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!', self.request.headers['Content-Type'])

    async def post(self):
        fp = open(ApplicationProperties.configure("application.storage.filesystem.uc.rootdir")+"/data", 'w+',encoding="utf-8")
        fp.write(json.dumps(WorkEnv.current_result,ensure_ascii=False))
        fp.flush()
        fp.close()
        print("Write done!")
                
class WhaleBase(object):
    def __init__(self):
        None

    def start(self):
       print('Application listeming...[Port:%s]'%(ApplicationProperties.configure(p_key='application.bluewhale.server.port')))
       application = Application([ (RequestMapping.whale_collect_start, WhaleHandler),(RequestMapping.whale_collect_write, WhaleWriter),])
       application.listen(ApplicationProperties.configure(p_key='application.bluewhale.server.port'))
       IOLoop.current().start()
        
if __name__ == '__main__':
    ApplicationProperties.populate({"config":"/Users/apple/Documents/var/workspace/var-daemon"})
    '''
    PgConnectionPool.ini(host=ApplicationProperties.configure("application.storage.postgres.connection.host"),
                                                    port=ApplicationProperties.configure("application.storage.postgres.connection.port"),
                                                    user=ApplicationProperties.configure("application.storage.postgres.connection.user"),
                                                    password=ApplicationProperties.configure("application.storage.postgres.connection.password"),
                                                    database=ApplicationProperties.configure("application.storage.postgres.connection.database"))
    '''
    WhaleBase().start()