# -*- coding: utf-8 -*-
import time,os,json,sys
from datetime import datetime,timedelta

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer
from tornado import httpclient

from scrapy.selector import Selector
parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
from fileBasedConfiguration import ApplicationProperties

_limit_parallel_domain_persecond = {}
_limit_max_domain_perday = {}

async def async_fetch_https(req):
    url = req["url"]
    mid = req["materialId"]
    print('[%s] async_fetch_https | url[%s]'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), url))    
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) VAR/1.0.0.1"
    }
    datadir = ApplicationProperties.configure('application.storage.filesystem.download.tempdir')
    http_client = httpclient.AsyncHTTPClient()
    try:
        response = await http_client.fetch(url,method='GET',headers=header,connect_timeout=ApplicationProperties.configure('application.exchange.server.download.connectTimeOut'),request_timeout=ApplicationProperties.configure('application.exchange.server.download.requestTimeOut'),validate_cert=False)
    except Exception as e:
        print("async_fetch_https Error: %s" % e)
    else:
        selector = Selector(text=response.body)
        #print(response.body)
        metaChr = selector.xpath('//meta/@charset')
        charset = 'utf-8'
        if  metaChr  :
            charset = metaChr.get()                
        else:
            metaChr = selector.xpath("//meta[contains('http-equiv','Content-Type')]/@content")
            if metaChr:
                charsetM=re.search(r'(?<=charset=)\w+',metaChr.get().lower())
                if charsetM:
                    charset=charsetM.group(0)
        print('Page encoding charset is %s'%(charset))
        
        seqno = str(mid)+datetime.now().strftime('%Y%m%d%H%M%S')
        req["writeTime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        req["seqno"] = seqno
        req["charset"] = charset
        ini = open(datadir+"/"+seqno+".ini", 'w+')
        ini.write(json.dumps(req)+"\n")
        
        fp = open(datadir+"/html/"+seqno, 'wb+')
        fp.write(response.body)
       # print(response.data)
        #print(pagebody)
        
        #fp = codecs.open(datadir+"/"+scenarioId, 'w+', encoding=charset)
        #pagebody = response.body.decode(charset)
        #fp.write(pagebody)

async def readTimeline():
    for k,v in _limit_parallel_domain_persecond.items():
        if v:
            for i,j in v.items():
                print("At %s, domain - %s, had %d parallel requests in"%(k,i, j.qsize()))
                #while not j.empty():
                 #   print (j.get()+", ")

    for k,v in _limit_max_domain_perday.items():
        if v:
            for i,j in v.items():
                print("Date:%s, domain - %s, had %d total requests in"%(k, i, j.qsize()))
                #while not j.empty():
                #    print (j.get()+", ")
          
async def timeline():
        if len(_limit_parallel_domain_persecond) <=0:
            starttime = datetime.datetime.now()
        else:
            starttime = datetime.datetime.strptime(list(_limit_parallel_domain_persecond.keys())[-1],'%Y%m%d%H%M%S')
        for i in range(10):
            targettime = starttime + timedelta(seconds=(i+1))
            targettimestr = targettime.strftime("%Y%m%d%H%M%S")
            _limit_parallel_domain_persecond[targettimestr] = {}
        
        daystr = (datetime.now() + timedelta(hours=1)).strftime("%Y%m%d")
        if daystr not in _limit_max_domain_perday:
            _limit_max_domain_perday[daystr] = {}
            
def decode(bMessage):
        return json.loads(bMessage)
    
class LimitServer(TCPServer):

    async def handle_stream(self, stream, address):
        while True:
            try:
                data = await stream.read_until(b"\n")
                args = decode(data)
                print("Recieve client's message:%s"%(args))
                
                #logger.info("Received bytes: %s", data)
                date = datetime.now().strftime("%Y%m%d")
                maxdic = _limit_max_domain_perday[datedic]

                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                paralleldic = _limit_parallel_domain_persecond[timestamp]   
                try:
                    if args["domain"] not in paralleldic:
                        paralleldic[args["domain"]] = Queue(maxsize=_properties.getProperty("application.exchange.server.maxParallelNumPerDomain"))
                    paralleldic[args["domain"]].put(args["compid"],False)
                except Exception as e:
                    print(args["domain"]+" max parallel num reached")
                    print(e)
                    await stream.write(args["domain"]+" max parallel num reached|n".encode())
                    return
                
                try:
                    if args["domain"] not in maxdic:
                        maxdic[args["domain"]] = Queue(maxsize=_properties.getProperty("application.exchange.server.maxNumPerDomainPerDay"))
                    maxdic[args["domain"]].put(args["compid"],False)
                except Exception as e:
                    print(args["domain"]+" max service num reached")
                    print(e)
                    await stream.write(args["domain"]+" max service num reached|n".encode())
                    return
                
                await stream.write("{\"status\":\"200\"}\n".encode())
                await async_fetch_https(args)
            except StreamClosedError:
                #logger.warning("Lost client at host %s", address[0])
                await stream.write("{\"status\":\"500\",\"msg\":\"StreamClosedError\"}\n".encode())
                break
            except Exception as e:
                await stream.write("{\"status\":\"500\",\"msg\":\""+str(e)+"\"}\n".encode())
                print(e)
                
class Server(TCPServer):

    async def handle_stream(self, stream, address):
        while True:
            try:
                data = await stream.read_until(b"\n")
                args = decode(data)
                print("Recieve client's message:%s"%(args))
                
                await stream.write("{\"status\":\"200\"}\n".encode())
                await async_fetch_https(args)
            except StreamClosedError:
                #logger.warning("Lost client at host %s", address[0])
                #await stream.write("{\"status\":\"500\",\"msg\":\"StreamClosedError\"}\n".encode())
                break
            except Exception as e:
                print(e)
                await stream.write("{\"status\":\"500\",\"msg\":\"ApplicationError\"}\n".encode())



class Exchange(object):     
    
    def __init__(self):
        self._server = None
        self._listenPort = None
        self._mode = None
        self._timelineSche = None
        
    def start(self):
        self._listenPort = ApplicationProperties.configure("application.exchange.server.port")
        self._mode = ApplicationProperties.configure("application.exchange.server.needLimitRequest")
        
        if self._mode == "False":
            self._server = Server()
            print("Nolimited Server starting...")
            
        elif self._mode == "True":
            self._timelineSche = TornadoScheduler()
            _scheduler.add_job(timeline,'interval', seconds=5)
            self._timelineSche.start()
            self._server = LimitServer()
            print("Limited Server starting...")
            
        self._server.listen(self._listenPort)
        #if q:
        #    q.put_nowait(self)
            
        IOLoop.current().start()
        
if __name__ == '__main__':
    ApplicationProperties.populate({"config":"/Users/apple/Documents/java/workspace/var-daemon"})
    Exchange().start()