# -*- coding: utf-8 -*-
import os,time,sys,json
from datetime import datetime, timedelta,date

from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag
import urllib3.contrib.pyopenssl

from tornado import gen, httpclient, ioloop, queues,web
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError

from scrapy.selector import Selector

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
    
from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

concurrency = 10
inboundq = queues.Queue()
dataq = {}
http_client = httpclient.AsyncHTTPClient()
http_header = {
        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36"
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) VAR/1.0.0.1"
    }
def geturl(url, newurl):
    if newurl.lower().startswith("https://") or newurl.lower().startswith("http://"):
        return newurl
    pure_url, frag = urldefrag(newurl)
    return urljoin(url, pure_url) 
    #return pure_url

def gettimepartition():
    return 'y20s1'

async def get_links_from_url(definition):
    """Download the page at `url` and parse it for links.
    Returned links have had the fragment after `#` removed, and have been made
    absolute so, e.g. the URL 'gen.html#tornado.gen.coroutine' becomes
    'http://www.tornadoweb.org/en/stable/gen.html'.
    """
    url  = definition["addr"]
    selectors = definition["selectors"]
    level = definition["level"]
    parent = ""
    if "parent" in definition:
        parent = definition["parent"]
    contents = []
    urls = []
    #response = await httpclient.AsyncHTTPClient().fetch(url)
    try:
        response = await http_client.fetch(url,method='GET',headers=http_header,validate_cert=False)
    except Exception as e:
        print("Error: %s" % e)
    else:
        print("fetched %s" % url)
        #print(response.body)

        #html = response.body.decode(errors="ignore")
        s = Selector(text=response.body)
        print(s)
        for selector in selectors:
            xpath = selector["xpath"]
            wrap = definition["wrap"]
            kv={}
            if wrap == 1:
                strings = ""
                kv["url"] = url
                kv["no"] = 1
                for s in s.xpath(xpath).xpath(".//text()").getall():
                    strings = strings+s.strip()
                kv["value"] = strings
                kv["hashcode"] = hash(strings)
                contents.append(kv)       
            else:
                #content.append(s.xpath(xpath).xpath(".//text()").getall())
                for idx,item in enumerate(s.xpath(xpath)):
                    #contents.append({"parent":parent, "rownum":level+"-"+str(idx), "data": {"value":item.xpath(".//text()").getall()}})
                    kv={}
                    kv["url"] = url
                    kv["no"] = idx+1
                    string = item.xpath(".//text()").getall()
                    if string:
                        strings=""
                        for s in string:
                            strings = strings+s.strip()
                        kv["value"] = strings
                        kv["hashcode"] = hash(strings)
                    else:
                        kv["value"] = ""
                        kv["hashcode"] = ""
                    if selector["extract"] == 1:
                        href = item.xpath("@href")
                        if href :
                            kv["href"] = geturl(url, href.get()) 
                            #urls.append({"addr":href.get(),"parent":level+"-"+str(idx)})
                            urls.append(kv["href"])
                    contents.append(kv)       
        #return [urljoin(url, remove_fragment(new_url)) for new_url in get_links(html)]
        return contents,urls

def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url


def get_links(xpath, selector):
    aTags = selector.xpath(xpath["xpath"])
    urls = []
    for tag in aTags:
        href = tag.xpath("@href")
        if href:
            urls.append(href.get())
    return urls
    '''
    class URLSeeker(HTMLParser):
        def __init__(self):
            HTMLParser.__init__(self)
            self.urls = []

        def handle_starttag(self, tag, attrs):
            href = dict(attrs).get("href")
            if href and tag == "a":
                self.urls.append(href)

    url_seeker = URLSeeker()
    url_seeker.feed(html)
    return url_seeker.urls
    '''
def write(task, writable):
    #print("I can write now...")
    #print(writable)
    data={}
    queue = writable["queue"] 
    rootcontents = writable["contents"] 
    childrennum = writable["childrennum"] 
    print("%d article, table list:"%(childrennum))
    #for list in rootcontents:
    #   print(list)
    #print("articles: ")
    try:
        data["tablelist"] = rootcontents
        data["contents"] = {}
        for idx in range(queue.qsize()):
            subcontents = queue.get_nowait()
            for con in subcontents:
                url = con["url"]
                value = con["value"]
                no = con["no"]
                hashcode = con["hashcode"]
                data["contents"][url] = {"value":value, "no":no, "hashcode":hashcode}
                
        print("insert material[%d] data: partition:%s"%(task["materialId"],task["pid"]))
        print(data)
        ttime = datetime.now()
        pt = gettimepartition()
        sql = "insert into t_uc_material_data__"+pt+"__"+task["pid"] + " (materialId,seqno,insertDay,contents,insertTime) values (%s,%s,%s,%s,%s) "
        sqldata = (task["materialId"],task["seqno"],date.today(),json.dumps(data),ttime)
        conn,cur = PgConnectionPool.getConn()
        cur.execute(sql, sqldata)
        conn.commit()
        print("insert material[%d] data: partition:%s, done!"%(task["materialId"],task["pid"]))
    except Exception as e:
        print(e)
    finally:
        PgConnectionPool.release(conn, cur)        
    
    '''
    timestamp = datetime.now()
    week = timestamp.strftime("%W")
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    hour = timestamp.strftime("%H")
    min = timestamp.strftime("%M")
    secs = timestamp.strftime("%S")
    
    materialId = writable["materialId"]
    seqno = writable["seqno"]
    conn,cur = _pgpool.getConnection()
    sql = 'insert into t_uc_material_data_' + writable["partition"] + " (materialId,week,month,year,hour,seconds,minutes,insertTime,parent, seqno,contents) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for content in writable["contents"]:
        if len(content["parent"]) > 0:
            parent = materialId+"-"+seqno+"-"+content["parent"]
        else:
            parent = ""
        data = (materialId,week,month,year,hour,seconds,minutes,timestamp,parent,materialId+"-"+seqno+"-"+content["seqno"],content["data"])
        cur.execute(sql, data)
    pgpool.close(conn, cur)
    '''
class Server(TCPServer):

    async def handle_stream(self, stream, address):
        while True:
            try:
                data = await stream.read_until(b"\n")
                args = json.loads(data)
                print("Recieve client's message:%s"%(args))
                
                await stream.write("{\"status\":\"200\"}\n".encode())
                await inboundq.put(args)
            except StreamClosedError:
                #logger.warning("Lost client at host %s", address[0])
                #await stream.write("{\"status\":\"500\",\"msg\":\"StreamClosedError\"}\n".encode())
                break
            except Exception as e:
                print(e)
                await stream.write("{\"status\":\"500\",\"msg\":\"ApplicationError\"}\n".encode())

class LoadCollectors(object):
    
    def __init__(self):
        None
        
    def run(self):
        urllib3.contrib.pyopenssl.inject_into_urllib3()
        start = time.time()
        fetching, fetched, dead = set(), set(), set()
        
        server = Server()
        server.listen(int(ApplicationProperties.configure("application.mass.server.port")))
        print("Server listening: %s"%(ApplicationProperties.configure("application.mass.server.port")))    
        '''
        properties = ApplicationProperties()
        application = web.Application([ (RequestMapping.submit_fetch_url_task, MainHandler),
                                                                                       (RequestMapping.get_current_fetch_worker_queue_num, dashboard), ])
        application.listen(properties.getProperty(p_key='server.port'))
        '''
        async def fetch_url(task):
            definition = task["definition"]
            current_url = definition["addr"]
            if current_url in fetching:
                return
    
            print("fetching %s" % current_url)
            fetching.add(current_url)
            try:
                contents,urls = await get_links_from_url(definition)
            except Exception as e:
                print("Error: %s" % e)
            else:
                print("after fetching %s" % current_url)
                fetching.remove(current_url)
                print(contents,urls)
                if definition["level"] == 1:
                    if len(urls)>0:
                        dataq[task["seqno"]]["queue"] = queues.Queue(len(urls))
                    dataq[task["seqno"]]["contents"] = contents
                    dataq[task["seqno"]]["childrennum"] = len(urls)
                    print("level 1 had done")
                    print(dataq)
                else:
                    if "queue" in dataq[task["seqno"]]:
                        print("child content had done")
                        print(contents)
                        q = dataq[task["seqno"]]["queue"]
                        q.put(contents)
                        
                        if q.qsize() == dataq[task["seqno"]]["childrennum"]:
                            write(task, dataq[task["seqno"]])
                            
                if "child" in definition:
                    for url in urls:
                            #newurls = [urljoin(url, remove_fragment(new_url)) for new_url in get_links(task["extractLinkSelector"], s)]
                            #for nu in newurls:
                                #subtask = copy.deepcopy(task)
                                subtask = {}
                                subtask["pid"]=task["pid"]
                                subtask["uuid"]=task["uuid"]
                                subtask["type"]=task["type"]
                                subtask["writeTime"]=task["writeTime"]
                                subtask["charset"]=task["charset"]
                                subtask["materialId"]=task["materialId"]
                                subtask["seqno"]=task["seqno"]
                                subtask["definition"] = {}
                                subtask["definition"]["addr"] = url#["addr"]
                                #subtask["definition"]["parent"] = url["parent"]
                                subtask["definition"]["selectors"] = definition["child"]["selectors"]
                                subtask["definition"]["level"] = definition["child"]["level"]
                                subtask["definition"]["wrap"] = definition["child"]["wrap"]
                                if "child" in definition["child"]:
                                    subtask["definition"]["child"] = definition["child"]["child"]
                                await inboundq.put(subtask)
                                
                
                fetched.add(current_url)
            
            #for new_url in urls:
                # Only follow links beneath the base URL
                #if new_url.startswith(base_url):
                #    await q.put(new_url)
            #    await inboundq .put(new_url)
                
        async def worker():
            async for task in inboundq:
                if task is None:
                    return
                try:
                    if task["type"] == 1:
                        definition = task["definition"]
                        if definition["level"] == 1:
                            seqno = task["seqno"]
                            dataq[seqno] = {"queue":None}
                    await fetch_url(task)
                    
                except Exception as e:
                    print("Exception: %s %s" % (e, url))
                    dead.add(url)
                finally:
                    inboundq.task_done()
    
        #await q.put(base_url)
    
        # Start workers, then wait for the work queue to be empty.
        workers = gen.multi([worker() for _ in range(concurrency)])
        ioloop.IOLoop.current().start()
        #io_loop = ioloop.IOLoop.current()
        #io_loop.run_sync(main)
        
if __name__ == '__main__':
    ApplicationProperties.populate({"config":"/Users/apple/Documents/var/workspace/var-daemon"})
    PgConnectionPool.ini(host=ApplicationProperties.configure("application.storage.postgres.connection.host"),
                                                    port=ApplicationProperties.configure("application.storage.postgres.connection.port"),
                                                    user=ApplicationProperties.configure("application.storage.postgres.connection.user"),
                                                    password=ApplicationProperties.configure("application.storage.postgres.connection.password"),
                                                    database=ApplicationProperties.configure("application.storage.postgres.connection.database"))


    LoadCollectors().run()