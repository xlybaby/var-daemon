# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta

from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag
import urllib3

from tornado import gen, httpclient, ioloop, queues,web
from _datetime import datetime

from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

concurrency = 10
inboundq = queues.Queue()
_pgpool = None
_properties = None

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
    response = await httpclient.AsyncHTTPClient().fetch(url)
    print("fetched %s" % url)

    #html = response.body.decode(errors="ignore")
    s = Selector(text=response.body)
    for selector in selectors:
        xpath = selector["xpath"]
        #content.append(s.xpath(xpath).xpath(".//text()").getall())
        for idx,item in enumerate(s.xpath(xpath)):
            contents.append({"parent":parent, "rownum":level+"-"+str(idx), "data": {"value":item.xpath(".//text()").getall()}})
            if selector["extract"] == 1:
                href = item.xpath("@href")
                if href :
                    urls.append({"addr":href.get(),"parent":level+"-"+str(idx)})
                   
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
def write(writable):
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
    
class Crawl(web.RequestHandler):
    def prepare(self):
        if 'Content-Type' not in self.request.headers:
            raise Exception('Unsupported content-type!')
         
        if  self.request.headers['Content-Type'].strip().startswith('application/json'):
            self._reqeustBody = json.loads(self.request.body)
        else:
            raise Exception('Unsupported content-type!',self.request.headers['Content-Type'])

    async def post(self):
        try:
            
            '''
            urls = self._reqeustBody["urls"]
            for url in urls:
                task = {}
                task["url"] = url
                task["materialId"] = self._reqeustBody["materialId"]
                task["partition"] = self._reqeustBody["partition"]
                task["pid"] = self._reqeustBody["pid"]
                task["level"] = self._reqeustBody["level"]
                task["selector"] = self._reqeustBody["selector"]
                task["extract"] = self._reqeustBody["extract"]
                task["extractLinkSelector"] = self._reqeustBody["extractLinkSelector"]
                task["child"] = self._reqeustBody["child"]
            '''
            await inboundq.put(self._reqeustBody);
            self.write({"status":200, "msg":"Mission starts"})
        except Exception as e:
            self.write({"status":500, "msg":e})
        
async def main():
    start = time.time()
    fetching, fetched, dead = set(), set(), set()
    
    properties = ApplicationProperties()
    application = web.Application([ (RequestMapping.submit_fetch_url_task, MainHandler),
                                                                                   (RequestMapping.get_current_fetch_worker_queue_num, dashboard), ])
    application.listen(properties.getProperty(p_key='server.port'))
        
    async def fetch_url(task):
        definition = task["definition"]
        current_url = definition["addr"]
        if current_url in fetching:
            return

        print("fetching %s" % current_url)
        fetching.add(current_url)
        contents,urls = await get_links_from_url(definition)
        if "child" in definition:
            for url in urls:
                    #newurls = [urljoin(url, remove_fragment(new_url)) for new_url in get_links(task["extractLinkSelector"], s)]
                    #for nu in newurls:
                        subtask = {}
                        subtask["materialId"]=task["materialId"]
                        subtask["seqno"]=task["seqno"]
                        subtask["definition"] = {}
                        subtask["definition"]["addr"] = url["addr"]
                        subtask["definition"]["parent"] = url["parent"]
                        subtask["definition"]["selectors"] = definition["child"]["selectors"]
                        subtask["definition"]["level"] = definition["child"]["level"]
                        if "child" in definition["child"]:
                            subtask["definition"]["child"] = definition["child"]["child"]
                        await inboundq.put(subtask)
        writable = {}
        writable["materialId"] = task["materialId"]
        writable["seqno"] = task["seqno"]
        writable["contents"] = contents
        write(writable)
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
                #definition = task["definition"]
                await fetch_url(task)
                
            except Exception as e:
                print("Exception: %s %s" % (e, url))
                dead.add(url)
            finally:
                q.task_done()

    #await q.put(base_url)

    # Start workers, then wait for the work queue to be empty.
    workers = gen.multi([worker() for _ in range(concurrency)])
    '''
    await q.join(timeout=timedelta(seconds=300))
    assert fetching == (fetched | dead)
    print("Done in %d seconds, fetched %s URLs." % (time.time() - start, len(fetched)))
    print("Unable to fetch %s URLS." % len(dead))

    # Signal all the workers to exit.
    for _ in range(concurrency):
        await q.put(None)
    await workers
    '''

class LoadCollectors(object):
    
    def __init__(self):
        None
        
    def run(self):
        urllib3.contrib.pyopenssl.inject_into_urllib3()
        _pgpool = PgConnectionPool(p_properties=_properties)
        _properties = ApplicationProperties()
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(main)