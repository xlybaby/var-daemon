# -*- coding: utf-8 -*-
import os,time,sys,json,re,requests
from datetime import datetime, timedelta,date
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag
import urllib3.contrib.pyopenssl

from tornado.httpclient import AsyncHTTPClient

from scrapy.selector import Selector

from fileBasedConfiguration import ApplicationProperties, PgConnectionPool

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
http_client = AsyncHTTPClient()
http_header = {
        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36"
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) VAR/1.0.0.1"
    }

def get_time_partition():
    return 'y20s1'

def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url

def get_links(html):
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
    
def db_write(material, writable):
    conn,cur = PgConnectionPool.getConn()
    try:
        ttime = datetime.now()
        pt = get_time_partition()
        sql = "insert into t_uc_material_data__"+pt+"__"+material["pid"] + " (materialId,seqno,insertDay,contents,insertTime) values (%s,%s,%s,%s,%s) "
        sqldata = (material["materialId"], material["seqno"], date.today(), json.dumps(material["data"]), ttime)
        cur.execute(sql, sqldata)
        conn.commit()
        print("insert material[%d] data: partition:%s, done!"%(task["materialId"],task["pid"]))
    except Exception as e:
        print(e)
    finally:
        PgConnectionPool.release(conn, cur)  
    
def get_url(url, newurl):
    if newurl.lower().startswith("https://") or newurl.lower().startswith("http://"):
        return newurl
    pure_url, frag = urldefrag(newurl)
    return urljoin(url, pure_url) 

def get_charset(html):
    selector = Selector(text=html)
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
    return charset

def get_nonblank_words(stringary):
    pattern = re.compile(r"\S+")
    string = "".join(stringary)
    return "".join( pattern.findall(string) )


async def async_fetch_url(url, connectTimeOut=7,requestTimeOut=14,proxy=None):
    print ("async_fetch_url: %s"%(url))
    if not url:
        return
    
    try:
        headers = {'content-type': 'application/json','User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0'}
        data = {"action":"fetchip"}
        r = requests.post('http://localhost:5015/ip/fetch', data=json.dumps(data), headers=headers)
        res=json.loads(r.text)
        #print("t%d got ip: %s"%(idx,res["ip"]))
        if "ip" in res:
            proxyIp=res["ip"]
            proxyPort = int(res["port"])
            print("proxy ip: %s:%d"%(proxyIp,proxyPort))
        #response = await http_client.fetch(url,method='GET',headers=http_header,connect_timeout=connectTimeOut,request_timeout=requestTimeOut, validate_cert=False)
            response = await http_client.fetch(url,method='GET',headers=http_header,connect_timeout=connectTimeOut,request_timeout=requestTimeOut, proxy_host= proxyIp, proxy_port = proxyPort, proxy_username= "liulingg_1983@126.com", proxy_password="llcoolJB1999",validate_cert=False)
        else:
            response = await http_client.fetch(url,method='GET',headers=http_header,connect_timeout=connectTimeOut,request_timeout=requestTimeOut, validate_cert=False)
    except Exception as e:
        print("Error: %s" % e)
        return None
    else:
        print("fetched %s" % url)
        return response.body
    
