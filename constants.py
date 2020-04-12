# -*- coding: utf-8 -*-
import sys,os

class RequestMapping(object):
    register_node = r'/register'
    fetch_dashboard = r'/dashboard'
    adjust_collect_job = r'/adjust/collect'
    fetch_collect_dashboard = r'/dashboard/collect'
    whale_collect_start = r'/bluewhale/collect'
    whale_collect_write = r'/bluewhale/write'
    dynamic_ip_assign = r'/ip/fetch'