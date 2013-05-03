from flow.util.process_info import ParentProcessInfo
from twisted.internet import reactor
from twisted.web.resource import Resource, NoResource
from twisted.web.server import Site
from twisted.web.static import File

import json
import logging
import os
import psutil
import socket

LOG = logging.getLogger(__name__)

class RootResource(Resource):
    def __init__(self, process_info):
        Resource.__init__(self)

        html_root = os.path.join(os.path.dirname(__file__), "web")

        self.putChild("basic", BasicResource(process_info))
        self.putChild("status", StatusResource(process_info))
        self.putChild("view", File(html_root))

class JSONResource(Resource):

    def __init__(self, process_info):
        Resource.__init__(self)
        self.process_info = process_info

    def get_data(self, process_info):
        raise NotImplementedError

    def render_GET(self, request):
        request.setHeader('Access-Control-Allow-Origin', '*')
        request.setHeader('Access-Control-Allow-Methods', 'GET')
        request.setHeader('Content-type', 'application/json')

        data = self.get_data()
        return json.dumps(data)

class BasicResource(JSONResource):
    def get_data(self):
        return self.process_info.get_basic_info()

    def getChild(self, name, request):
        try:
            pid = int(name)
        except ValueError:
            return NoResource()

        if pid == self.process_info.pid:
            return BasicLeafResource(self.process_info)
        elif pid in self.process_info.children.keys():
            return BasicLeafResource(self.process_info.children[pid])
        else:
            return NoResource()

class BasicLeafResource(BasicResource):
    isLeaf = True

class StatusResource(JSONResource):
    isLeaf = True
    def get_data(self):
        return self.process_info.get_process_status()

class ProcessMonitor(object):
    def __init__(self, pid):
        self.pid = pid

    def start(self):
        process = psutil.Process(os.getpid())
        process_info = ParentProcessInfo(process=process)

        factory = Site(RootResource(process_info))
        # FIXME fixed port
        iport = reactor.listenTCP(8889, factory)
        listen_port = iport.getHost().port
        listen_host = socket.gethostname()

        LOG.info("Process Monitor at http://%s:%s/view", listen_host, listen_port)
