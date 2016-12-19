#!/usr/bin/env python

# Route GLUE2 messages from a source (amqp, file, directory) to a destination (print, directory, api)
from __future__ import print_function
from __future__ import print_function
import os
import sys
import argparse
import logging
import signal
import datetime
from time import sleep
import base64
import amqp
import json
import socket
import ssl
from ssl import _create_unverified_context
from daemon import runner
import pdb

try:
    import http.client as httplib
except ImportError:
    import httplib

curr_folder = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, '%s/../../django_xsede_warehouse' % curr_folder)

# using components of Django "standalone"
import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'xsede_warehouse.settings'
django.setup()
from monitoring_provider.process import Glue2Process,Glue2NewDocument,StatsSummary

Monitoring_Handled_Types = ('.general.', '.gram.','.gridftp.', '.gsissh.')

class Monitoring():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.dest = {}
        for var in ['type', 'obj', 'host', 'port', 'display']:
            self.src[var] = None
            self.dest[var] = None

        parser = argparse.ArgumentParser(epilog='File|Directory SRC|DEST syntax: {file|directory}:<file|directory path and name')
        parser.add_argument('daemonaction', nargs='?', choices=('start', 'stop', 'restart'), \
                            help='{start, stop, restart} daemon')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {amqp, file, directory} (default=amqp)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {print, directory, direct, or api} (default=print)')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_monitoring.conf', \
                            help='Configuration file default=./route_monitoring.conf')
        parser.add_argument('--verbose', action='store_true', \
                            help='Verbose output')
        parser.add_argument('--daemon', action='store_true', \
                            help='Daemonize execution')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        config_file = os.path.abspath(self.args.config)
        try:
            with open(config_file, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            self.logger.error('Error "%s" parsing config=%s' % (e, config_file))
            sys.exit(1)

        # Initialize logging
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'INFO', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: %s' % numeric_log)
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(numeric_log)
        self.formatter = logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.FileHandler(self.config['LOG_FILE'])
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
        if 'src' not in self.args or not self.args.src: # Tests for None and empty ''
            if 'SOURCE' in self.config:
                self.args.src = self.config['SOURCE']
        if 'src' not in self.args or not self.args.src:
            self.args.src = 'amqp:info1.dyn.xsede.org:5671'
            # self.args.src = 'amqp:info1.dyn.xsede.org:5672'
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['type'], self.src['obj']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            self.src['type'] = self.args.src
        if self.src['type'] == 'dir':
            self.src['type'] = 'directory'
        elif self.src['type'] not in ['amqp', 'file', 'directory']:
            self.logger.error('Source not {amqp, file, directory}')
            sys.exit(1)
        if self.src['type'] == 'amqp':
            idx = self.src['obj'].find(':')
            if idx > 0:
                (self.src['host'], self.src['port']) = (self.src['obj'][0:idx], self.src['obj'][idx+1:])
            else:
                self.src['host'] = self.src['obj']
            if not self.src['port']:
                self.src['port'] = '5671'
            self.src['display'] = '%s@%s:%s' % (self.src['type'], self.src['host'], self.src['port'])
        elif self.src['obj']:
            self.src['display'] = '%s:%s' % (self.src['type'], self.src['obj'])
        else:
            self.src['display'] = self.src['type']

        if 'dest' not in self.args or not self.args.dest:
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if 'dest' not in self.args or not self.args.dest:
            self.args.dest = 'print'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['type'], self.dest['obj']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['type'] = self.args.dest
        if self.dest['type'] == 'dir':
            self.dest['type'] = 'directory'
        elif self.dest['type'] not in ['print', 'directory', 'direct', 'api']:
            self.logger.error('Destination not {print, directory, direct, api}')
            sys.exit(1)
        if self.dest['type'] == 'api':
            idx = self.dest['obj'].find(':')
            if idx > 0:
                (self.dest['host'], self.dest['port']) = (self.dest['obj'][0:idx], self.dest['obj'][idx+1:])
            else:
                self.dest['host'] = self.dest['obj']
            if not self.dest['port']:
                self.dest['port'] = '443'
            self.dest['display'] = '%s@%s:%s' % (self.dest['type'], self.dest['host'], self.dest['port'])
        elif self.dest['obj']:
            self.dest['display'] = '%s:%s' % (self.dest['type'], self.dest['obj'])
        else:
            self.dest['display'] = self.dest['type']

        if self.src['type'] in ['file', 'directory'] and self.dest['type'] == 'directory':
            self.logger.error('Source {file, directory} can not be routed to Destination {directory}')
            sys.exit(1)

        if self.dest['type'] == 'directory':
            if not self.dest['obj']:
                self.dest['obj'] = os.getcwd()
            self.dest['obj'] = os.path.abspath(self.dest['obj'])
            if not os.access(self.dest['obj'], os.W_OK):
                self.logger.error('Destination directory=%s not writable' % self.dest['obj'])
                sys.exit(1)
        if self.args.daemonaction:
            self.stdin_path = '/dev/null'
            if 'LOG_FILE' in self.config:
                self.stdout_path = self.config['LOG_FILE'].replace('.log', '.daemon.log')
                self.stderr_path = self.stdout_path
            else:
                self.stdout_path = '/dev/tty'
                self.stderr_path = '/dev/tty'
            self.pidfile_timeout = 5
            if 'PID_FILE' in self.config:
                self.pidfile_path =  self.config['PID_FILE']
            else:
                name = os.path.basename(__file__).replace('.py', '')
                self.pidfile_path =  '/var/run/%s/%s.pid' % (name ,name)


    def exit_signal(self, signal, frame):
        self.logger.error('Caught signal, exiting...')
        sys.exit(0)

    def ConnectAmqp_Anonymous(self):
        self.src['port'] = '5672'
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede')
    #                           heartbeat=2)

    def ConnectAmqp_UserPass(self):
        ssl_opts = {'ca_certs': os.environ.get('X509_USER_CERT')}
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede',
                               userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
    #                           heartbeat=1,
                               ssl=ssl_opts)

    def ConnectAmqp_X509(self):
        ssl_opts = {'ca_certs': self.config['X509_CACERTS'],
                   'keyfile': '/path/to/key.pem',
                   'certfile': '/path/to/cert.pem'}
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede',
    #                           heartbeat=2,
                               ssl=ssl_opts)

    def src_amqp(self):
        return

    def amqp_callback(self, message):
        st = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        type = message.delivery_info['exchange']
        tag = message.delivery_tag
        resource = message.delivery_info['routing_key']
        if self.dest['type'] == 'print':
            self.dest_print(st, type, resource, message.body)
        elif self.dest['type'] == 'directory':
            self.dest_directory(st, type, resource, message.body)
        elif self.dest['type'] == 'api':
            self.dest_restapi(st, type, resource, message.body)
        elif self.dest['type'] == 'direct':
            self.dest_direct(st, type, resource, message.body)
        self.channel.basic_ack(delivery_tag=tag)

    def dest_print(self, st, type, resource, message_body):
        print('%s exchange=%s, routing_key=%s, size=%s, dest=PRINT' %
            (st, type, resource, len(message_body) ) )
        if self.dest['obj'] != 'dump':
            return
        try:
            py_data = json.loads(message_body)
        except ValueError as e:
            self.logger.error('Parsing Exception: %s' % (e))
            return
        for key in py_data:
            print('  Key=%s' % key)

    def dest_directory(self, st, type, resource, message_body):
        dir = os.path.join(self.dest['obj'], type)
        if not os.access(dir, os.W_OK):
            self.logger.critical('%s exchange=%s, routing_key=%s, size=%s Directory not writable "%s"' %
                  (st, type, resource, len(message_body), dir ) )
            return
        file_name = resource + '.' + st
        file = os.path.join(dir, file_name)
        self.logger.info('%s exchange=%s, routing_key=%s, size=%s dest=file:<exchange>/%s' %
                  (st, type, resource, len(message_body), file_name ) )
        with open(file, 'w') as fd:
            fd.write(message_body)
            fd.close()

    def dest_restapi(self, st, type, resource, message_body):
        global response, data
        if type in ['glue2.computing_activity']:
            self.logger.debug('exchange=%s, routing_key=%s, size=%s dest=DROP' %
                  (type, resource, len(message_body) ) )
            return

        if type in ['inca']:
            """
            r_idx = resource.find('.')
            status = resource[0:r_idx]
            resource = resource[r_idx+1:len(resource)]
            resource_id = 'unknown'
            name = 'unknown'

            for stype in Monitoring_Handled_Types:
                if stype in resource:
                    r_idx = resource.find(stype)
                    name = resource[0:r_idx]
                    resource_id = resource[r_idx+len(stype):len(resource)]
                    self.logger.debug('status: %s, resource_id: %s, name: %s, resource: %s' % \
                        (status, resource_id, name, resource))

            data = message_body
            data = json.loads(data)
            if 'rep:report' in data:
                xsede_is = {}
                xsede_is['ID'] = resource
                #xsede_is['ResourceID'] = resource_id
                xsede_is['Name'] = name
                data['XSEDE_IS'] = xsede_is
                message_body = json.dumps(data)
                resource = resource_id
            """
            data = json.loads(message_body)
            if not 'rep:report' in data:
                resource = data['TestResult']['Associations']['ResourceID']
            else:
                self.logger.debug('exchange=%s, routing_key=%s, size=%s discarding old format' %
                      (type, resource, len(message_body) ) )
                return

        elif type in ['nagios']:
            data = json.loads(message_body)
            resource = data['TestResult']['Associations']['ResourceID']

        headers = {'Content-type': 'application/json',
            'Authorization': 'Basic %s' % base64.standard_b64encode( self.config['API_USERID'] + ':' + self.config['API_PASSWORD']) }
        url = '/monitoring-provider-api/v1/process/doctype/%s/resourceid/%s/' % (type, resource)
        if self.dest['host'] not in ['localhost', '127.0.0.1'] and self.dest['port'] != '8000':
            url = '/wh1' + url
        (host, port) = (self.dest['host'].encode('utf-8'), self.dest['port'].encode('utf-8'))
        retries = 0
        while retries < 100:
            try:
                if self.dest['port'] == '443':
    #                ssl_con = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, capath='/etc/grid-security/certificates/')
    #                ssl_con.load_default_certs()
    #                ssl_con.load_cert_chain('certkey.pem')
                    ssl_con = ssl._create_unverified_context(check_hostname=False, \
                                                             certfile=self.config['X509_CERT'], keyfile=self.config['X509_KEY'])
                    conn = httplib.HTTPSConnection(host, port, context=ssl_con)
                else:
                    conn = httplib.HTTPConnection(host, port)
                self.logger.debug('POST %s' % url)
                conn.request('POST', url, message_body, headers)
                response = conn.getresponse()
                self.logger.info('exchange=%s, routing_key=%s, size=%s dest=POST http_response=status(%s)/reason(%s)' %
                    (type, resource, len(message_body), response.status, response.reason ) )
                data = response.read()
                conn.close()
                break
            except (socket.error) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Failed API POST: %s (retrying in %s/minutes)' % (e, sleepminutes))
                sleep(sleepminutes*60)
            except httplib.BadStatusLine as e:
                self.logger.error('Exception "%s" on POST of type="%s" and resource="%s"' % \
                                  (type(e).__name__, type, resource))
                break

        if response.status in [400, 403]:
            self.logger.error('response=%s' % data)
            return
        try:
            obj = json.loads(data)
    #        if isinstance(obj, dict):
    #            self.logger.info(StatsSummary(obj))
    #        else:
    #            self.logger.error('Response %s' % obj)
    #            raise ValueError('')
        except ValueError as e:
            self.logger.error('API response not in expected format (%s)' % e)

    def dest_direct(self, ts, type, resource, message_body):
        if type in ['inca']:
            data = message_body
            data = json.loads(data)
            """
            if 'rep:report' in data:
                r_idx = resource.find('.')
                status = resource[0:r_idx]
                resource = resource[r_idx+1:len(resource)]
                resource_id = 'unknown'
                name = 'unknown'

                for stype in Monitoring_Handled_Types:
                    if stype in resource:
                        r_idx = resource.find(stype)
                        name = resource[0:r_idx]
                        resource_id = resource[r_idx+len(stype):len(resource)]
                        print (
                        'status: %s, resource_id: %s, name: %s, resource: %s' % (status, resource_id, name, resource))

                xsede_is = {}
                xsede_is['ID'] = resource
                #xsede_is['ResourceID'] = resource_id
                xsede_is['Name'] = name
                data['XSEDE_IS'] = xsede_is
                #message_body = json.dumps(data)
                resource = resource_id

                doc = Glue2Process()
                result = doc.process(type,resource,data)
                print (StatsSummary(result))
            else:
            """
            if not 'rep:report' in data:
                doc = Glue2Process()
                resource = data['TestResult']['Associations']['ResourceID']
                result = doc.process(type, resource, data)
                print(StatsSummary(result))

        elif type in ['nagios']:
            doc = Glue2Process()
            data = json.loads(message_body)
            resource = data['TestResult']['Associations']['ResourceID']
            result = doc.process(type,resource,data)
            print (StatsSummary(result))

    def process_file(self, path):
        file_name = path.split('/')[-1]
        if file_name[0] == '.':
            return
        
        idx = file_name.rfind('.')
        resource = file_name[0:idx]
        ts = file_name[idx+1:len(file_name)]
        with open(path, 'r') as file:
            data=file.read().replace('\n','')
            file.close()
        try:
            py_data = json.loads(data)
        except ValueError as e:
            self.logger.error('Parsing "%s" Exception: %s' % (path, e))
            return

        if 'ApplicationEnvironment' in py_data or 'ApplicationHandle' in py_data:
            type = 'glue2.applications'
        elif 'ComputingManager' in py_data or 'ComputingService' in py_data or \
            'ExecutionEnvironment' in py_data or 'Location' in py_data or 'ComputingShare' in py_data:
            type = 'glue2.compute'
        elif 'ComputingActivity' in py_data:
            type = 'glue2.computing_activities'
        elif 'TestResult' in py_data:
            type = py_data['TestResult']['Extension']['Source'].lower()
            resource = py_data['TestResult']['Associations']['ResourceID']
        else:
            self.logger.error('Document type not recognized: ' + path)
            return
        self.logger.info('Processing file: ' + path)

        if self.dest['type'] == 'api':
            self.dest_restapi(ts, type, resource, data)
        elif self.dest['type'] == 'direct':
            self.dest_direct(ts,type,resource,data)
        elif self.dest['type'] == 'print':
            self.dest_print(ts, type, resource, data)
    
    # Where we process
    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)

        self.logger.info('Starting program=%s pid=%s, uid=%s(%s)' % \
                     (os.path.basename(__file__), os.getpid(), os.geteuid(), os.getlogin()))
        self.logger.info('Source: ' + self.src['display'])
        self.logger.info('Destination: ' + self.dest['display'])

        if self.src['type'] == 'amqp':
            conn = self.ConnectAmqp_UserPass()
            # conn = self.ConnectAmqp_Anonymous()

            self.channel = conn.channel()
            declare_ok = self.channel.queue_declare(queue='monitoring-router')
            queue = declare_ok.queue
            self.channel.queue_bind(queue,'inca','#')
            self.channel.queue_bind(queue,'nagios','#')

            # st = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            # self.logger.info('Binding to queues=(%s)' % ', '.join(queues))
            self.channel.basic_consume(queue,callback=self.amqp_callback)
            while True:
                self.channel.wait()

        elif self.src['type'] == 'file':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isfile(self.src['obj']):
                self.logger.error('Source is not a readable file=%s' % self.src['obj'])
                sys.exit(1)
            self.process_file(self.src['obj'])

        elif self.src['type'] == 'directory':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isdir(self.src['obj']):
                self.logger.error('Source is not a readable directory=%s' % self.src['obj'])
                sys.exit(1)
            for file1 in os.listdir(self.src['obj']):
                fullfile1 = os.path.join(self.src['obj'], file1)
                if os.path.isfile(fullfile1):
                    self.process_file(fullfile1)
                elif os.path.isdir(fullfile1):
                    for file2 in os.listdir(fullfile1):
                        fullfile2 = os.path.join(fullfile1, file2)
                        if os.path.isfile(fullfile2):
                            self.process_file(fullfile2)

if __name__ == '__main__':
    #how to run
    # python route_monitoring.py -d api:localhost:8000
    router = Monitoring()
    if router.args.daemonaction is None:
        # Interactive execution
        myrouter = router.run()
        sys.exit(0)

# Daemon execution
    daemon_runner = runner.DaemonRunner(router)
    daemon_runner.daemon_context.files_preserve=[router.handler.stream]
    daemon_runner.daemon_context.working_directory=router.config['RUN_DIR']
    daemon_runner.do_action()
