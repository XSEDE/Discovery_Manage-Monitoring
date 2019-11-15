#!/usr/bin/env python3

# Route Inca/Nagios GLUE2 messages
#   from a source (amqp, file, directory)
#   to a destination (print, directory, warehouse, api)
from __future__ import print_function
from __future__ import print_function
import amqp
import argparse
import base64
import datetime
from datetime import datetime
import json
import logging
import logging.handlers
import os
import pwd
import re
import shutil
import signal
import socket
import ssl
from ssl import _create_unverified_context
import sys
from time import sleep

try:
    import http.client as httplib
except ImportError:
    import httplib

import django
django.setup()
from django.conf import settings
from monitoring_provider.process import Glue2ProcessRawMonitoring, StatsSummary, Glue2DeleteExpiredMonitoring

from daemon import runner
import pdb

class Route_Monitoring():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.altsrc = {}
        self.dest = {}
        for var in ['type', 'obj', 'host', 'port', 'display']:
            self.src[var] = None
            self.altsrc[var] = None
            self.dest[var] = None

        parser = argparse.ArgumentParser(epilog='File|Directory SRC|DEST syntax: {file|directory}:<file|directory path and name')
        parser.add_argument('daemonaction', nargs='?', choices=('start', 'stop', 'restart'), \
                            help='{start, stop, restart} daemon')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {amqp, file, directory} (default=amqp)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {print, directory, warehouse, or api} (default=print)')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_monitoring.conf', \
                            help='Configuration file default=./route_monitoring.conf')
        # Don't set the default so that we can apply the precedence argument || config || default
        parser.add_argument('-q', '--queue', action='store', \
                            help='AMQP queue default=monitoring-router')
        parser.add_argument('--expire', action='store_true', \
                            help='Delete expired monitoring records')
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
        self.config_file = os.path.abspath(self.args.config)
        try:
            with open(self.config_file, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            self.logger.error('Error "%s" parsing config=%s' % (e, self.config_file))
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
#        self.logger = logging.getLogger('DaemonLog')
        self.logger = logging.getLogger('xsede.glue2')
        self.logger.setLevel(numeric_log)
#       self.formatter = logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
#       self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], when='W6', backupCount=999, utc=True)
#       self.handler.setFormatter(self.formatter)
#       self.logger.addHandler(self.handler)

        # Verify arguments and parse compound arguments
        if 'src' not in self.args or not self.args.src: # Tests for None and empty ''
            if 'SOURCE' in self.config:
                self.args.src = self.config['SOURCE']
        if 'src' not in self.args or not self.args.src:
            self.args.src = 'amqp:infopub.xsede.org:5671'
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
        elif self.dest['type'] not in ['print', 'directory', 'warehouse', 'api']:
            self.logger.error('Destination not {print, directory, warehouse, api}')
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
        elif self.dest['type'] == 'warehouse':
            self.dest['display'] = '{}@database={}'.format(self.dest['type'], settings.DATABASES['default']['HOST'])
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
            self.SaveDaemonLog(self.stdout_path)
            self.pidfile_timeout = 5
            if 'PID_FILE' in self.config:
                self.pidfile_path =  self.config['PID_FILE']
            else:
                name = os.path.basename(__file__).replace('.py', '')
                self.pidfile_path =  '/var/run/%s/%s.pid' % (name ,name)

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines=file.read()
                file.close()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '%s.%s' % (path, ts)
                    shutil.copy(path, newpath)
                    print('SaveDaemonLog as ' + newpath)
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.error('Caught signal, exiting...')
        sys.exit(0)

    def ConnectAmqp_UserPass(self):
        ssl_opts = {'ca_certs': os.environ.get('X509_USER_CERT')}
        try:
            host = '{}:{}'.format(self.src['host'], self.src['port'])
            self.logger.info('AMQP connecting to host={} as userid={}'.format(host, self.config['AMQP_USERID']))
            conn = amqp.Connection(login_method='AMQPLAIN', host=host, virtual_host='xsede',
                               userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
                               heartbeat=15,
                               ssl=ssl_opts)
            conn.connect()
            return conn
        except Exception as err:
            self.logger.error('AMQP connect to primary error: ' + format(err))

        alternate = self.config.get('AMQP_FALLBACK', None)
        if not alternate:
            self.logger.error('No AMQP_FALLBACK, quitting...')
            sys.exit(1)

        idx = alternate.find(':')
        if idx > 0:
            (self.altsrc['type'], self.altsrc['obj']) = (alternate[0:idx], alternate[idx+1:])
        else:
            self.altsrc['type'] = alternate
        if self.altsrc['type'] == 'dir':
            self.altsrc['type'] = 'directory'
        elif self.altsrc['type'] not in ['amqp']:
            self.logger.error('Alternate source not {amqp}')
            sys.exit(1)
        idx = self.altsrc['obj'].find(':')
        if idx > 0:
            (self.altsrc['host'], self.altsrc['port']) = (self.altsrc['obj'][0:idx], self.altsrc['obj'][idx+1:])
        else:
            self.altsrc['host'] = self.altsrc['obj']
        if not self.altsrc['port']:
            self.altsrc['port'] = '5671'
        self.altsrc['display'] = '%s@%s:%s' % (self.altsrc['type'], self.altsrc['host'], self.altsrc['port'])

        try:
            host = '%s:%s' % (self.altsrc['host'], self.altsrc['port'])
            self.logger.info('AMQP connecting to host={} as userid={}'.format(host, self.config['AMQP_USERID']))
            conn = amqp.Connection(login_method='AMQPLAIN', host=host, virtual_host='xsede',
                               userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
                               heartbeat=15,
                               ssl=ssl_opts)
            conn.connect()
            return conn
        except Exception as err:
            self.logger.error('AMQP connect to alternate error: ' + format(err))
            self.logger.error('Quitting...')
            sys.exit(1)

    def ConnectAmqp_X509(self):
        ssl_opts = {'ca_certs': self.config['X509_CACERTS'],
                   'keyfile': '/path/to/key.pem',
                   'certfile': '/path/to/cert.pem'}
        conn = amqp.Connection(login_method='EXTERNAL', host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede',
                               heartbeat=15, ssl=ssl_opts)
        conn.connect()
        return conn

    def src_amqp(self):
        return

    def dest_print(self, st, doctype, resourceid, message_body):
        print('{} exchange={}, routing_key={}, size={}, dest=PRINT'.format(st, doctype, resourceid, len(message_body) ) )
        if self.dest['obj'] != 'dump':
            return
        try:
            py_data = json.loads(message_body)
        except ValueError as e:
            self.logger.error('Parsing Exception: %s' % (e))
            return
        for key in py_data:
            print('  Key=' + key)

    def dest_directory(self, st, doctype, resourceid, message_body):
        dir = os.path.join(self.dest['obj'], doctype)
        if not os.access(dir, os.W_OK):
            self.logger.critical('%s exchange=%s, routing_key=%s, size=%s Directory not writable "%s"' %
                  (st, doctype, resourceid, len(message_body), dir ) )
            return
        file_name = resourceid + '.' + st
        file = os.path.join(dir, file_name)
        self.logger.info('%s exchange=%s, routing_key=%s, size=%s dest=file:<exchange>/%s' %
                  (st, doctype, resourceid, len(message_body), file_name ) )
        with open(file, 'w') as fd:
            fd.write(message_body)
            fd.close()

    def dest_restapi(self, st, doctype, resourceid, message_body):
        global response, data
        if doctype not in ['inca','nagios']:
            self.logger.debug('exchange=%s, routing_key=%s, size=%s dest=DROP' %
                  (doctype, resourceid, len(message_body) ) )
            return

        if doctype in ['inca']:
            data = json.loads(message_body)
            if 'rep:report' in data:
                self.logger.debug('exchange=%s, routing_key=%s, size=%s discarding old format' %
                      (doctype, resourceid, len(message_body) ) )
                return

            try:
                resourceid = data['TestResult']['Associations']['ResourceID']
            except:
                self.logger.error('exchange=%s, routing_key=%s, size=%s missing Associations->ResourceID' %
                                  (doctype, resourceid, len(message_body) ) )
                return

        elif doctype in ['nagios']:
            data = json.loads(message_body)
            try:
                resourceid = data['TestResult']['Associations']['ResourceID']
            except:
                self.logger.error('exchange=%s, routing_key=%s, size=%s missing Associations->ResourceID' %
                                  (doctype, resourceid, len(message_body) ) )
                return

        headers = {'Content-type': 'application/json',
            'Authorization': 'Basic %s' % base64.standard_b64encode( (self.config['API_USERID'] + ':' + self.config['API_PASSWORD']).encode() ).decode() }
        url = '/monitoring-provider-api/v1/process/doctype/%s/resourceid/%s/' % (doctype, resourceid)
        if self.dest['host'] not in ['localhost', '127.0.0.1'] and self.dest['port'] != '8000':
            url = '/wh1' + url
#  Updated for Python 3.6 upgrade
#        (host, port) = (self.dest['host'].encode('utf-8'), self.dest['port'].encode('utf-8'))
        (host, port) = (self.dest['host'], self.dest['port'])
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
                self.logger.info('RESP exchange=%s, routing_key=%s, size=%s dest=POST http_response=status(%s)/reason(%s)' %
                    (doctype, resourceid, len(message_body), response.status, response.reason ) )
                data = response.read()
                conn.close()
                break
            except (socket.error) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception socket.error to %s:%s; sleeping %s/minutes before retrying' % \
                                  (host, port, sleepminutes))
                sleep(sleepminutes*60)
            except (httplib.BadStatusLine) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception httplib.BadStatusLine to %s:%s; sleeping %s/minutes before retrying' % \
                                  (host, port, sleepminutes))
                sleep(sleepminutes*60)

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

    def dest_warehouse(self, ts, doctype, resourceid, message_body):
        proc = Glue2ProcessRawMonitoring(application=os.path.basename(__file__), function='dest_warehouse')
        (code, message) = proc.process(ts, doctype, resourceid, message_body)
#        if code is False:
#            self.logger.error(message)
#        else:
#            self.logger.info(message)

    def process_file(self, path):
        file_name = path.split('/')[-1]
        if file_name[0] == '.':
            return
        
        idx = file_name.rfind('.')
        resourceid = file_name[0:idx]
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
            doctype = 'glue2.applications'
        elif 'ComputingManager' in py_data or 'ComputingService' in py_data or \
            'ExecutionEnvironment' in py_data or 'Location' in py_data or 'ComputingShare' in py_data:
            doctype = 'glue2.compute'
        elif 'ComputingActivity' in py_data:
            doctype = 'glue2.computing_activities'
        elif 'TestResult' in py_data:
            doctype = py_data['TestResult']['Extension']['Source'].lower()
            resourceid = py_data['TestResult']['Associations']['ResourceID']
        else:
            self.logger.error('Document type not recognized: ' + path)
            return
        self.logger.info('Processing file: ' + path)

        if self.dest['type'] == 'api':
            self.dest_restapi(ts, doctype, resourceid, data)
        elif self.dest['type'] == 'warehouse':
            self.dest_warehouse(ts, doctype, resourceid, data)
        elif self.dest['type'] == 'print':
            self.dest_print(ts, doctype, resourceid, data)

    # Where we process
    def amqp_callback(self, message):
        st = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        doctype = message.delivery_info['exchange']
        tag = message.delivery_tag
        resourceid = message.delivery_info['routing_key']
        if self.dest['type'] == 'print':
            self.dest_print(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'directory':
            self.dest_directory(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'warehouse':
            self.dest_warehouse(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'api':
            self.dest_restapi(st, doctype, resourceid, message.body)
        self.channel.basic_ack(delivery_tag=tag)

        self.warehouse_expire()

    def amqp_consume_setup(self):
        now = datetime.utcnow()
        try:
            if (now - self.amqp_consume_setup_last).seconds < 300:  # 5 minutes
                self.logger.error('Too recent amqp_consume_setup, quitting...')
                sys.exit(1)
        except SystemExit:
            raise
        except:
            pass
        self.amqp_consume_setup_last = now

        self.conn = self.ConnectAmqp_UserPass()
        self.channel = self.conn.channel()
        self.channel.basic_qos(prefetch_size=0, prefetch_count=4, a_global=True)
        which_queue = self.args.queue or self.config.get('QUEUE', 'monitoring-router')
        queue = self.channel.queue_declare(queue=which_queue, durable=True, auto_delete=False).queue
        exchanges = ['inca','nagios']
        for ex in exchanges:
            self.channel.queue_bind(queue, ex, '#')
        self.logger.info('AMQP Queue={}, Exchanges=({})'.format(which_queue, ', '.join(exchanges)))
        st = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        self.channel.basic_consume(queue, callback=self.amqp_callback)
    
    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)

        self.logger.info('Starting program=%s pid=%s, uid=%s(%s)' % \
                     (os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))
        self.logger.info('Source: ' + self.src['display'])
        self.logger.info('Destination: ' + self.dest['display'])
        self.logger.info('Config: ' + self.config_file)
        
        if self.args.expire:
            self.expirer = Glue2DeleteExpiredMonitoring(interval = 3600)
        
        if self.src['type'] == 'amqp':
            self.amqp_consume_setup()
            while True:
                try:
                    self.conn.drain_events(timeout=15)
                    self.conn.heartbeat_tick()
                    sleep(5)
                    continue # Loops back to the while
                except (socket.timeout):
                    self.logger.info('AMQP drain_events timeout, heartbeat_tick')
                    self.conn.heartbeat_tick()
                    sleep(5)
                    continue
                except Exception as err:
                    self.logger.error('AMQP drain_events error: ' + format(err))
                try:
                    self.conn.close()
                except Exception as err:
                    self.logger.error('AMQP connection.close error: ' + format(err))
                sleep(30)   # Sleep a little and then try to reconnect
                self.amqp_consume_setup()

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

    def warehouse_expire(self):
        if self.args.expire:
            (code, message) = self.expirer.delete()
            if not code:
                self.logger.error('Expirer reported: {}'.format(message))
            elif self.args.verbose and message:
                self.logger.info('Expirer reported: {}'.format(message))

if __name__ == '__main__':
    router = Route_Monitoring()
    if router.args.daemonaction is None:
        # Interactive execution
        myrouter = router.run()
        sys.exit(0)

# Daemon execution
    daemon_runner = runner.DaemonRunner(router)
    daemon_runner.daemon_context.files_preserve=[router.logger.handlers[0].stream]
    daemon_runner.daemon_context.working_directory=router.config['RUN_DIR']
    daemon_runner.do_action()
