"""
This run a deltup server which respond to getdelta.sh [1] script.

Configuration should be provided into HandleResponse class and getdelta.sh
fetch command must honor Content-Disposition in responses.

1: http://linux01.gwdg.de/~nlissne/getdelta-0.7.8.tar.bz2
"""

import os
import sys
PY3 = sys.version_info.major == 3

from os import fork, system, _exit, kill, getpid
from signal import SIGKILL
import subprocess
import json
import errno
import fcntl

if PY3:
    from urllib import parse as urlparse
    from urllib.request import urlretrieve
    from urllib.request import FancyURLopener
else:   
    from urllib2 import urlparse
    from urllib import urlretrieve
    from urllib import FancyURLopener


class NotFoundException(Exception):
    pass

class DtuGenerationFailure(Exception):
    pass

class FileDownloader(FancyURLopener):
    def http_error_404(self, url, fp, errcode, errmsg, headers):
        raise NotFoundException


def is_unicode(v):
    if PY3:
        return isinstance(v, str)
    return isinstance(v, unicode)


def dict_unicode_vals_encoder(d):
    res = {}
    for k, v in d.items():
        if is_unicode(v):
            v = v.encode('utf-8')
        if isinstance(v, dict):
            v = json_loader_dict(v)
        res[k] = v
    return res


def dtu_file_name(have, want):
    return '%s-%s.dtu' % (have, want)


def download_if_file_doesnt_exit(file_name, url, src_dir):
    """return absolute path of result file"""
    file_url = url.rsplit('/', 1)[0] + '/' + file_name
    file_path = os.path.join(src_dir, file_name)
    if os.path.exists(file_path):
        pass
    else:
        fn, hdrs = FileDownloader().retrieve(file_url, file_path)
    return file_path


class LockingFile(object):
    def __init__(self, fp, mode):
        self.f = open(fp, mode)
        fcntl.flock(self.f.fileno(), fcntl.LOCK_EX)
    def __enter__(self):
        return self.f
    def __exit__(self, *exc):
        fcntl.flock(self.f.fileno(), fcntl.LOCK_UN)
        self.f.close()


class HandleResponse(object):
    ROOT_DIR = '/opt/servedelta'
    WORKERS_PID_FILES_PREFIX = ROOT_DIR + '/proc'
    DTU_FILES_DIR = ROOT_DIR + '/files/dtu'
    SOURCE_FILES_READ_DIRECTORY = ROOT_DIR + '/files'
    SOURCE_FILES_WRITE_DIRECTORY = ROOT_DIR + '/files'

    SELF_URL = "http://localhost:7580"
    DTU_FILES_URL_PREFIX = SELF_URL + '/files/dtu'

    def __call__(self, environ, start_response):
        self.wd = dict()
        self.environ = environ
        self.start_response = start_response
        return self.dispatch_request()

    def request_is_failed_dtu_file_generation(self):
        return self.environ['PATH_INFO'].endswith('.failed')

    def request_is_dtu_file(self):
        return self.environ['PATH_INFO'] == '/deltup'

    def redirectToDtuFile(self, file_name):
        self.start_response('301 Moved Permanently', [('Location', self.DTU_FILES_URL_PREFIX + '/' + file_name)])
        return []

    def respondIsQueued(self):
        self.start_response('202 Accepted', [('Content-Disposition', 'attachment; filename=deltup-queued')])
        if self.environ['REQUEST_METHOD'] == 'HEAD':
            return []
        self.load_worker_file()
        return self.wd['STATUS']

    def respondIsFailed(self, dtu_file_name, reason=''):
        self.start_response('404 Not Found', [('Content-Disposition',
                                               'attachment; filename=' + os.path.splitext(dtu_file_name)[0] + '.failed')])
        if self.environ['REQUEST_METHOD'] == 'HEAD':
            return []
        return self.wd['REASON']

    def handleInvalidRequest(self, message):
        self.start_response('504', [('Content-Type', 'text/plain')])
        return message

    def proceed_deltup_request(self):
        self.load_worker_file()
        if self.wd['STATUS'] == b'failed':
            return self.respondIsFailed(self.wd['DTU_FILE_NAME'], self.wd['REASON'])
        elif self.wd['STATUS'] == b'finished':
            os.remove(self.worker_file_path)
            return self.redirectToDtuFile(self.wd['DTU_FILE_NAME'])
        else:
            return self.respondIsQueued()

    @property
    def worker_file_path(self):
        return self.WORKERS_PID_FILES_PREFIX + '/' + self.environ.get('HTTP_X_FORWARDED_FOR', self.environ['REMOTE_ADDR']) + '.lock'

    def load_worker_file(self, raise_on_not_found=True):
        try:
            with LockingFile(self.worker_file_path, 'r') as f:
                self.wd = json.load(f, object_hook=dict_unicode_vals_encoder)
        except (IOError,) as err:
            if err.errno == errno.ENOENT and not raise_on_not_found:
                return False
            else:
                raise err
        return True

    def update_worker_file(self):
        with LockingFile(self.worker_file_path, 'w') as f:
            json.dump(self.wd, f)

    def dtu_file_response(self):
        qsd = urlparse.parse_qs(self.environ['QUERY_STRING'])

        try:
            have = qsd['have'][0]
            want = qsd['want'][0]
            url = qsd['url'][0]
            version = qsd.get('version')
            time = qsd.get('time')
        except (KeyError,) as err:
            return self.handleInvalidRequest(("required parameter `%s' is not provided" % err.message))
        if os.path.exists(self.DTU_FILES_DIR + '/' + dtu_file_name(have, want)):
            return self.redirectToDtuFile(dtu_file_name(have, want))

        self.load_worker_file(False)
        
        if self.wd:
            if self.wd.get('DTU_FILE_NAME') == dtu_file_name(have, want):
                return self.proceed_deltup_request()
            else:
                try:
                    kill(self.wd['PID'], SIGKILL)
                except OSError: pass
                self.wd.clear()
                # TODO: remove files
        
        pid = fork()
        assert (pid != -1)
        if pid:
            self.wd['STATUS'] = 'STARTED'
            self.update_worker_file()
            return self.respondIsQueued()
        else:
            try:
                self.wd['PID'] = getpid()
                self.wd['DTU_FILE_NAME'] = dtu_file_name(have, want)
                self.wd['STATUS'] = 'GET SOURCE FILE'
                self.update_worker_file()

                try:
                    src_file = download_if_file_doesnt_exit(have, url, self.SOURCE_FILES_WRITE_DIRECTORY)
                except NotFoundException:
                    raise DtuGenerationFailure('Source file could not be fetched')

                self.wd['STATUS'] = 'GET DESTINATION FILE'
                self.update_worker_file()

                try:
                    dst_file = download_if_file_doesnt_exit(want, url, self.SOURCE_FILES_WRITE_DIRECTORY)
                except NotFoundException:
                    raise DtuGenerationFailure('Destination file could not be fetched')

                #TODO: put these in config
                p = subprocess.Popen(['deltup', '-d', self.SOURCE_FILES_WRITE_DIRECTORY,
                                                '-D', self.SOURCE_FILES_READ_DIRECTORY,
                                                '-m', '-g', '9', have, want,
                                                self.DTU_FILES_DIR + '/' + self.wd['DTU_FILE_NAME']], 
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     executable='/usr/bin/deltup')
                self.wd['STATUS'] = 'GENERATING DTU FILE'
                self.update_worker_file()
                if p.wait() == 0:
                    self.wd['STATUS'] = 'finished'
                    self.update_worker_file()
                else:
                    raise DtuGenerationFailure('deltup: ' + '\n'.join(p.communicate())+str(h))
                _exit(0)
            except (DtuGenerationFailure, Exception) as err:
                self.wd['STATUS'] = 'failed'
                if isinstance(err, DtuGenerationFailure):
                    self.wd['REASON'] = str(err)
                else:
                    self.wd['REASON'] = ('unknown <%s>' % err)
                self.update_worker_file()

    def dispatch_request(self):
        if self.request_is_failed_dtu_file_generation():
            self.load_worker_file()
            return self.respondIsFailed()
        if self.request_is_dtu_file():
            return self.dtu_file_response()
        self.start_response('404 Not Found', [])
        return b'RESOURCE NOT FOUND'


def create_app():
    return HandleResponse()

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    srv = make_server('127.0.0.1', 7500, create_app())
    srv.serve_forever()
