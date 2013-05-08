from magic import Magic
from collections import defaultdict

import time
import os
import psutil

_MAGIC = Magic()

class SingleProcessInfo(object):
    def __init__(self, process=None, stat_freq=10.0, cpu_percent_freq=2.0):
        self.pid = process.pid
        self.stat_freq = stat_freq
        self.cpu_percent_freq = cpu_percent_freq

        self._open_file_stats = {}
        self._last_cpu_percent = {'time_of_check':0.0, 'value':0.0}
        self._process = process
        self.last = 0.0

    def get_basic_info(self):
        p = self._process
        basic_info = {}
        try:
            p = self._process

            basic_info = {
                    'cmdline': p.cmdline,
                    'parent_pid': p.ppid,
                    'pid': p.pid,
                    'working_directory': p.getcwd()
                    }
        except psutil._error.Error:
            pass
        return basic_info

    def get_process_status(self):
        info = {}
        try:
            info.update(self._get_general_info())
            info.update(self._get_thread_info())
            info.update(self._get_memory_info())
            info.update(self._get_cpu_info())
            info.update(self._get_file_info())
        except psutil._error.Error:
            info['is_running'] = self._process.is_running()
        return info

    def _get_file_info(self):
        p = self._process
        ofi = defaultdict(dict)
        for openfile in p.get_open_files():
            real_path = openfile.path
            fd = openfile.fd

            file_key = str((real_path, fd))

            fdinfo = self.get_fdinfo(fd)
            if (file_key in self._open_file_stats and
                    (fdinfo['read_only'] or not self.should_stat(file_key))):
                file_stats = self._open_file_stats[file_key]
            else:
                file_stats = self.get_file_stats(real_path)
                self._open_file_stats[file_key] = file_stats

            ofi[real_path][fd] = file_stats
            ofi[real_path][fd].update(fdinfo)
        return {'open_files':ofi}

    def _get_general_info(self):
        p = self._process
        info = {}
        info['pid'] = p.pid
        info['time'] = time.time()
        info['is_running'] = p.is_running()
        return info

    def _get_thread_info(self):
        threads = self._process.get_threads()
        ti = {}
        for thread in threads:
            ti[thread.id] = {
                    'user_time': thread.user_time,
                    'system_time': thread.system_time
                    }
        return {'threads':ti}

    def _get_memory_info(self):
        p = self._process
        info = {}
        info['memory_percent'] = p.get_memory_percent()
        info['memory_rss'], info['memory_vms'] = p.get_memory_info()
        return info

    def _get_cpu_info(self):
        info = {}
        info['cpu_percent'] = self._get_cpu_percent()
        info['cpu_user'], info['cpu_system'] = self._process.get_cpu_times()
        return info

    def _get_cpu_percent(self):
        if self.should_update_cpu_percent():
            self._last_cpu_percent = {'time_of_check':time.time(),
                    'value':self._process.get_cpu_percent(interval=0.0)}
        cpu_percent = self._last_cpu_percent['value']
        return cpu_percent

    @staticmethod
    def _should(time_of_last_thing, max_freq):
        now = time.time()
        period = time.time() - time_of_last_thing
        freq = 1.0/(period + 0.00000001)
        return freq <= max_freq

    def should_update_cpu_percent(self):
        return self._should(self._last_cpu_percent['time_of_check'],
                self.cpu_percent_freq)

    def should_stat(self, file_key):
        last = self._open_file_stats[file_key]['time_of_stat']
        return self._should(last, self.stat_freq)

    @staticmethod
    def get_file_stats(real_path):
        time_of_stat = time.time()
        size = os.stat(real_path).st_size
        file_type = _MAGIC.from_file(real_path)
        return {'size':size, "type":file_type,
                "time_of_stat":time_of_stat}

    def get_fdinfo(self, fd):
        fdinfo_path = self.fdinfo_path(fd)
        try:
            fdinfo = open(fdinfo_path).readlines()
        except IOError:
            fdinfo = []

        info = {}
        for line in fdinfo:
            if line.startswith("pos:"):
                fields = line.split()
                info["pos"] = int(fields[1])
            elif line.startswith("flags:"):
                fields = line.split()
                flags = int(fields[1])
                info["flags"] = flags
                info["read_only"] = flags % 10 == 0
        return info

    def fdinfo_path(self, fd):
        return "/proc/%d/fdinfo/%d" % (self.pid, fd)

    def fd_path(self, fd):
        return "/proc/%d/fd/%d" % (self.pid, fd)


class ParentProcessInfo(SingleProcessInfo):
    def __init__(self, update_children_freq=1.0, **kwargs):
        SingleProcessInfo.__init__(self, **kwargs)
        self.children = {}
        self.update_children_freq = update_children_freq
        self._last_children_update = 0.0

    def get_process_status(self):
        self.update_children()

        my_status = SingleProcessInfo.get_process_status(self)
        my_pid = self.pid

        info = {my_pid: my_status}
        for pid, child in self.children.iteritems():
            info[pid] = child.get_process_status()

        return info

    def update_children(self):
        if self._should(self._last_children_update, self.update_children_freq):
            self._last_children_update = time.time()

            self._children = self._process.get_children(recursive=True)

            for _child in self._children:
                if _child.pid not in self.children.keys():
                    self.children[_child.pid] = SingleProcessInfo(_child)
