

import time
import select
import syslog
import logging
import platform
import threading
from systemd import journal
import bucky3.module as module


class SystemDJournal(module.MetricsSrcProcess):
    default_event_map = {
        'MESSAGE': 'message',
        'SYSLOG_IDENTIFIER': 'identifier',
        '_EXE': 'command',
        '_HOSTNAME': 'host',
        '_MACHINE_ID': 'machine_id',
        '_BOOT_ID': 'boot_id',
        '_PID': 'pid',
        '_UID': 'uid',
        '_GID': 'gid',
        '_SYSTEMD_UNIT': 'systemd_unit',
    }

    syslog_facility_map = {
        syslog.LOG_AUTH: 'auth',
        syslog.LOG_AUTHPRIV: 'auth',
        syslog.LOG_CRON: 'daemon',
        syslog.LOG_DAEMON: 'daemon',
        syslog.LOG_KERN: 'kernel',
        syslog.LOG_LPR: 'daemon',
        syslog.LOG_MAIL: 'mail',
        syslog.LOG_NEWS: 'mail',
        syslog.LOG_SYSLOG: 'syslog',
    }
    syslog_default_facility = 'user'

    syslog_severity_map = {
        syslog.LOG_EMERG: 'critical',
        syslog.LOG_ALERT: 'critical',
        syslog.LOG_CRIT: 'critical',
        syslog.LOG_ERR: 'error',
        syslog.LOG_WARNING: 'warning',
        syslog.LOG_DEBUG: 'debug',
    }
    syslog_default_severity = 'info'

    def __init__(self, *args):
        assert platform.system() == 'Linux' and platform.release() >= '3'
        super().__init__(*args)

    def init_config(self):
        super().init_config()
        # Imitate Python logging levels, to stay consistent with other modules.
        log_level_map = {
            logging.getLevelName(logging.CRITICAL): syslog.LOG_CRIT,
            logging.getLevelName(logging.ERROR): syslog.LOG_ERR,
            logging.getLevelName(logging.WARNING): syslog.LOG_WARNING,
            logging.getLevelName(logging.INFO): syslog.LOG_INFO,
            logging.getLevelName(logging.DEBUG): syslog.LOG_DEBUG,
            logging.getLevelName(logging.NOTSET): syslog.LOG_DEBUG
        }
        journal_log_level = self.cfg.get('journal_log_level', 'INFO')
        self.journal_log_level = log_level_map.get(journal_log_level, syslog.LOG_INFO)
        self.timestamp_window = self.cfg.get('timestamp_window', 600)
        self.event_map = self.cfg.get('event_map', self.default_event_map)

    def read_journal(self):
        with journal.Reader() as j:
            j.log_level(self.log_level)
            j.this_boot()
            j.this_machine()
            j.seek_realtime(time.time() - self.timestamp_window)
            # https://www.g-loaded.eu/2016/11/26/how-to-tail-log-entries-through-the-systemd-journal-using-python/
            # The article says this call is needed
            # j.get_previous()
            p = select.poll()
            p.register(j.fileno(), j.get_events())

            while True:
                try:
                    if p.poll(1000):
                        if j.process() == journal.APPEND:
                            for event in j:
                                self.handle_event(event)
                except InterruptedError:
                    pass

    def loop(self):
        threading.Thread(name='JournalReadThread', target=self.read_journal).start()
        super().loop()

    def handle_event(self, event):
        event_severity = event.get('PRIORITY')
        if event_severity is not None:
            if event_severity > self.journal_log_level:
                return
            event_severity = self.syslog_severity_map.get(event_severity, self.syslog_default_severity)

        obj = {}
        for k, v in self.event_map.items():
            if k in event:
                tmp = event[k]
                if isinstance(tmp, (int, float, bool)) or tmp is None:
                    obj[v] = tmp
                else:
                    obj[v] = str(tmp)
        if not obj:
            return

        event_facility = event.get('SYSLOG_FACILITY')
        if event_facility is not None:
            obj['facility'] = self.syslog_facility_map.get(event_facility, self.syslog_default_facility)
        if event_severity is not None:
            obj['severity'] = event_severity

        event_timestamp = event.get('_SOURCE_REALTIME_TIMESTAMP') or event.get('__REALTIME_TIMESTAMP')
        if event_timestamp is None:
            event_timestamp = time.time()
        else:
            event_timestamp = event_timestamp.timestamp()

        self.buffer_metric('logs', obj, event_timestamp, None)
