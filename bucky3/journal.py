

import time
import select
import syslog
import logging
import platform
import datetime
from systemd import journal
import bucky3.module as module
import bucky3.tracing as tracing
import json


class SystemdJournal(module.MetricsSrcProcess, tracing.Tracer):
    event_map = {
        'MESSAGE': 'message',
        '_EXE': 'command',
        '_HOSTNAME': 'host',
        '_MACHINE_ID': 'machine_id',
        '_BOOT_ID': 'boot_id',
        '_PID': 'pid',
        '_UID': 'uid',
        '_GID': 'gid',
        '_SYSTEMD_UNIT': 'systemd_unit',
        'CONTAINER_NAME': 'container_name',
        'CONTAINER_ID': 'container_id'
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
        module.MetricsSrcProcess.__init__(self, *args)
        tracing.Tracer.__init__(self)
        self.json_decoder = json.JSONDecoder()

    def init_cfg(self):
        super().init_cfg()
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
        self.timestamp_window = self.cfg.get('timestamp_window', 60)
        self.bucket_name = self.cfg.get('journal_bucket', 'logs')
        if self.cfg.get('parse_as_json', False):
            self.process_event = self.decode_json

    def flush(self, system_timestamp):
        ret1 = module.MetricsSrcProcess.flush(self, system_timestamp)
        ret2 = tracing.Tracer.flush(self, system_timestamp)
        return ret1 and ret2

    def read_journal(self):
        # https://www.g-loaded.eu/2016/11/26/how-to-tail-log-entries-through-the-systemd-journal-using-python/
        with journal.Reader() as j:
            j.log_level(self.journal_log_level)
            j.this_boot()
            j.this_machine()

            if self.timestamp_window > 0:
                j.seek_realtime(datetime.datetime.utcfromtimestamp(time.time() - self.timestamp_window))
                recv_timestamp = time.time()
                for event in j:
                    self.handle_event(recv_timestamp, event)

            p = select.poll()
            p.register(j.fileno(), j.get_events())
            while True:
                try:
                    if p.poll(3000):
                        if j.process() == journal.APPEND:
                            recv_timestamp = time.time()
                            for event in j:
                                self.handle_event(recv_timestamp, event)
                except InterruptedError:
                    pass

    def loop(self):
        self.start_thread('JournalReadThread', self.read_journal)
        super().loop()

    def process_event(self, event):
        return event

    def decode_json(self, event):
        try:
            message = event['message'].strip()
            processed_event = dict(event)
            del processed_event['message']
            obj, end = self.json_decoder.raw_decode(message)
            if end == len(message) and isinstance(obj, dict) and obj:
                for k, v in obj.items():
                    if k in processed_event:
                        continue
                    if not isinstance(v, (int, float, bool, str)) and v is not None:
                        return event
                    processed_event[k] = v
                return processed_event
        except ValueError:
            pass

        return event

    def handle_event(self, recv_timestamp, event):
        event_severity = event.get('PRIORITY')
        if event_severity is not None:
            if event_severity > self.journal_log_level:
                return
            event_severity = self.syslog_severity_map.get(event_severity, self.syslog_default_severity)

        obj = {}
        for k, v in self.event_map.items():
            if k in event:
                tmp = event[k]
                if isinstance(tmp, (str, int, float, bool)) or tmp is None:
                    obj[v] = tmp
                else:
                    obj[v] = str(tmp)

        message = obj.get('message', '').rstrip()
        if not message:
            return
        obj['message'] = message

        event_facility = event.get('SYSLOG_FACILITY')
        if event_facility is not None:
            # We see things like [b'DHCP4', b'DHCP6'] or b'RFKILL' for event_facility.
            # TODO: should we map those to a default one or pass them along untouched?
            if isinstance(event_facility, int):
                obj['facility'] = self.syslog_facility_map.get(event_facility, self.syslog_default_facility)
            else:
                obj['facility'] = self.syslog_default_facility
        if event_severity is not None:
            obj['severity'] = event_severity

        event_timestamp = event.get('_SOURCE_REALTIME_TIMESTAMP') or event.get('__REALTIME_TIMESTAMP')
        if event_timestamp is None:
            event_timestamp = recv_timestamp
        else:
            event_timestamp = event_timestamp.timestamp()

        self.input(recv_timestamp, event_timestamp, obj)

    def output(self, recv_timestamp, event_timestamp, event):
        event = self.process_event(event)
        self.buffer_metric(self.bucket_name, event, event_timestamp, None)
