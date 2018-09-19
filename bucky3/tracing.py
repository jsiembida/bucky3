

import re


class CancelTrace(Exception):
    # Raised by tracers to report a false positive.
    # That is, when the tracer first matched the stream but later detected something wrong.
    pass


class PythonTracer:
    """
    https://docs.python.org/3/library/traceback.html

    Traceback (most recent call last):
      File "<stdin>", line 3, in foo
    ValueError: could not convert string to float: 'ads'
    
    During handling of the above exception, another exception occurred:
    
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "<stdin>", line 5, in foo
    ValueError: invalid literal for int() with base 10: 'asdf'
    """

    last_line_regex = re.compile(r'[a-zA-Z][a-zA-Z0-9\.]*')

    def __call__(self, line):
        # Initial matcher cannot raise CancelTrace, only subsequent matchers can.
        if line == 'Traceback (most recent call last):':
            return self._first_line

    def _first_line(self, line):
        if line.startswith('  File') or line.startswith('    '):
            return self._traceback_line
        raise CancelTrace()

    def _traceback_line(self, line):
        if line.startswith('  File') or line.startswith('    '):
            return self._traceback_line
        if self.last_line_regex.match(line):
            return self._last_line
        raise CancelTrace()

    def _last_line(self, line):
        if line.startswith('During handling of the above exception'):
            # Almost identical to __call__ but should cancel if no match
            return self._nested_exception

    def _nested_exception(self, line):
        if line == 'Traceback (most recent call last):':
            return self._first_line
        raise CancelTrace()


class JavaTracer:
    """
    https://docs.oracle.com/javase/8/docs/api/java/lang/Throwable.html#printStackTrace--

     HighLevelException: MidLevelException: LowLevelException
             at Junk.a(Junk.java:13)
             at Junk.main(Junk.java:4)
     Caused by: MidLevelException: LowLevelException
             at Junk.c(Junk.java:23)
             at Junk.b(Junk.java:17)
             at Junk.a(Junk.java:11)
             ... 1 more
     Caused by: LowLevelException
             at Junk.e(Junk.java:30)
             at Junk.d(Junk.java:27)
             at Junk.c(Junk.java:21)
             ... 3 more
    """

    # This doesn't catch all legal exception names, catching all would produce tonnes of false positives.
    first_line_regex = re.compile(r'[a-zA-Z][a-zA-Z0-9]*(\.[a-zA-Z][a-zA-Z0-9$]*)+:')
    backtrace_line_regex = re.compile(r'\s+at ')
    backtrace_last_line_regex = re.compile(r'\s+\.\.\.\s+\d+\s+more')
    caused_by_line_regex = re.compile(r'\s*Caused by:')
    supressed_line_regex = re.compile(r'\s*Suppressed:')

    def __call__(self, line):
        # Initial matcher cannot raise CancelTrace, only subsequent matchers can.
        if line.startswith('Exception in thread '):
            return self._first_line
        if self.first_line_regex.match(line):
            return self._first_line

    def _first_line(self, line):
        if self.backtrace_line_regex.match(line):
            return self._backtrace_line
        raise CancelTrace()

    def _backtrace_line(self, line):
        if self.backtrace_line_regex.match(line):
            return self._backtrace_line
        if self.backtrace_last_line_regex.match(line):
            return self._last_line
        if self.caused_by_line_regex.match(line):
            return self._first_line
        if self.supressed_line_regex.match(line):
            return self._first_line

    def _last_line(self, line):
        if self.caused_by_line_regex.match(line):
            return self._first_line
        if self.supressed_line_regex.match(line):
            return self._first_line


class Tracer:
    def __init__(self):
        self.streams = {}
        self.tracers = (PythonTracer(), JavaTracer())

    def _coalesce_events(self, stream):
        stream_event = dict(stream[0][2])
        coalesced_message = '\n'.join(i[2]['message'] for i in stream)
        stream_event['message'] = coalesced_message
        return stream[0][1], stream_event

    def _find_tracer(self, message):
        # Return the first matched tracer, should work.
        for matcher in self.tracers:
            matcher = matcher(message)
            if matcher is not None:
                return matcher

    def _event_signature(self, event):
        return ':'.join((
            str(event.get('boot_id') or ''),
            str(event.get('uid') or ''),
            str(event.get('gid') or ''),
            str(event.get('pid') or ''),
            str(event.get('command') or ''),
            str(event.get('systemd_unit') or ''),
        ))

    def output(self, recv_timestamp, event_timestamp, event):
        pass  # Overload it in the subclass

    def input(self, recv_timestamp, event_timestamp, event):
        signature = self._event_signature(event)
        q = [(recv_timestamp, event_timestamp, event)]

        while q:
            recv_timestamp, event_timestamp, event = q.pop(0)
            message = event['message']

            if signature in self.streams:
                stream_matcher, stream_events = self.streams[signature]
                next_matcher, first_event, last_event = None, stream_events[0], stream_events[-1]
                first_event_timestamp, last_event_timestamp = first_event[1], last_event[1]
                # Only try to match messages that are no more than 1sec apart
                if last_event_timestamp <= event_timestamp <= last_event_timestamp + 1:
                    try:
                        next_matcher = stream_matcher(message)
                    except CancelTrace:
                        # False positive / malformed trace / etc. We try to recover only if we have a small trace
                        # in buffer (here, up to three lines). This is because retrying is expensive, recovery is nasty
                        # to get right and long traces might be truncated anyway.
                        if len(stream_events) <= 3:
                            self.output(first_event[0], first_event_timestamp, first_event[2])
                            q = stream_events[1:] + q
                            q.append((recv_timestamp, event_timestamp, event))
                            del self.streams[signature]
                            continue
                if next_matcher is None:
                    stream_timestamp, stream_event = self._coalesce_events(stream_events)
                    self.output(first_event[0], stream_timestamp, stream_event)
                    del self.streams[signature]
                else:
                    stream_events.append((recv_timestamp, event_timestamp, event))
                    if len(stream_events) > 1000:
                        stream_events = stream_events[len(stream_events) - 500:]
                    self.streams[signature] = next_matcher, stream_events
                    return

            matcher = self._find_tracer(message)
            if matcher is None:
                self.output(recv_timestamp, event_timestamp, event)
            else:
                self.streams[signature] = matcher, [(recv_timestamp, event_timestamp, event)]

    def flush(self, system_timestamp, force=False):
        stale_streams = []
        for k, (matcher, stream) in self.streams.items():
            last_recv_timestamp = stream[-1][0]
            if force or (system_timestamp - last_recv_timestamp > 1):
                stream_timestamp, stream_event = self._coalesce_events(stream)
                self.output(stream[0][0], stream_timestamp, stream_event)
                stale_streams.append(k)
        for k in stale_streams:
            del self.streams[k]

        return True
