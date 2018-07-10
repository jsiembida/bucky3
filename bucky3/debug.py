

import sys
import pprint
import bucky3.module as module


class DebugOutput(module.MetricsPushProcess):
    pprinter = pprint.PrettyPrinter(stream=sys.stderr, indent=1, width=120, depth=5, compact=False)

    def process_values(self, *args, **kwargs):
        if args:
            self.pprinter.pprint(args)
        if kwargs:
            self.pprinter.pprint(kwargs)
