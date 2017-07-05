# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2012 Cloudant, Inc.

import multiprocessing
import logging


log = logging.getLogger(__name__)


class Client(multiprocessing.Process):
    def __init__(self, pipe):
        super(Client, self).__init__()
        self.daemon = True
        self.pipe = pipe

    def run(self):
        while True:
            try:
                if not self.pipe.poll(1):
                    self.tick()
                    continue
                sample = self.pipe.recv()
            except KeyboardInterrupt:
                continue
            if sample is None:
                break
            if type(sample[1]) is dict:
                self.send_bulk(*sample)
            else:
                self.send(*sample)

    def send(self, name, value, time, metadata=None):
        raise NotImplementedError()

    def send_bulk(self, name, value, time, metadata=None):
        for k in value.keys():
            if name.endswith('.'):
                metric_name = name + k
            else:
                metric_name = name + '.' + k
            self.send(metric_name, value[k], time, metadata)

    def tick(self):
        pass
