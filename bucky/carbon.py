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
# Copyright 2011 Cloudant, Inc.


import bucky.cfg as cfg
import bucky.common as common


class CarbonClient(common.MetricsPushProcess, common.TCPConnector):
    def __init__(self, *args):
        super().__init__(*args, default_port=2003)

    def flush_buffer(self):
        # For TCP we probably can just pump all buffer into the wire.
        self.socket = self.socket or self.get_tcp_socket(connect=True)
        payload = '\n'.join(self.buffer).encode()
        self.socket.sendall(payload)

    def build_name(self, metadata):
        buf = [metadata[k] for k in cfg.name_mapping if k in metadata]
        return '.'.join(buf)

    def process_values(self, name, values, timestamp, metadata=None):
        metadata = metadata or {}
        metadata.update(name=name)
        for k, v in values.items():
            metadata.update(value=k)
            name = self.build_name(metadata)
            self.buffer.append("%s %s %s" % (name, v, int(timestamp)))
