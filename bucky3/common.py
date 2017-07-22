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


import io
import sys
import logging


def setup_logging(cfg, module_name=None):
    if module_name:
        # Reinit those in subprocesses to avoid races on the underlying streams
        sys.stdout = io.TextIOWrapper(io.FileIO(1, mode='wb', closefd=False))
        sys.stderr = io.TextIOWrapper(io.FileIO(2, mode='wb', closefd=False))
    root = logging.getLogger(module_name)
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(cfg.get('log_level', 'INFO'))
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)-15s][%(levelname)s] %(name)s(%(process)d) - %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return logging.getLogger(module_name)
