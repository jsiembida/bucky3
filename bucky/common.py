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


import os
import io
import sys
import string
import logging
import bucky.cfg as cfg


def load_config(config_file, module_name=None):
    new_config = {}
    with open(config_file or cfg.__file__, 'r') as f:
        config_template = string.Template(f.read())
        config_str = config_template.substitute(os.environ)
        exec(config_str, new_config)

    if module_name:
        # Module specific config requested, if exists, should override the globals
        if module_name in new_config:
            module_config = new_config.pop(module_name)
            new_config.update(module_config)

    return new_config


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
