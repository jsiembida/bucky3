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

from setuptools import setup
from bucky3 import __version__

setup(
    name='bucky3',
    version=__version__,

    description='Monitoring agent for Linux and Docker with StatsD, InfluxDB, Prometheus and Elasticsearch support',
    author='Jarek Siembida',
    author_email='jarek.siembida@gmail.com',
    license='ASF2.0',
    url='http://github.com/jsiembida/bucky3.git',

    classifiers=[
        'Development Status :: 5 - Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: BSD',
        'Operating System :: Unix',
        'Programming Language :: Python',
        "Programming Language :: Python :: 3",
        'Topic :: Internet :: Log Analysis',
        'Topic :: System :: Networking :: Monitoring',
        'Topic :: Utilities',
    ],
    zip_safe=False,
    packages=['bucky3'],
    include_package_data=True,

    entry_points="""\
    [console_scripts]
    bucky3=bucky3.main:main
    """
)
