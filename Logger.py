#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'
# !/usr/bin/python
#
# Copyright 2015 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import logging.handlers
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("METRIXA_LOGGING")
logger.setLevel(logging.INFO)
logging.Formatter.converter = time.gmtime

