# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# !/usr/bin/python3

import json
import os
import requests
from pathlib import Path

if __name__ == '__main__':
    web_hook_url = os.getenv('WEB_HOOK_URL')
    title_prefix = os.getenv('REPORT_TITLE_PREFIX')
    title_prefix = title_prefix if title_prefix else ""

    data_map = json.loads(os.getenv('DATA_MAP'))
    artifact_url_prefix = "https://github.com/%s/actions/runs/%s/artifacts/" % (os.getenv('CURRENT_REPO'), os.getenv('RUN_ID'))

    total_passed = 0
    total_failed = 0
    reports_dict = {}

    for key, value in data_map.items():
        if not value:
            continue
        total_failed += int(value['failure-num'])
        total_passed += int(value['success-num'])
        reports_dict[key] = {'num_passed': int(value['success-num']), 'num_failed': int(value['failure-num']), 'run_time_seconds': float(value['run-time-secs']), 'artifact-id': value['artifact-id']}

    if not reports_dict:
        print("No reports found.")
        exit(0)

    post_data = {"msg_type": "interactive", "card": {
        "header": {"template": "green",
                   "title": {"content": "🔈 %s E2E test results summary" % title_prefix, "tag": "plain_text"}},
        "elements": [
            {"fields": [
                {"is_short": True,
                 "text": {"content": "** ✅ Total Passed：**\n %d \n" % total_passed, "tag": "lark_md"}},
                {"is_short": True,
                 "text": {"content": "** ❎ Total Failed：**\n %d \n" % total_failed, "tag": "lark_md"}}],
                "tag": "div"
            },
            {"tag": "hr"},
        ]}}

    for key, value in reports_dict.items():
        post_data['card']['elements'].append(
            {"fields": [
                {"is_short": True, "text": {"content": "** ☕️ suite_id：**\n %s \n" % key, "tag": "lark_md"}},
                {"is_short": True, "text": {"content": "** ▶ run_time_seconds：**\n %.2f \n" % value['run_time_seconds'],
                                            "tag": "lark_md"}},
                {"is_short": True,
                 "text": {"content": "** ✅ Passed：**\n %d \n" % value['num_passed'], "tag": "lark_md"}},
                {"is_short": True,
                 "text": {"content": "** ❎ Failed：**\n %d \n" % value['num_failed'], "tag": "lark_md"}}],
                "tag": "div"
            }
        )
        post_data['card']['elements'].append(
            {
                "actions": [{
                    "tag": "button",
                    "text": {
                        "content": "See more details for %s" % key,
                        "tag": "lark_md"
                    },
                    "url": artifact_url_prefix + value['artifact-id'],
                    "type": "default",
                    "value": {}
                }],
                "tag": "action"
            }
        )
        post_data['card']['elements'].append({"tag": "hr"})

    headers = {'Content-Type': 'application/json'}
    r = requests.post(url=web_hook_url, json=post_data, headers=headers)
    print(r.text)