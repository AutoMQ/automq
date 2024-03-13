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
    show_results_url = os.getenv('SHOW_RESULTS_URL')
    storage_path = os.getenv('STORAGE_PATH')
    title_prefix = os.getenv('REPORT_TITLE_PREFIX')
    title_prefix = title_prefix if title_prefix else ""

    # iterate all the folders in the storage path
    base_path = Path(storage_path)
    all_tests_folder = sorted(base_path.iterdir(), key=os.path.getmtime)

    total_passed = 0
    total_failed = 0
    reports_dict = {}

    for path in all_tests_folder:
        if not path.is_dir():
            continue
        suite_id = path.name
        with open(os.path.join(path, "report.json")) as f:
            data = json.load(f)
            total_failed += data['num_failed']
            total_passed += data['num_passed']
            reports_dict[suite_id] = {'num_passed': data['num_passed'], 'num_failed': data['num_failed'],
                                      'run_time_seconds': data['run_time_seconds']}

    if not reports_dict:
        print("No reports found in %s" % storage_path)
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
                    "url": "http://%s/kafka/%s" % (show_results_url, key),
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
