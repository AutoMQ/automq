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

    # Path object for the current file
    current_file = Path(__file__)
    # Get the directory of the current file
    project_dir = current_file.parent.parent
    base_path = project_dir.joinpath("results")
    # get the latest e2e tests log folder in results folder
    latest_tests_folder = os.readlink(os.path.join(base_path, "latest")).split('/')[-1]

    post_data = {"msg_type": "interactive", "card": {
        "header": {"template": "green",
                   "title": {"content": "🔈 E2E test results summary", "tag": "plain_text"}},
        "elements": [{"fields": [
            {"is_short": True, "text": {"content": "", "tag": "lark_md"}},
            {"is_short": True, "text": {"content": "", "tag": "lark_md"}},
            {"is_short": True, "text": {"content": "", "tag": "lark_md"}},
            {"is_short": True, "text": {"content": "", "tag": "lark_md"}}], "tag": "div"}
        ]}}
    with open(os.path.join(base_path, latest_tests_folder, "report.json")) as f:
        data = json.load(f)
        post_data['card']['elements'][0]['fields'][0]['text']['content'] = "** ☕️ session_id：**\n %s \n" % \
                                                                           data['session_context']['session_id']
        post_data['card']['elements'][0]['fields'][1]['text']['content'] = "** ▶ run_time_seconds：**\n %.2f \n" % data[
            'run_time_seconds']
        post_data['card']['elements'][0]['fields'][2]['text']['content'] = "** ✅ Passed：**\n %d \n" % data['num_passed']
        post_data['card']['elements'][0]['fields'][3]['text']['content'] = "** ❎ Failed：**\n %d \n" % data['num_failed']
    headers = {'Content-Type': 'application/json'}
    r = requests.post(url=web_hook_url, json=post_data, headers=headers)
    print(r.text)
