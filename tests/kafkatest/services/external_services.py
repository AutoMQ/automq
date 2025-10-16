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

import subprocess

class DockerComposeService:
    """
    A helper class to manage the lifecycle of an external service
    defined in a docker-compose.yaml file.
    This is NOT a ducktape service and must be managed manually from test code.
    """
    def __init__(self, compose_file_path, logger):
        """
        :param compose_file_path: Path to the docker-compose.yaml file.
        :param logger: The test logger instance.
        """
        self.compose_file_path = compose_file_path
        self.logger = logger

    def start(self, env=None):
        """
        Starts the service using 'docker compose up'.
        :param env: A dictionary of environment variables to pass to the command.
        """
        self.logger.info(f"Manually starting external service from {self.compose_file_path}...")
        self._run_command("up -d", env)

    def stop(self):
        """
        Stops the service using 'docker compose down'.
        """
        self.logger.info(f"Manually stopping external service from {self.compose_file_path}...")
        self._run_command("down --remove-orphans -v")

    def _run_command(self, command, env=None):
        env_prefix = ""
        if env:
            for key, value in env.items():
                env_prefix += f"{key}='{value}' "

        # Use sudo -E to preserve environment variables for the docker compose command.
        cmd = f"{env_prefix} sudo -E docker compose -f {self.compose_file_path} {command}"
        
        try:
            self.logger.info(f"Running command: {cmd}")
            subprocess.check_call(cmd, shell=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to run command: {cmd}. Error: {e}")
            log_cmd = f"{env_prefix} sudo docker compose -f {self.compose_file_path} logs"
            subprocess.run(log_cmd, shell=True)
            raise