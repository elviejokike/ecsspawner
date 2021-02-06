import logging
import os
import string
from concurrent.futures import ThreadPoolExecutor
import socket
from time import sleep
import random
import uuid

import boto3
import escapism
from jupyterhub.spawner import Spawner
from tornado import gen
from tornado.platform.asyncio import AnyThreadEventLoopPolicy
from traitlets import Integer, Unicode, Dict
from traitlets.config import LoggingConfigurable
import asyncio

asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

thread_pool = ThreadPoolExecutor(5)


@gen.coroutine
def _run_async(function, *args, **kwargs):
    retries = 10
    logger.info(f"[ASYNC] Running {function} with {args}")
    for retry in range(retries):
        try:
            ret = yield thread_pool.submit(function, *args, **kwargs)
            return ret
        except Exception as e:
            logger.info(
                f"Encountered exception \n{e}\n while attempting to run {function} {args} - retrying {retry}/{retries}"
            )
            yield gen.sleep(1)


class EcsTaskSpawner(Spawner):
    """
    ECS Task Spawner
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        """ Creates and boots a new server to host the worker instance."""
        self.log.info("function create_new_instance %s" % self.user.name)
        self.ecs_client = boto3.client("ecs", region_name="ap-south-1")
        self.ec2_client = boto3.client("ec2", region_name="ap-south-1")

    _executor = None

    strategy = Unicode(
        "ECSxEC2SpawnerHandler",
        config=True,
        help="""
        Indicates if the ECS Spawner mechanism must create an EC2 instance itself, or let ECS to choose one for us.
        """,
    )

    strategy_parms = Dict(
        {},
        config=True,
        help="""
        Strategy parameters.
        """,
    )

    ip = Unicode(
        "0.0.0.0",
        config=True,
        help="""
        The IP address (or hostname) the single-user server should listen on.
        """,
    )

    @property
    def executor(self):
        """single global executor"""
        cls = self.__class__
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(1)
        return cls._executor

    def _get_spawner_handler(self):
        """
        Return the right handler based on the strategy
        :return SpawnerHandler: a handler
        """
        if self.strategy == "ECSxEC2SpawnerHandler":
            return ECSxEC2SpawnerHandler(self, **self.strategy_parms)
        if self.strategy == "ECSSpawnerHandler":
            return ECSSpawnerHandler(self, **self.strategy_parms)
        if self.strategy == "EC2SpawnerHandler":
            return EC2SpawnerHandler(self, **self.strategy_parms)

        raise ValueError("Strategy not properly specified")

    @gen.coroutine
    def start(self, server_type=None):
        self.log.info("function start for user %s" % self.user.name)
        print(self.user_options)
        # handler = self._get_spawner_handler()

        # result = yield self.executor.submit(handler.start)
        #
        # return result.result(timeout=200)

        return (yield self._get_spawner_handler().start(self.user_options))

    @gen.coroutine
    def stop(self, now=False):
        self.log.info("function stop called for %s" % self.user.name)
        self._get_spawner_handler().stop()
        terminated = yield self._get_spawner_handler()._terminate_instance()
        return terminated
        self.clear_state()

    @gen.coroutine
    def poll(self):
        self.log.debug("function poll for user %s" % self.user.name)

        return (yield self._get_spawner_handler().poll())


class SpawnerHandler(LoggingConfigurable):
    """
    Generic Handler
    """

    def __init__(self, spawner, **kwargs):
        self.spawner = spawner
        self.user = spawner.user
        self.hub = spawner.hub
        self.ecs_client = spawner.ecs_client
        self.ec2_client = spawner.ec2_client

    def get_env(self):
        return self.spawner.get_env()

    @gen.coroutine
    def start(self, server_type=None):
        pass

    @gen.coroutine
    def stop(self):
        pass

    @gen.coroutine
    def poll(self):
        pass


class ECSSpawnerHandler(SpawnerHandler):
    """
    Using ECS Task:
    """

    ec2_instance_template = Unicode(
        "",
        config=True,
        help="""
        Name of the EC2 Instance Template to be used when creating a EC2 Instance
        """,
    )

    ec2_instance_template_version = Unicode(
        "",
        config=True,
        help="""
        Version of the EC2 Instance Template to be used when creating a EC2 Instance
        """,
    )

    ecs_task_definition = Unicode(
        "",
        config=True,
        help="""
            Name of the Task Definition to be used when running the task.
        """,
    )

    def __init__(
        self,
        spawner,
        cluster_name,
        ecs_task_definition,
        ec2_instance_template,
        ec2_instance_template_version=13,
        **kwargs,
    ):
        super().__init__(spawner)
        self.cluster_name = cluster_name
        self.ecs_task_definition = ecs_task_definition
        self.ec2_instance_template = ec2_instance_template
        latest_ver = str(
            len(
                self.ec2_client.describe_launch_template_versions(
                    LaunchTemplateName=self.ec2_instance_template
                )["LaunchTemplateVersions"]
            )
        )
        # Always use the latest version
        self.ec2_instance_template_version = latest_ver
        self.thread_pool = ThreadPoolExecutor(5)

        self.port = random.randint(7000, 8000)
        self.task_name = "notebook_task" + str(self.port) + str(uuid.uuid4())


    @gen.coroutine
    def start(self, server_type=None):
        task = yield self.get_task()
        if task is None:
            ip_address = yield self._create_new_task()
            return ip_address, self.port
        raise ValueError("Not handled yet")

    @gen.coroutine
    def stop(self):
        self.log.info("internal stop called")
        task = yield self.get_task()

        # Only Stop the task
        task = self.ecs_client.stop_task(
            cluster=self.cluster_name, task=task["taskArn"]
        )

        self.log.info("stop called successfully")

    def _terminate_instance(self):
        self.log.info("terminate instance called")
        container_instances = self.ecs_client.list_container_instances(
            cluster=self.cluster_name, status="ACTIVE"
        )["containerInstanceArns"]

        self.log.info("container instances:%s" % container_instances)

        container_instances = self.ecs_client.describe_container_instances(
            cluster=self.cluster_name, containerInstances=container_instances
        )["containerInstances"]

        for container_instance in container_instances:
            total_tasks = (
                container_instance["runningTasksCount"]
                + container_instance["pendingTasksCount"]
            )
            self.log.info("total_tasks:%s" % total_tasks)
            agent_connected = container_instance["agentConnected"]
            if agent_connected and total_tasks == 0:
                self.ec2_client.terminate_instances(
                    InstanceIds=[container_instance["ec2InstanceId"]]
                )
                self.log.info(
                    "instance with id %s is stopped"
                    % container_instance["ec2InstanceId"]
                )

    @gen.coroutine
    def poll(self):
        pass

    @gen.coroutine
    def get_task(self):
        tasks = self.ecs_client.list_tasks(
            cluster=self.cluster_name,
            startedBy=self._get_task_identifier(),
            desiredStatus="RUNNING",
        )
        if tasks and len(tasks["taskArns"]) > 0:
            return self.ecs_client.describe_tasks(
                cluster=self.cluster_name, tasks=[tasks["taskArns"][0]]
            )["tasks"][0]
        else:
            return None

    def _get_task_identifier(self):
        """
        Return Task identifier
        :return:
        """
        return "EcsTaskSpawner:" + self.user.name

    @gen.coroutine
    def _create_new_task(self):
        self.log.info("function create new task for user %s" % self.user.name)
        selected_container_instance = yield self._get_user_instance(
            server_type=None
        )
        task_def_arn = yield self._get_task_definition()

        env = self.get_env()
        env["JPY_USER"] = self.user.name
        env["JPY_BASE_URL"] = self.user.server.base_url
        env["JPY_COOKIE_NAME"] = self.user.server.cookie_name

        container_env = self._expand_env(env)

        self.log.info("starting ecs task for user %s" % self.user.name)

        task = self.ecs_client.start_task(
            taskDefinition=task_def_arn,
            cluster=self.cluster_name,
            startedBy=self._get_task_identifier(),
            containerInstances=[
                selected_container_instance["containerInstanceArn"]
            ],
            # overrides={
            #     'containerOverrides': [
            #         {
            #             'name': 'notebook',
            #             'environment': container_env
            #         }
            #     ]
            # },
        )["tasks"][0]
        sleep(5)
        waiter = self.ecs_client.get_waiter("tasks_running")
        waiter.wait(cluster=self.cluster_name, tasks=[task["taskArn"]])

        instance_id = self.ecs_client.describe_container_instances(
            cluster=self.cluster_name,
            containerInstances=[task["containerInstanceArn"]],
        )["containerInstances"][0]["ec2InstanceId"]

        private_ip = self.ec2_client.describe_instances(
            InstanceIds=[
                instance_id,
            ],
        )["Reservations"][0]["Instances"][0]["PrivateIpAddress"]

        self.ecs_client.deregister_task_definition(
            taskDefinition=self.task_name + ":1"
        )
        self.log.info("%s task definition deregistered" % self.task_name)

        return private_ip

    @gen.coroutine
    def _get_task_definition(self):
        """
        Return the Arn of the Task Definition to be used when creating the task
        :return:
        """
        self.log.info(
            "function get task definition for user %s" % self.user.name
        )

        # if self.ecs_task_definition != '':
        #     task_def = self.ecs_client.describe_task_definition(taskDefinition=self.ecs_task_definition)[
        #         'taskDefinition']
        #     return task_def['taskDefinitionArn']

        task_def = {
            "family": self.task_name,
            "volumes": [],
            "networkMode": "bridge",
            "containerDefinitions": [
                {
                    "memory": 30,
                    "cpu": 30,
                    "essential": True,
                    "name": "terminado-image",
                    "image": self.image,
                    "portMappings": [
                        {
                            "containerPort": 8000,
                            "hostPort": self.port,
                            "protocol": "tcp",
                        }
                    ],
                }
            ],
        }

        response = self.ecs_client.register_task_definition(**task_def)
        task_def_arn = response["taskDefinition"]["taskDefinitionArn"]

        return task_def_arn

    def _expand_env(self, env):
        """
        Expand get_env to ECS task environment
        """
        result = []

        if env:
            for key in env.keys():
                entry = {"name": key, "value": env.get(key)}
                result.append(entry)

        return result

    def get_env(self):
        env = super().get_env()

        ip = socket.gethostbyname(socket.gethostname())

        env[
            "JPY_HUB_API_URL"
        ] = f'http://{os.environ.get("HUB_HOST_IP", ip)}:8081/jupyter/hub/api'
        env["JPY_HUB_PREFIX"] = self.hub.server.base_url

        env.update(
            dict(
                JPY_USER=self.user.name,
                JPY_COOKIE_NAME=self.user.server.cookie_name,
                JPY_BASE_URL=self.user.server.base_url,
                JPY_HUB_PREFIX=self.hub.server.base_url,
            )
        )

        return env

    @gen.coroutine
    def _get_user_instance(self, server_type):
        self.log.info(
            "function get user instance for user %s" % self.user.name
        )
        # For now - one instance per user. TODO: Multiple users per instance, based on instance resources.
        instance = yield self._get_container_instance()
        return instance or (yield self._create_instance(server_type))

    @gen.coroutine
    def _create_instance(self, server_type):
        self.log.info(
            f"function create instance for user {self.user.name} template {self.ec2_instance_template}"
        )
        environment_name = os.environ.get("HUB_ENVIRONMENT", "OodleJupyterHub")
        ec2_name = environment_name + "-" + self.user.name

        instance = self.ec2_client.run_instances(
            MinCount=1,
            MaxCount=1,
            InstanceType="t3.small",  # self.server_types.get(server_type['server'][0], 't3.small'),
            LaunchTemplate={
                "LaunchTemplateName": self.ec2_instance_template,
                "Version": self.ec2_instance_template_version,
            },
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": ec2_name},
                        {"Key": "Environment", "Value": environment_name},
                        {
                            "Key": "Project",
                            "Value": os.environ.get(
                                "HUB_PROJECT", "JupyterHUB-Project"
                            ),
                        },
                    ],
                },
            ],
        )["Instances"][0]

        arn = yield self._await_instance_ecs(instance["InstanceId"])

        instance = self.ec2_client.describe_instances(
            InstanceIds=[instance["InstanceId"]]
        )["Reservations"][0]["Instances"][0]
        instance["containerInstanceArn"] = arn
        self.ec2_instance_info = instance
        return instance

    @gen.coroutine
    def _await_instance_ecs(self, instance_id):
        def _wait_instance_registered(self, instance_id):
            while True:
                sleep(5)
                instances = self.ecs_client.list_container_instances(
                    cluster=self.cluster_name,
                    filter=f"ec2InstanceId == {instance_id}",
                )["containerInstanceArns"]
                self.log.info(f"Waiting for >0 instances. Found: {instances}")
                if len(instances) > 0:
                    return instances[0]

        ret = yield self.thread_pool.submit(
            _wait_instance_registered, self, instance_id
        )
        return ret

    @gen.coroutine
    def _get_container_instance(self):
        """
        Look for container instance related to user name
        :return:
        """

        container_instances_arns = self.ecs_client.list_container_instances(
            cluster=self.cluster_name
        )["containerInstanceArns"]
        if len(container_instances_arns) < 1:
            return None
        container_instances = self.ecs_client.describe_container_instances(
            cluster=self.cluster_name,
            containerInstances=container_instances_arns,
        )["containerInstances"]

        for container_instance in container_instances:

            agent_connected = container_instance["agentConnected"]
            total_tasks = (
                container_instance["runningTasksCount"]
                + container_instance["pendingTasksCount"]
            )
            if not agent_connected or total_tasks >= 60:
                continue

            remaining_resources = container_instance["remainingResources"]

            for resource in remaining_resources:
                if resource["name"] == "PORTS":
                    while True:
                        if str(self.port) not in resource["stringSetValue"]:
                            self.task_name = (
                                "terminado_task"
                                + str(self.port)
                                + str(uuid.uuid4())
                            )
                            break
                        else:
                            self.port = random.randint(7000, 8000)

            return {**container_instance}

        return None


class ECSxEC2SpawnerHandler(ECSSpawnerHandler):
    """
    Using single EC2 Instance for every ECS Task
    """

    ec2_instance_template = Unicode(
        "",
        config=True,
        help="""
        Name of the EC2 Instance Template to be used when creating a EC2 Instance
        """,
    )

    ec2_instance_template_version = Unicode(
        "",
        config=True,
        help="""
        Version of the EC2 Instance Template to be used when creating a EC2 Instance
        """,
    )

    port = Integer(
        8000,
        help="""
        Default port to 8000
        """,
    )

    server_types = {
        "xsmall": "t3.small",
        "small": "r5.large",
        "medium": "r5.xlarge",
        "large": "r5.2xlarge",
    }

    def __init__(
        self,
        spawner,
        ec2_instance_template=None,
        ec2_instance_template_version="13",
        port=8000,
        **kwargs,
    ):

        super().__init__(spawner, **kwargs)
        self.ec2_instance_template = ec2_instance_template
        latest_ver = str(
            len(
                self.ec2_client.describe_launch_template_versions(
                    LaunchTemplateName=self.ec2_instance_template
                )["LaunchTemplateVersions"]
            )
        )
        # Always use the latest version
        self.ec2_instance_template_version = latest_ver
        self.thread_pool = ThreadPoolExecutor(5)
        if port:
            self.port = port

    @gen.coroutine
    def start(self, server_type=None):
        print(server_type)
        task = yield self.get_task()
        if task is None:
            ip_address = yield self._create_new_task(server_type)
            return ip_address, self.port
        # TODO

    @gen.coroutine
    def stop(self):
        task = yield self.get_task()
        if task:
            self.ecs_client.stop_task(
                cluster=self.cluster_name, task=task["taskArn"]
            )
            # Stop the Instance Itself
            container_instance_arn = task["containerInstanceArn"]
            container_instance = self.ecs_client.describe_container_instances(
                cluster=self.cluster_name,
                containerInstances=[container_instance_arn],
            )["containerInstances"][0]

            # TODO: Change this when having multiple users per instance
            self.log.info(
                f'Stopping instance {container_instance["ec2InstanceId"]} for user {self.user.name}'
            )
            stop_resp = self.ec2_client.stop_instances(
                InstanceIds=[container_instance["ec2InstanceId"]],
                Hibernate=False,
            )
            self.log.info(stop_resp)
            return True

            # self.ec2_client.terminate_instances(InstanceIds=[
            #     container_instance['ec2InstanceId']
            # ],
            #     DryRun=False
            # )

        else:
            self.log.info(
                "No ECS task found to be stopped %s" % self.user.name
            )

    @gen.coroutine
    def poll(self):
        task = yield self.get_task()
        if task:
            return None  # Still running
        else:
            return 0

    @gen.coroutine
    def _create_new_task(self, server_type):
        self.log.info("function create new task for user %s" % self.user.name)
        task_def_arn = yield self._get_task_definition()

        selected_container_instance = yield self._get_user_instance(
            server_type
        )

        env = self.get_env()
        env["JPY_USER"] = self.user.name
        env["JPY_BASE_URL"] = self.user.server.base_url
        env["JPY_COOKIE_NAME"] = self.user.server.cookie_name

        container_env = self._expand_env(env)

        self.log.info(
            f"starting ecs task for user {self.user.name} / selected instance {selected_container_instance}"
        )

        tries = 0
        max = 7
        task = None
        while True:
            try:
                task = self.ecs_client.start_task(
                    taskDefinition=task_def_arn,
                    cluster=self.cluster_name,
                    startedBy=self._get_task_identifier(),
                    containerInstances=[
                        selected_container_instance["containerInstanceArn"]
                    ],
                    # overrides={
                    #     'containerOverrides': [
                    #         {
                    #             'name': 'notebook',
                    #             'environment': container_env
                    #         }
                    #     ]
                    # },
                )
                if not task.get("failures", False):
                    break
            except Exception as e:
                self.log.info(
                    f"Encountered exception {e} while attempting to launch task for {self.user.name}; ignoring."
                )

            if tries <= max:
                tries += 1
                self.log.info(
                    f'Task launched failed - retrying {tries}/{max}: {task.get("failures") if task else "Exception"}'
                )
                sleep(20)
            else:
                raise Exception(
                    f'Could not launch task for {self.user.name} on {selected_container_instance["containerInstanceArn"]}'
                )

        # self.log.info(task)
        task = task["tasks"][0]
        tries = 0

        try:
            waiter = self.ecs_client.get_waiter("tasks_running")
            waiter.wait(cluster=self.cluster_name, tasks=[task["taskArn"]])
        except Exception as e:
            if tries > max:
                raise e
            self.log.info(f"Waiter exited early {e} - retrying {tries}/{max}")
            tries += 1
            sleep(20)

        self.log.info("ecs task up and running for %s" % self.user.name)

        return selected_container_instance["NetworkInterfaces"][0][
            "PrivateIpAddress"
        ]

    @gen.coroutine
    def _get_user_instance(self, server_type):
        self.log.info(
            "function get user instance for user %s" % self.user.name
        )
        # For now - one instance per user. TODO: Multiple users per instance, based on instance resources.
        instance = yield self._get_container_instance()
        return instance or (yield self._create_instance(server_type))

    @gen.coroutine
    def _create_instance(self, server_type):
        self.log.info(
            f"function create instance for user {self.user.name} template {self.ec2_instance_template}"
        )
        environment_name = os.environ.get("HUB_ENVIRONMENT", "OodleJupyterHub")
        ec2_name = environment_name + "-" + self.user.name

        instance = self.ec2_client.run_instances(
            MinCount=1,
            MaxCount=1,
            InstanceType="t3.small",  # self.server_types.get(server_type['server'][0], 't3.small'),
            LaunchTemplate={
                "LaunchTemplateName": self.ec2_instance_template,
                "Version": self.ec2_instance_template_version,
            },
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": ec2_name},
                        {"Key": "Environment", "Value": environment_name},
                        {
                            "Key": "Project",
                            "Value": os.environ.get(
                                "HUB_PROJECT", "JupyterHUB-Project"
                            ),
                        },
                    ],
                },
            ],
        )["Instances"][0]

        arn = yield self._await_instance_ecs(instance["InstanceId"])

        instance = self.ec2_client.describe_instances(
            InstanceIds=[instance["InstanceId"]]
        )["Reservations"][0]["Instances"][0]
        instance["containerInstanceArn"] = arn
        self.ec2_instance_info = instance
        return instance

    @gen.coroutine
    def _await_instance_ecs(self, instance_id):
        def _wait_instance_registered(self, instance_id):
            while True:
                sleep(5)
                instances = self.ecs_client.list_container_instances(
                    cluster=self.cluster_name,
                    filter=f"ec2InstanceId == {instance_id}",
                )["containerInstanceArns"]
                self.log.info(f"Waiting for >0 instances. Found: {instances}")
                if len(instances) > 0:
                    return instances[0]

        ret = yield self.thread_pool.submit(
            _wait_instance_registered, self, instance_id
        )
        return ret

    @gen.coroutine
    def _get_container_instance(self):
        """
        Look for container instance related to user name
        :return:
        """

        container_instances_arns = self.ecs_client.list_container_instances(
            cluster=self.cluster_name
        )["containerInstanceArns"]
        if len(container_instances_arns) < 1:
            return None
        container_instances = self.ecs_client.describe_container_instances(
            cluster=self.cluster_name,
            containerInstances=container_instances_arns,
        )["containerInstances"]

        for container_instance in container_instances:
            instance = self.ec2_client.describe_instances(
                InstanceIds=[container_instance["ec2InstanceId"]]
            )["Reservations"][0]["Instances"][0]

            if instance.get("Tags") is not None and any(
                x["Key"] == "Name" and self.user.name in x["Value"]
                for x in instance["Tags"]
            ):
                instance_state = instance["State"]["Name"]
                if instance_state == "terminated":
                    continue
                self.log.info(
                    f"found instance for user {self.user.name}\n{instance}"
                )
                if not instance_state == "running":
                    self.log.info(
                        f"starting instance for user {self.user.name}\n{instance}"
                    )
                    self.ec2_client.start_instances(
                        InstanceIds=[container_instance["ec2InstanceId"]]
                    )
                    arn = yield self._await_instance_ecs(
                        instance["InstanceId"]
                    )
                    instance = self.ec2_client.describe_instances(
                        InstanceIds=[container_instance["ec2InstanceId"]]
                    )["Reservations"][0]["Instances"][0]
                self.log.info(
                    f"returning from find_instance.. {instance}\n{container_instance}"
                )
                return {**instance, **container_instance}

        return None

    def _expand_user_properties(self, template):
        # Make sure username and servername match the restrictions for DNS labels
        safe_chars = set(string.ascii_lowercase + string.digits)

        # Set servername based on whether named-server initialised
        servername = ""

        legacy_escaped_username = "".join(
            [s if s in safe_chars else "-" for s in self.user.name.lower()]
        )
        safe_username = escapism.escape(
            self.user.name, safe=safe_chars, escape_char="-"
        ).lower()
        return template.format(
            userid=self.user.id,
            username=safe_username,
            legacy_escape_username=legacy_escaped_username,
            servername=servername,
        )
