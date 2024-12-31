# Description: This module contains the ZooArgoWorkflowsRunner class which is the main class of the zoo_argowf_runner package.
from datetime import datetime
import uuid
from loguru import logger
import os
from typing import Union
from zoo_argowf_runner.handlers import ExecutionHandler
from zoo_argowf_runner.argo_api import Execution
from zoo_argowf_runner.zoo_helpers import ZooConf, ZooInputs, ZooOutputs, CWLWorkflow
from zoo_argowf_runner.volume import VolumeTemplates

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()


class ZooArgoWorkflowsRunner:
    def __init__(
        self,
        cwl,
        conf,
        inputs,
        outputs,
        execution_handler: Union[ExecutionHandler, None] = None,
    ):
        self.zoo_conf = ZooConf(conf)
        self.inputs = ZooInputs(inputs)
        self.outputs = ZooOutputs(outputs)
        self.cwl = CWLWorkflow(cwl, self.zoo_conf.workflow_id)

        self.handler = execution_handler

        self.storage_class = os.environ.get("STORAGE_CLASS", "standard")
        self.monitor_interval = 30

    def get_volume_size(self) -> str:
        """returns volume size that the pods share"""

        resources = self.cwl.eval_resource()

        # TODO how to determine the "right" volume size
        volume_size = max(
            max(resources["tmpdirMin"] or [0]), max(resources["tmpdirMax"] or [0])
        ) + max(max(resources["outdirMin"] or [0]), max(resources["outdirMax"] or [0]))

        if volume_size == 0:
            volume_size = os.environ.get("DEFAULT_VOLUME_SIZE", "10Gi")
        else:
            volume_size = f"{volume_size}Mi"

        logger.info(f"volume_size: {volume_size}")

        return f"{volume_size}"

    def get_max_cores(self) -> int:
        """returns the maximum number of cores that pods can use"""
        resources = self.cwl.eval_resource()

        max_cores = max(
            max(resources["coresMin"] or [0]), max(resources["coresMax"] or [0])
        )

        if max_cores == 0:
            max_cores = int(os.environ.get("DEFAULT_MAX_CORES"), 4)
        logger.info(f"max cores: {max_cores}")

        return max_cores

    def get_max_ram(self) -> str:
        """returns the maximum RAM that pods can use"""
        resources = self.cwl.eval_resource()
        max_ram = max(max(resources["ramMin"] or [0]), max(resources["ramMax"] or [0]))

        if max_ram == 0:
            max_ram = int(os.environ.get("DEFAULT_MAX_RAM"), 4096)
        logger.info(f"max RAM: {max_ram}Mi")

        return f"{max_ram}Mi"

    def update_status(self, progress: int, message: str = None) -> None:
        """updates the execution progress (%) and provides an optional message"""
        if message:
            self.zoo_conf.conf["lenv"]["message"] = message

        zoo.update_status(self.zoo_conf.conf, progress)

    def get_workflow_id(self):
        """returns the workflow id (CWL entry point)"""
        return self.zoo_conf.workflow_id

    def get_processing_parameters(self):
        """Gets the processing parameters from the zoo inputs"""
        return self.inputs.get_processing_parameters()

    def get_workflow_inputs(self, mandatory=False):
        """Returns the CWL workflow inputs"""
        return self.cwl.get_workflow_inputs(mandatory=mandatory)

    def assert_parameters(self):
        """checks all mandatory processing parameters were provided"""
        return all(
            elem in list(self.get_processing_parameters().keys())
            for elem in self.get_workflow_inputs(mandatory=True)
        )

    def get_workflow_uid(self):
        """returns the workflow unique identifier"""

        def shorten_for_k8s(value: str) -> str:
            """shortens the namespace to 43 characters leaving room for the pod name"""
            while len(value) > 43:
                value = value[:-1]
                while value.endswith("-"):
                    value = value[:-1]
            return value

        return shorten_for_k8s(
            f"{str(self.zoo_conf.workflow_id).replace('_', '-')}-"
            f"{str(datetime.now().timestamp()).replace('.', '')}-{uuid.uuid4()}"
        )

    def execute(self):
        self.update_status(progress=3, message="Pre-execution hook")
        self.handler.pre_execution_hook()

        if not (self.assert_parameters()):
            logger.error("Mandatory parameters missing")
            return zoo.SERVICE_FAILED

        logger.info("execution started")
        self.update_status(progress=5, message="starting execution")

        processing_parameters = {
            **self.handler.get_additional_parameters(),
            **self.get_processing_parameters(),
        }

        logger.info("Processing parameters")
        logger.info(processing_parameters)

        self.update_status(progress=15, message="upload required files")

        self.execution = Execution(
            namespace=self.zoo_conf.conf["auth_env"]["user"],
            workflow=self.cwl,
            entrypoint=self.get_workflow_id(),
            workflow_name=self.get_workflow_uid(),
            processing_parameters=processing_parameters,
            volume_size=self.get_volume_size(),
            max_cores=self.get_max_cores(),
            max_ram=self.get_max_ram(),
            storage_class=self.storage_class,
            handler=self.handler,
        )

        additional_configmaps = [
            VolumeTemplates.create_config_map_volume(
                name="cwl-wrapper-config-vol",
                config_map_name="cwl-wrapper-config",
                items=[
                    {"key": "main.yaml", "path": "main.yaml", "mode": 420},
                    {"key": "rules.yaml", "path": "rules.yaml", "mode": 420},
                    {"key": "stage-in.cwl", "path": "stage-in.cwl", "mode": 420},
                    {"key": "stage-out.cwl", "path": "stage-out.cwl", "mode": 420},
                ],
                default_mode=420,
                optional=False,
            )
        ]

        additional_secrets = [
            VolumeTemplates.create_secret_volume(name="usersettings-vol", secret_name="user-settings")
        ]

        self.execution.run(
            additional_configmaps=additional_configmaps,
            additional_secrets=additional_secrets,
        )

        self.update_status(progress=20, message="execution submitted")

        logger.info("execution")

        # add self.update_status to tell Zoo the execution is running and the progress
        self.execution.monitor(
            interval=self.monitor_interval, update_function=self.update_status
        )

        if self.execution.is_completed():
            logger.info("execution complete")

        if self.execution.is_successful():
            exit_value = zoo.SERVICE_SUCCEEDED
            logger.info(f"execution successful - exit value: {exit_value}")
        else:
            exit_value = zoo.SERVICE_FAILED
            logger.info(f"execution failed - exit value: {exit_value}")

        self.update_status(
            progress=90, message="delivering outputs, logs and usage report"
        )

        logger.info("handle outputs execution logs")
        output = self.execution.get_output()
        logger.info(f"output: {output}")
        log = self.execution.get_log()
        usage_report = self.execution.get_usage_report()
        tool_logs = self.execution.get_tool_logs()
        stac_catalog = self.execution.get_stac_catalog()
        feature_collection = self.execution.get_feature_collection()

        self.outputs.set_output(output)

        self.handler.handle_outputs(
            log=log,
            output=output,
            usage_report=usage_report,
            tool_logs=tool_logs,
            execution=self.execution,
        )

        self.update_status(progress=97, message="Post-execution hook")

        self.handler.post_execution_hook(
            log=log,
            output=output,
            usage_report=usage_report,
            tool_logs=tool_logs,
        )

        self.update_status(
            progress=100,
            message=f'execution {"failed" if exit_value == zoo.SERVICE_FAILED else "successful"}',
        )

        return exit_value
