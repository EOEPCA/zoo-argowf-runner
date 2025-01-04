# this file contains the class that handles the execution of the workflow using hera-workflows and Argo Workflows API
from typing import Callable, Optional, Tuple
import requests
import json
import os
from hera.workflows import WorkflowsService
from loguru import logger
import time
from zoo_argowf_runner.cwl2argo import cwl_to_argo
from zoo_argowf_runner.zoo_helpers import CWLWorkflow


class Execution:
    """
    Handles the execution of workflows using the Hera Workflows library and Argo Workflows API.
    """

    def __init__(
        self,
        namespace: str,
        workflow: CWLWorkflow,
        entrypoint: str,
        workflow_name: str,
        processing_parameters: dict,
        volume_size: str,
        max_cores: int,
        max_ram: int,
        storage_class: str,
        handler: Callable,
    ) -> None:
        """
        Initialize the execution class with workflow and execution parameters.

        :param namespace: Kubernetes namespace where the workflow is executed.
        :param workflow: CWLWorkflow object representing the workflow definition.
        :param entrypoint: Entry point for the workflow.
        :param workflow_name: Unique name for the workflow execution.
        :param processing_parameters: Dictionary of parameters for the workflow execution.
        :param volume_size: Size of the volume for the workflow.
        :param max_cores: Maximum number of CPU cores for the workflow.
        :param max_ram: Maximum RAM (in MiB) for the workflow.
        :param storage_class: Storage class for the workflow.
        :param handler: Callable to handle workflow execution updates.
        """

        self.workflow = workflow
        self.entrypoint = entrypoint
        self.workflow_name = workflow_name
        self.processing_parameters = processing_parameters
        self.volume_size = volume_size
        self.max_cores = max_cores
        self.max_ram = max_ram
        self.storage_class = storage_class
        self.handler = handler

        self.token = os.environ.get("ARGO_WF_TOKEN", None)

        if self.token is None:
            raise ValueError("ARGO_WF_TOKEN environment variable is not set")

        self.namespace = namespace
        self.workflows_service = os.environ.get(
            "ARGO_WF_ENDPOINT", "http://localhost:2746"
        )

        self.completed = False
        self.successful = False

    @staticmethod
    def get_workflow_status(
        workflow_name: str, argo_server: str, namespace: str, token: str
    ) -> Optional[Tuple[str, dict]]:
        """
        Fetch the current status of the workflow using the Argo Workflows API.

        :param workflow_name: Name of the workflow.
        :param argo_server: URL of the Argo Workflows server.
        :param namespace: Kubernetes namespace where the workflow is executed.
        :param token: Bearer token for authentication.
        :return: Tuple containing the status and workflow information.
        """
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        logger.info(
            f"Getting url: {argo_server}/api/v1/workflows/{namespace}/{workflow_name}"
        )
        """Fetches the current status of the workflow."""
        response = requests.get(
            f"{argo_server}/api/v1/workflows/{namespace}/{workflow_name}",
            headers=headers,
            verify=False,  # Use verify=True with valid SSL certificates
        )

        logger.info(f"Workflow status response: {response.status_code}")
        if response.status_code == 200:
            workflow_info = response.json()
            status = workflow_info.get("status", {}).get("phase", "Unknown")
            return status, workflow_info
        else:
            print(f"Failed to retrieve workflow status: {response.status_code}")
            return None
    
    def monitor(self, interval: int = 30, update_function: Optional[Callable] = None) -> None:
        """
        Monitor the execution of the workflow and update the progress.

        :param interval: Time interval (in seconds) between status checks.
        :param update_function: Callable to handle progress updates.
        """

        def progress_to_percentage(progress: str) -> int:
            """Convert progress string (e.g., '2/10') to percentage."""
            completed, total = map(int, progress.split("/"))
            return int((completed / total) * 100)

        while True:
            status, workflow_status = self.get_workflow_status(
                workflow_name=self.workflow_name,
                argo_server=self.workflows_service,
                namespace=self.namespace,
                token=self.token,
            )
            if status:
                logger.info(f"Workflow Status: {status}")

                if update_function and status not in [
                    "Succeeded",
                    "Failed",
                    "Error",
                    "Unknown",
                ]:
                    logger.info(workflow_status.get("status", {}).get("progress"))
                    progress = workflow_status.get("status", {}).get("progress", "0/1")
                    percentage = progress_to_percentage(progress)
                    update_function(percentage, "Argo Workflows is handling the execution")

                # Check if the workflow has completed
                if status in ["Succeeded"]:

                    self.completed = True
                    self.successful = True
                    break

                elif status in ["Failed", "Error"]:
                    self.completed = True
                    self.successful = False
                    logger.info(f"Workflow has completed with status: {status}")
                    break

            time.sleep(interval)

    def is_completed(self) -> bool:
        """Check if the execution is completed."""
        return self.completed

    def is_successful(self) -> bool:
        """Check if the execution was successful."""
        if self.get_execution_output_parameter("outcome") == "succeeded":
            self.successful = True

        if self.get_execution_output_parameter("outcome") == "failure":
            self.successful = False
        
        return self.successful

    def get_execution_output_parameter(self, output_parameter_name: str):
        """
        Retrieve the specified output parameter from the workflow execution.

        :param output_parameter_name: Name of the output parameter.
        :return: Value of the output parameter or None.
        """
        logger.info(f"Retrieving output parameter: {output_parameter_name}")

        _, workflow_status = self.get_workflow_status(
            workflow_name=self.workflow_name,
            argo_server=self.workflows_service,
            namespace=self.namespace,
            token=self.token,
        )

        for output_parameter in (
            workflow_status.get("status")
            .get("nodes")
            .get(self.workflow_name)
            .get("outputs", {})
            .get("parameters", {})  # it's a list
        ):
            if output_parameter.get("name") in [output_parameter_name]:
                return output_parameter.get("value", {})

    def get_output(self):
        """Retrieve the output."""
        return self.get_feature_collection()

    def get_results(self):
        """Retrieve the 'results' output parameter."""
        return self.get_execution_output_parameter("results")

    def get_log(self) -> Optional[str]:
        """Retrieve the 'log' output parameter."""
        return self.get_execution_output_parameter("log")

    def get_usage_report(self):
        """Retrieve the 'usage-report' output parameter."""
        return self.get_execution_output_parameter("usage-report")

    def get_stac_catalog(self) -> Optional[str]:
        """Retrieve the 'stac-catalog' output parameter."""
        return self.get_execution_output_parameter("stac-catalog")

    def get_feature_collection(self) -> Optional[str]:
        """Retrieve the 'feature-collection' output parameter."""
        return self.get_execution_output_parameter("feature-collection")

    def get_tool_logs(self):
        """
        Retrieve tool logs from the workflow execution and save them locally.

        :return: List of paths to saved tool log files.
        """
        usage_report = json.loads(self.get_usage_report())

        tool_logs = []

        for child in usage_report.get("children"):
            logger.info(f"Getting tool logs for step {child.get('name')}")
            response = requests.get(
                f"{self.workflows_service}/artifact-files/{self.namespace}/workflows/{self.workflow_name}/{self.workflow_name}/outputs/tool-logs/{child.get('name')}.log"
            )
            with open(f"{child.get('name')}.log", "w") as f:
                f.write(response.text)
            tool_logs.append(f"{child.get('name')}.log")

        return tool_logs

    def run(self, **kwargs) -> None:
        """
        Create and submit the Argo Workflow object using the CWL definition and execution parameters.
        """
        inputs = {"inputs": self.processing_parameters}

        wf = cwl_to_argo(
            workflow=self.workflow,
            entrypoint=self.entrypoint,
            argo_wf_name=self.workflow_name,
            inputs=inputs,
            volume_size=self.volume_size,
            max_cores=self.max_cores,
            max_ram=self.max_ram,
            storage_class=self.storage_class,
            namespace=self.namespace,
            **kwargs,
        )

        workflows_service = WorkflowsService(
            host=self.workflows_service,
            verify_ssl=None,
            namespace=self.namespace,
            token=self.token,
        )

        wf.workflows_service = workflows_service
        wf.workflows_service.namespace = self.namespace
        wf.create()
