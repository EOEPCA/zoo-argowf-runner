# this file contains the class that handles the execution of the workflow using hera-workflows and Argo Workflows API
import requests
import json
import os
from hera.workflows import WorkflowsService
from loguru import logger
import time
from zoo_argowf_runner.cwl2argo import cwl_to_argo
from zoo_argowf_runner.zoo_helpers import CWLWorkflow


class Execution(object):
    def __init__(
        self,
        namespace,
        workflow: CWLWorkflow,
        entrypoint,
        workflow_name,
        processing_parameters,
        volume_size,
        max_cores,
        max_ram,
        storage_class,
        handler,
    ):

        self.workflow = workflow
        self.entrypoint = entrypoint
        self.processing_parameters = processing_parameters
        self.volume_size = volume_size
        self.max_cores = max_cores
        self.max_ram = max_ram
        self.storage_class = storage_class
        self.handler = handler

        self.token = os.environ.get("ARGO_WF_TOKEN", None)

        if self.token is None:
            raise ValueError("ARGO_WF_TOKEN environment variable is not set")

        self.workflow_name = workflow_name
        self.namespace = namespace
        self.workflows_service = os.environ.get(
            "ARGO_WF_ENDPOINT", "http://localhost:2746"
        )

        self.completed = False
        self.successful = False

    @staticmethod
    def get_workflow_status(workflow_name, argo_server, namespace, token):
        # this method gets the status of the workflow using the Argo Workflows API

        # Headers for API request
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

    def monitor(self, interval=30, update_function=None):
        # this method monitors the execution of the workflow using the Argo Workflows API

        def progress_to_percentage(progress):
            # Split the string and convert to integers
            completed, total = map(int, progress.split("/"))

            # Calculate percentage
            percentage = (completed / total) * 100
            return percentage

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
                    progress = progress_to_percentage(
                        workflow_status.get("status", {}).get("progress", {})
                    )
                    update_function(
                        int(progress), "Argo Workflows is handling the execution"
                    )
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

    def is_completed(self):
        # this method checks if the execution is completed
        return self.completed

    def is_successful(self):
        # this method checks if the execution was successful
        return self.successful

    def get_execution_output_parameter(self, output_parameter_name):
        # this method gets the output parameter from the execution using the Argo Workflows API
        logger.info(f"Getting output parameter {output_parameter_name}")

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
            .get("outputs")
            .get("parameters")
        ):
            if output_parameter.get("name") in [output_parameter_name]:
                return output_parameter.get("value")

    def get_output(self):
        # get the results output from the execution using the Argo Workflows API
        self.get_execution_output_parameter("results")

    def get_log(self):
        # get the log output from the execution using the Argo Workflows API
        self.get_execution_output_parameter("log")

    def get_usage_report(self):
        # get the usage report output from the execution using the Argo Workflows API
        self.get_execution_output_parameter("usage-report")

    def get_stac_catalog(self):
        # get the STAC catalog output from the execution using the Argo Workflows API
        self.get_execution_output_parameter("stac-catalog")

    def get_feature_collection(self):
        # get the feature collection output from the execution using the Argo Workflows API
        self.get_execution_output_parameter("feature-collection")

    def get_tool_logs(self):
        # this method gets the tool logs from the execution using the Argo Workflows API

        # Get the usage report
        usage_report = json.loads(self.get_execution_output_parameter("usage-report"))

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

    def run(self):
        # this method creates and submits the Argo Workflow object using the CWL and parameters

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
