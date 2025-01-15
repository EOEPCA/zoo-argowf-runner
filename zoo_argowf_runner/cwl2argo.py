# Description: This file contains the function to convert a CWL workflow to an Argo workflow.
from __future__ import annotations
import os
from typing import Optional

from hera.workflows.models import (
    Parameter,
    Quantity,
    ResourceRequirements,
    ScriptTemplate,
    TemplateRef,
)

from zoo_argowf_runner.template import WorkflowTemplates
from zoo_argowf_runner.zoo_helpers import CWLWorkflow
from zoo_argowf_runner.volume import VolumeTemplates


def cwl_to_argo(
    workflow: CWLWorkflow,
    entrypoint: str,
    argo_wf_name: str,
    inputs: Optional[dict] = None,
    volume_size: Optional[str] = "10Gi",
    max_cores: Optional[int] = 4,
    max_ram: Optional[str] = "4Gi",
    storage_class: Optional[str] = "standard",
    namespace: Optional[str] = "default",
    **kwargs,
):
    """
    Converts a CWLWorkflow object to an Argo Workflow specification.

    Args:
        workflow (CWLWorkflow): The CWL workflow to be converted.
        entrypoint (str): The entrypoint step in the CWL workflow.
        argo_wf_name (str): The name for the Argo workflow.
        inputs (Optional[dict]): Input parameters for the workflow execution.
        volume_size (Optional[str]): Size of the volume to be used by the workflow.
        max_cores (Optional[int]): Maximum CPU cores allowed for the workflow.
        max_ram (Optional[str]): Maximum memory allowed for the workflow.
        storage_class (Optional[str]): The storage class for volume claims.
        namespace (Optional[str]): Kubernetes namespace to run the workflow in.

    Returns:
        dict: An Argo workflow specification generated from the CWL workflow.
    """

    prepare_content = f"""
import json

content = json.loads(\"\"\"{workflow.raw_cwl}\"\"\".replace("'", '"'))

inputs = "{{{{inputs.parameters.inputs}}}}"

parameters = json.loads(inputs.replace("'", '"'))

with open("/tmp/cwl_workflow.json", "w") as f:
    json.dump(content, f)

with open("/tmp/cwl_parameters.json", "w") as f:
    json.dump(parameters.get("inputs"), f)

"""

    annotations = {
        "workflows.argoproj.io/version": ">= v3.3.0",
    }

    annotations["workflows.argoproj.io/title"] = workflow.get_label()
    annotations["workflows.argoproj.io/description"] = workflow.get_doc()
    annotations["eoap.ogc.org/version"] = workflow.get_version()
    annotations["eoap.ogc.org/title"] = workflow.get_label()
    annotations["eoap.ogc.org/abstract"] = workflow.get_doc()

    vl_claim_t_list = [
        VolumeTemplates.create_volume_claim_template(
            name="calrissian-wdir",
            storage_class_name=storage_class,
            storage_size=volume_size,
            access_modes=["ReadWriteMany"],
        ),
    ]

    config_map_vl_list = []

    if "additional_configmaps" in kwargs:
        config_map_vl_list.extend(kwargs["additional_configmaps"])

    secret_vl_list = []

    if "additional_secrets" in kwargs:
        secret_vl_list.extend(kwargs["additional_secrets"])

    #    secret_volume(name="usersettings-vol", secretName="user-settings")
    # ]

    workflow_sub_step = [
        WorkflowTemplates.create_workflow_step(
            name="prepare",
            template="prepare",
            parameters=[
                {"name": key, "value": f"{{{{inputs.parameters.{key}}}}}"}
                for key in ["inputs"]
            ],
        ),
        WorkflowTemplates.create_workflow_step(
            name="argo-cwl",
            template_ref=TemplateRef(
                name=os.environ.get("ARGO_CWL_RUNNER_TEMPLATE", "argo-cwl-runner"),
                template=os.environ.get(
                    "ARGO_CWL_RUNNER_ENTRYPOINT", "calrissian-runner"
                ),
            ),
            parameters=[
                Parameter(name="entry_point", value=entrypoint),
                Parameter(name="max_ram", value=max_ram),
                Parameter(name="max_cores", value=max_cores),
                Parameter(
                    name="parameters",
                    value="{{ steps.prepare.outputs.parameters.inputs }}",
                ),
                Parameter(
                    name="cwl", value="{{ steps.prepare.outputs.parameters.workflow }}"
                ),
            ],
            continue_on={"error": "true"},
        ),
    ]

    templates = [
        WorkflowTemplates.create_template(
            name=entrypoint,
            sub_steps=workflow_sub_step,
            inputs_parameters=[{"name": key} for key in ["inputs"]],
            outputs_parameters=[
                {
                    "name": "results",
                    "expression": "steps['argo-cwl'].outputs.parameters['results']",
                },
                {
                    "name": "log",
                    "expression": "steps['argo-cwl'].outputs.parameters['log']",
                },
                {
                    "name": "usage-report",
                    "expression": "steps['argo-cwl'].outputs.parameters['usage-report']",
                },
                {
                    "name": "stac-catalog",
                    "expression": "steps['argo-cwl'].outputs.parameters['stac-catalog']",
                },
                {
                    "name": "feature-collection",
                    "expression": "steps['argo-cwl'].outputs.parameters['feature-collection']",
                },
                {
                    "name": "outcome",
                    "expression": "steps['argo-cwl'].outputs.parameters['outcome']",
                },
            ],
            outputs_artifacts=[
                {
                    "name": "tool-logs",
                    "from_expression": "steps['argo-cwl'].outputs.artifacts['tool-logs']",
                },
                {
                    "name": "calrissian-output",
                    "from_expression": "steps['argo-cwl'].outputs.artifacts['calrissian-output']",
                },
                {
                    "name": "calrissian-stderr",
                    "from_expression": "steps['argo-cwl'].outputs.artifacts['calrissian-stderr']",
                },
                {
                    "name": "calrissian-report",
                    "from_expression": "steps['argo-cwl'].outputs.artifacts['calrissian-report']",
                },
            ],
        ),
        WorkflowTemplates.create_template(
            name="prepare",
            inputs_parameters=[{"name": key} for key in ["inputs"]],
            outputs_parameters=[
                {"name": "inputs", "path": "/tmp/cwl_parameters.json"},
                {"name": "workflow", "path": "/tmp/cwl_workflow.json"},
            ],
            script=ScriptTemplate(
                image="docker.io/library/python:3.9",
                resources=ResourceRequirements(
                    requests={"memory": Quantity(__root__="1Gi"), "cpu": int(1)}
                ),
                volume_mounts=[],
                command=["python"],
                source=prepare_content,
            ),
        )
    ]

    synchro = WorkflowTemplates.create_synchronization(
        sync_type="semaphore",
        config_map_ref_key="workflow",
        config_map_ref_name=os.environ.get("ARGO_WF_SYNCHRONIZATION_CM"),
    )

    return WorkflowTemplates.generate_workflow(
        name=argo_wf_name,
        entrypoint=entrypoint,
        annotations=annotations,
        inputs={"inputs": inputs},
        synchronization=synchro,
        volume_claim_template=vl_claim_t_list,
        secret_volume=secret_vl_list,
        config_map_volume=config_map_vl_list,
        templates=templates,
        namespace=namespace,
    )
