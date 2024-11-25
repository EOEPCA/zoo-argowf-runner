# Description: This file contains the function to convert a CWL workflow to an Argo workflow.
import os
from typing import Optional
from hera.workflows.models import (
    Parameter,
    Quantity,
    ResourceRequirements,
    ScriptTemplate,
    TemplateRef,
)

from zoo_argowf_runner.template import (
    workflow_step,
    template,
    generate_workflow,
    synchronization,
)
from zoo_argowf_runner.zoo_helpers import CWLWorkflow
from zoo_argowf_runner.volume import (
    volume_claim_template,
    config_map_volume,
    secret_volume,
)


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
):

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
        volume_claim_template(
            name="calrissian-wdir",
            storageClassName=storage_class,
            storageSize=volume_size,
            accessMode=["ReadWriteMany"],
        ),
    ]

    secret_vl_list = [
        secret_volume(name="usersettings-vol", secretName="user-settings")
    ]

    workflow_sub_step = [
        workflow_step(
            name="prepare",
            template="prepare",
            parameters=[
                {"name": key, "value": f"{{{{inputs.parameters.{key}}}}}"}
                for key in ["inputs"]
            ],
        ),
        workflow_step(
            name="argo-cwl",
            template_ref=TemplateRef(
                name="argo-cwl-runner", template="calrissian-runner"
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
        ),
    ]

    templates = [
        template(
            name=entrypoint,
            subStep=workflow_sub_step,
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
        template(
            name="prepare",
            inputs_parameters=[{"name": key} for key in ["inputs"]],
            outputs_parameters=[
                {"name": "inputs", "path": "/tmp/cwl_parameters.json"},
                {"name": "workflow", "path": "/tmp/cwl_workflow.json"},
            ],
            script=ScriptTemplate(
                image="docker.io/library/prepare:0.1",
                resources=ResourceRequirements(
                    requests={"memory": Quantity(__root__="1Gi"), "cpu": int(1)}
                ),
                volume_mounts=[],
                command=["python"],
                source=prepare_content,
            ),
        ),
    ]

    synchro = synchronization(
        type="semaphore",
        configMapRef_key="workflow",
        configMapRef_name=os.environ.get("ARGO_WF_SYNCHRONIZATION_CM"),
    )

    return generate_workflow(
        name=argo_wf_name,
        entrypoint=entrypoint,
        annotations=annotations,
        inputs={"inputs": inputs},
        synchronization=synchro,
        volume_claim_template=vl_claim_t_list,
        secret_volume=secret_vl_list,
        config_map_volume=[],
        templates=templates,
        namespace=namespace,
    )
