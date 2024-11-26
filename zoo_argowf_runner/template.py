# Description: This file contains the functions to generate the Argo workflow templates.
from hera.workflows import (
    Workflow,
    Steps,
)

from hera.workflows.models import (
    Arguments,
    Artifact,
    ConfigMapKeySelector,
    Inputs,
    Outputs,
    ParallelSteps,
    Parameter,
    PersistentVolumeClaim,
    ScriptTemplate,
    SemaphoreRef,
    Synchronization,
    Template,
    TemplateRef,
    ValueFrom,
    Volume,
    WorkflowStep,
)

from typing import Optional
from typing import List, Dict


def synchronization(
    type: str,
    configMapRef_key: str,
    configMapRef_name: Optional[str] = None,
    optional: Optional[bool] = None,
) -> Synchronization:
    if type == "semaphore":
        semaphore = SemaphoreRef(
            config_map_key_ref=ConfigMapKeySelector(
                name=configMapRef_name, key=configMapRef_key, optional=optional
            )
        )
        return Synchronization(semaphore=semaphore)


def workflow_step(
    name: str,
    parameters: Optional[List[Parameter]] = None,
    artifacts: Optional[List[Artifact]] = None,
    template: Optional[str] = None,
    template_ref: Optional[TemplateRef] = None,
) -> WorkflowStep:

    arguments = Arguments(parameters=parameters, artifacts=artifacts)

    return WorkflowStep(
        name=name, template=template, arguments=arguments, template_ref=template_ref
    )


def template(
    name: str,
    subStep: Optional[List[WorkflowStep]] = None,
    inputs_parameters: Optional[List[dict] | Inputs] = None,
    inputs_artifacts: Optional[List[dict] | Inputs] = None,
    outputs_parameters: Optional[List[dict] | Outputs] = None,
    outputs_artifacts: Optional[List[dict] | Outputs] = None,
    script: Optional[ScriptTemplate] = None,
) -> Steps:

    steps = None
    if subStep:
        steps = []
        for sub in subStep:
            steps.append(ParallelSteps(__root__=[sub]))

    inputs = Inputs()
    outputs = Outputs()

    if isinstance(inputs_parameters, List):
        parameters = [Parameter(name=elem["name"]) for elem in inputs_parameters]
        inputs.parameters = parameters
    elif isinstance(inputs_parameters, Inputs):
        inputs = inputs_parameters

    if isinstance(inputs_artifacts, List):
        artifacts = [
            Artifact(name=elem["name"], from_expression=elem["from_expression"])
            for elem in inputs_artifacts
        ]
        inputs.artifacts = artifacts
    elif isinstance(inputs_artifacts, Inputs):
        inputs = inputs_artifacts

    if isinstance(outputs_parameters, List):
        if "expression" in outputs_parameters[0].keys():
            parameters = [
                Parameter(
                    name=elem["name"],
                    value_from=ValueFrom(expression=elem["expression"]),
                )
                for elem in outputs_parameters
            ]
        if "path" in outputs_parameters[0].keys():
            parameters = [
                Parameter(name=elem["name"], value_from=ValueFrom(path=elem["path"]))
                for elem in outputs_parameters
            ]
        outputs.parameters = parameters
    elif isinstance(outputs_parameters, Outputs):
        outputs = outputs_parameters

    if isinstance(outputs_artifacts, List):
        artifacts = [
            Artifact(name=elem["name"], from_expression=elem["from_expression"])
            for elem in outputs_artifacts
        ]
        outputs.artifacts = artifacts
    elif isinstance(outputs_artifacts, Outputs):
        outputs = outputs_artifacts

    if inputs.artifacts == None and inputs.parameters == None:
        inputs = None
    if outputs.artifacts == None and outputs.parameters == None:
        outputs = None

    return Template(
        name=name,
        steps=steps,
        inputs=inputs,
        outputs=outputs,
        script=script,
    )


def generate_workflow(
    name: str,
    entrypoint: str,
    service_account_name: Optional[str] = None,
    annotations: Optional[dict] = None,
    inputs: Optional[List[dict]] = None,
    synchronization: Optional[Synchronization] = None,
    volume_claim_template: Optional[List[PersistentVolumeClaim]] = None,
    secret_volume: Optional[List[Volume]] = None,
    config_map_volume: Optional[List[Volume]] = None,
    templates: Optional[List[Steps]] = None,
    namespace: Optional[str] = None,
):

    volumes = []
    arguments = []

    wf = Workflow(
        name=name,
        annotations=annotations,
        entrypoint=entrypoint,
        namespace=namespace,
        service_account_name=service_account_name,
        synchronization=synchronization,
    )
    if inputs:
        for key, input in inputs.items():
            arguments.append(Parameter(name=key, value=str(input)))
        wf.arguments = arguments
    if volume_claim_template:
        wf.volume_claim_templates = volume_claim_template
    if secret_volume:
        volumes.extend(secret_volume)
    if config_map_volume:
        volumes.extend(config_map_volume)
    if templates:
        wf.templates = templates

    wf.volumes = volumes

    return wf
