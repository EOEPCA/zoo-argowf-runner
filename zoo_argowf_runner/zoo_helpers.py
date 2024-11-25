# Description: Helper classes for the zoo-argowf-runner
import os
import attr
import inspect
import cwl_utils
from cwl_utils.parser import load_document_by_yaml


# useful class for hints in CWL
@attr.s
class ResourceRequirement:
    coresMin = attr.ib(default=None)
    coresMax = attr.ib(default=None)
    ramMin = attr.ib(default=None)
    ramMax = attr.ib(default=None)
    tmpdirMin = attr.ib(default=None)
    tmpdirMax = attr.ib(default=None)
    outdirMin = attr.ib(default=None)
    outdirMax = attr.ib(default=None)

    @classmethod
    def from_dict(cls, env):
        return cls(
            **{k: v for k, v in env.items() if k in inspect.signature(cls).parameters}
        )


class CWLWorkflow:
    def __init__(self, cwl, workflow_id):
        self.raw_cwl = cwl
        self.cwl = load_document_by_yaml(cwl, "io://")
        self.workflow_id = workflow_id

    def get_version(self):

        return self.raw_cwl.get("s:softwareVersion", "")

    def get_label(self):

        return self.get_workflow().label

    def get_doc(self):

        return self.get_workflow().doc

    def get_workflow(self) -> cwl_utils.parser.cwl_v1_0.Workflow:
        # returns a cwl_utils.parser.cwl_v1_0.Workflow)
        ids = [elem.id.split("#")[-1] for elem in self.cwl]

        return self.cwl[ids.index(self.workflow_id)]

    def get_object_by_id(self, id):
        ids = [elem.id.split("#")[-1] for elem in self.cwl]
        return self.cwl[ids.index(id)]

    def get_workflow_inputs(self, mandatory=False):
        inputs = []
        for inp in self.get_workflow().inputs:
            if mandatory:
                if inp.default is not None or inp.type == ["null", "string"]:
                    continue
                else:
                    inputs.append(inp.id.split("/")[-1])
            else:
                inputs.append(inp.id.split("/")[-1])
        return inputs

    @staticmethod
    def has_scatter_requirement(workflow):
        return any(
            isinstance(
                requirement,
                (
                    cwl_utils.parser.cwl_v1_0.ScatterFeatureRequirement,
                    cwl_utils.parser.cwl_v1_1.ScatterFeatureRequirement,
                    cwl_utils.parser.cwl_v1_2.ScatterFeatureRequirement,
                ),
            )
            for requirement in workflow.requirements
        )

    @staticmethod
    def get_resource_requirement(elem):
        """Gets the ResourceRequirement out of a CommandLineTool or Workflow

        Args:
            elem (CommandLineTool or Workflow): CommandLineTool or Workflow

        Returns:
            cwl_utils.parser.cwl_v1_2.ResourceRequirement or ResourceRequirement
        """
        resource_requirement = []

        # look for requirements
        if elem.requirements is not None:
            resource_requirement = [
                requirement
                for requirement in elem.requirements
                if isinstance(
                    requirement,
                    (
                        cwl_utils.parser.cwl_v1_0.ResourceRequirement,
                        cwl_utils.parser.cwl_v1_1.ResourceRequirement,
                        cwl_utils.parser.cwl_v1_2.ResourceRequirement,
                    ),
                )
            ]

            if len(resource_requirement) == 1:
                return resource_requirement[0]

        # look for hints
        if elem.hints is not None:
            resource_requirement = [
                ResourceRequirement.from_dict(hint)
                for hint in elem.hints
                if hint["class"] == "ResourceRequirement"
            ]

            if len(resource_requirement) == 1:
                return resource_requirement[0]

    def eval_resource(self):
        resources = {
            "coresMin": [],
            "coresMax": [],
            "ramMin": [],
            "ramMax": [],
            "tmpdirMin": [],
            "tmpdirMax": [],
            "outdirMin": [],
            "outdirMax": [],
        }

        for elem in self.cwl:
            if isinstance(
                elem,
                (
                    cwl_utils.parser.cwl_v1_0.Workflow,
                    cwl_utils.parser.cwl_v1_1.Workflow,
                    cwl_utils.parser.cwl_v1_2.Workflow,
                ),
            ):
                if resource_requirement := self.get_resource_requirement(elem):
                    for resource_type in [
                        "coresMin",
                        "coresMax",
                        "ramMin",
                        "ramMax",
                        "tmpdirMin",
                        "tmpdirMax",
                        "outdirMin",
                        "outdirMax",
                    ]:
                        if getattr(resource_requirement, resource_type):
                            resources[resource_type].append(
                                getattr(resource_requirement, resource_type)
                            )
                for step in elem.steps:
                    if resource_requirement := self.get_resource_requirement(
                        self.get_object_by_id(step.run[1:])
                    ):
                        multiplier = (
                            int(os.getenv("SCATTER_MULTIPLIER", 2))
                            if step.scatter
                            else 1
                        )
                        for resource_type in [
                            "coresMin",
                            "coresMax",
                            "ramMin",
                            "ramMax",
                            "tmpdirMin",
                            "tmpdirMax",
                            "outdirMin",
                            "outdirMax",
                        ]:
                            if getattr(resource_requirement, resource_type):
                                resources[resource_type].append(
                                    getattr(resource_requirement, resource_type)
                                    * multiplier
                                )
        return resources


class ZooConf:
    def __init__(self, conf):
        self.conf = conf
        self.workflow_id = self.conf["lenv"]["Identifier"]


class ZooInputs:
    def __init__(self, inputs):
        # this conversion is necessary
        # because zoo converts array of length 1 to a string
        for inp in inputs:
            if (
                "maxOccurs" in inputs[inp].keys()
                and int(inputs[inp]["maxOccurs"]) > 1
                and not isinstance(inputs[inp]["value"], list)
            ):
                inputs[inp]["value"] = [inputs[inp]["value"]]

        self.inputs = inputs

    def get_input_value(self, key):
        try:
            return self.inputs[key]["value"]
        except KeyError as exc:
            raise exc
        except TypeError:
            pass

    def get_processing_parameters(self):
        """Returns a list with the input parameters keys"""
        res = {}
        hasVal = False
        for key, value in self.inputs.items():
            if "dataType" in value:
                if isinstance(value["dataType"], list):
                    # How should we pass array for an input?
                    import json

                    res[key] = value["value"]
                else:
                    if value["dataType"] in ["double", "float"]:
                        res[key] = float(value["value"])
                    elif value["dataType"] == "integer":
                        res[key] = int(value["value"])
                    elif value["dataType"] == "boolean":
                        res[key] = int(value["value"])
                    else:
                        res[key] = value["value"]
            else:
                if "cache_file" in value:
                    if "mimeType" in value:
                        res[key] = {
                            "class": "File",
                            "path": value["cache_file"],
                            "format": value["mimeType"],
                        }
                    else:
                        res[key] = {
                            "class": "File",
                            "path": value["cache_file"],
                            "format": "text/plain",
                        }
                else:
                    res[key] = value["value"]
        return res


class ZooOutputs:
    def __init__(self, outputs):
        self.outputs = outputs
        # decuce the output key
        output_keys = list(self.outputs.keys())
        if len(output_keys) > 0:
            self.output_key = output_keys[0]
        else:
            self.output_key = "stac"
            if "stac" not in self.outputs.keys():
                self.outputs["stac"] = {}

    def get_output_parameters(self):
        """Returns a list with the output parameters keys"""
        return {key: value["value"] for key, value in self.outputs.items()}

    def set_output(self, value):
        """set the output result value"""
        self.outputs[self.output_key]["value"] = value
