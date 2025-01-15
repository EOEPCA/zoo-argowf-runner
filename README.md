# zoo-argowf-runner

Zoo runner using Argo Workflows

## Environment variables

- `STORAGE_CLASS`: k8s cluster RWX storage class, defaults to `standard`.
- `DEFAULT_VOLUME_SIZE`: Calrissian default RWX volume size, defaults to `12Gi`.
- `DEFAULT_MAX_CORES`: Calrissian default max cores, defaults to `4`.
- `DEFAULT_MAX_RAM`: Calrissian default max RAM, defaults to `4Gi`.
- `ARGO_WF_ENDPOINT`: this is the Argo Workflows API endpoint, defaults to `"http://localhost:2746"`.
- `ARGO_WF_TOKEN`: this is the Argo Workflows API token that can be retrieved with: `kubectl get -n ns1 secret argo.service-account-token -o=jsonpath='{.data.token}' | base64 --decode`
- `ARGO_WF_SYNCHRONIZATION_CM`: this is the Argo Workflows synchronizaion configmap (with key "workflow"). For tests, we use "semaphore-argo-cwl-runner"
- `ARGO_CWL_RUNNER_TEMPLATE`: this is the Argo Workflows WorkflowTemplate that runs the CWL, defaults to: "argo-cwl-runner"
- `ARGO_CWL_RUNNER_ENTRYPOINT`: this is the Argo Workflows WorkflowTemplate entrypoint, defaults to: "calrissian-runner"

## Requirements

The Argo Workflows deployment has a Argo Workflows `WorkflowTemplate` or `ClusterWorkflowTemplate` impllementing the execution of a Calrissian Job and exposing the interface:

**Input parameters:** 

```yaml
    templates:
    - name: calrissian-runner
      inputs:
        parameters:
          - name: parameters
            description: Parameters in JSON format
          - name: cwl
            description: CWL document in JSON format
          - name: max_ram
            default: 8G
            description: Max RAM (e.g. 8G)
          - name: max_cores
            default: '4'
            description: Max cores (e.g. 4)
          - name: entry_point
            description: CWL document entry_point
```

**Outputs:**

```yaml
    outputs:
        parameters:
          - name: results
            valueFrom:
              parameter: '{{steps.get-results.outputs.parameters.calrissian-output}}'
          - name: log
            valueFrom:
              parameter: '{{steps.get-results.outputs.parameters.calrissian-stderr}}'
          - name: usage-report
            valueFrom:
              parameter: '{{steps.get-results.outputs.parameters.calrissian-report}}'
          - name: stac-catalog
            valueFrom:
              parameter: '{{steps.stage-out.outputs.parameters.stac-catalog}}'
          - name: feature-collection
            valueFrom:
              parameter: >-
                {{steps.feature-collection.outputs.parameters.feature-collection}}
        artifacts:
          - name: tool-logs
            from: '{{steps.get-results.outputs.artifacts.tool-logs}}'
          - name: calrissian-output
            from: '{{steps.get-results.outputs.artifacts.calrissian-output}}'
          - name: calrissian-stderr
            from: '{{steps.get-results.outputs.artifacts.calrissian-stderr}}'
          - name: calrissian-report
            from: '{{steps.get-results.outputs.artifacts.calrissian-report}}'
```

Where: 

- `results` is the Calrissian job stdout
- `log` is the Calrissian job stderr
- `usage-report` is the Calrissian usage report
- `stac-catalog` is the s3 path to the published STAC Catalog
- `feature-collection` is the Feature Collection with the STAC Items produced

And the artifacts:

- `tool-logs` is the Calrissian CWL step logs defined as:


```yaml
        artifacts:
          - name: tool-logs
            path: /calrissian/logs
            s3:
              key: '{{workflow.name}}-{{workflow.uid}}-artifacts/tool-logs'
            archive:
              none: {}
```

- `calrissian-output` is the Calrissian stdout
- `calrissian-stderr` is the Calrissian job stderr
- `calrissian-report` is the Calrissian usage report

See the example provided in folder `example`

## Caveats

### Additional volumes in the Argo Workflows WorkflowTemplate that runs the CWL

Let's say one wants to add a configmap on the Argo Workflows WorkflowTemplate that runs the CWL.

By design, this volume must also be declared in an Argo Workflows Workflow that wants to run the WorkflowTemplate in a step.

This means that if the Argo Workflows WorkflowTemplate that runs the CWL declares:

```yaml
volumes:
  - name: cwl-wrapper-config-vol
    configMap:
      name: cwl-wrapper-config
      items:
        - key: main.yaml
        - key: rules.yaml
        - key: stage-in.cwl
        - key: stage-out.cwl
```

The 

```python
config_map_volume(
    name="cwl-wrapper-config-vol",
    configMapName="cwl-wrapper-config",
    items=[{"key": "main.yaml"}, {"key": "rules.yaml"}, {"key": "stage-in.yaml"}, {"key": "stage-out.yaml"}],
    defaultMode=420,
    optional=False
)
```
