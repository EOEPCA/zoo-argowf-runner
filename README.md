# zoo-argowf-runner

Zoo runner using Argo Workflows

## Environment variables

- `STORAGE_CLASS`: standard
- `DEFAULT_VOLUME_SIZE` 12Gi
- `DEFAULT_MAX_CORES`: 4
- `DEFAULT_MAX_RAM`: 4Gi
- `ARGO_WF_ENDPOINT`: "http://localhost:2746"
- `ARGO_WF_TOKEN`: `kubectl get -n ns1 secret argo.service-account-token -o=jsonpath='{.data.token}' | base64 --decode`
- `ARGO_WF_SYNCHRONIZATION_CM`: "semaphore-water-bodies"