import os
import unittest

from tests.water_bodies.service import water_bodies


class TestWaterBodiesService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class ZooStub(object):
            def __init__(self):
                self.SERVICE_SUCCEEDED = 3
                self.SERVICE_FAILED = 4

            def update_status(self, conf, progress):
                print(f"Status {progress}")

            def _(self, message):
                print(f"invoked _ with {message}")

        try:
            import zoo
        except ImportError:
            print("Not running in zoo instance")

            zoo = ZooStub()

        os.environ["ARGO_WF_ENDPOINT"] = "http://localhost:2746"
        os.environ["ARGO_WF_TOKEN"] = (
            "eyJhbGciOiJSUzI1NiIsImtpZCI6ImZSUUYycDZNbnI4MTZLeXVFUnpyZk9FcUJGTHdQQzI4SURGdHhQc0pzRXMifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJuczEiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlY3JldC5uYW1lIjoiYXJnby5zZXJ2aWNlLWFjY291bnQtdG9rZW4iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiYXJnbyIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjExZjI1NDUxLWE4OGEtNDFkZC1hNGIxLTJlNTM3ZGUyOGU3NiIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpuczE6YXJnbyJ9.dUVo3WCJgoVN5NVh3VVeKw7uF6hE6moCj8Mu5W7ioJ4M_lG_bJ4BZ7XRaJVrQMF--vMQAiNDr-_GSC0R7ItKmRqhafkONy61MeEDz_6u-j0ay4fj8qofgBatnF7lWcVVvNklVZxZ4IGb62SJepAvzerxsz5HhSG8icn9U0cUsyg4Vw_wsHntic-yFY4Eyp6kYSEXO5G2v_0KXLKcpLB41YLfMQaE8cu0ghmE5mFmKkKOf0oDHhLlch-j_K0IpGvXKcjZGlwjiukD1dCL3E6BSHYMutuz4n27Bg62nf3rZBErUkJB2GRjN-tKIoYMrEuwyEbv7TSuCeqHGyfWn3Tu0A"
        )
        os.environ["ARGO_WF_SYNCHRONIZATION_CM"] = (
            "semaphore-argo-cwl-runner-stage-in-out"
        )
        os.environ["ARGO_CWL_RUNNER_TEMPLATE"] = "argo-cwl-runner-stage-in-out"
        os.environ["DEFAULT_VOLUME_SIZE"] = "12Gi"
        os.environ["STORAGE_CLASS"] = "standard"

        cls.zoo = zoo

        conf = {}
        conf["lenv"] = {"message": "", "Identifier": "water-bodies", "usid": "abc-1234"}
        conf["tmpPath"] = "/tmp"
        conf["main"] = {"tmpUrl": "http://localhost/logs/"}
        conf["auth_env"] = {"user": "ns1"}
        cls.conf = conf

        inputs = {
            "aoi": {"value": "-118.985,38.432,-118.183,38.938"},
            "bands": {"value": ["green", "nir08"]},
            "epsg": {"value": "EPSG:4326"},
            "item": {
                "value": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/landsat-c2-l2/items/LC09_L2SP_042033_20231015_02_T1"  # noqa
            },
        }

        cls.inputs = inputs

        outputs = {"Result": {"value": ""}}

        cls.outputs = outputs

    def test_execution(self):
        exit_code = water_bodies(
            conf=self.conf, inputs=self.inputs, outputs=self.outputs
        )

        self.assertEqual(exit_code, self.zoo.SERVICE_SUCCEEDED)
