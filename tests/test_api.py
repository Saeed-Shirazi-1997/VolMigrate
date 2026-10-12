import unittest
import asyncio
import httpx
from volmigrate.api.main import app
from volmigrate.core.database import init_db


class TestAPI(unittest.TestCase):
    def test_api_lifecycle_and_endpoints(self):
        async def _run():
            await init_db()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                # Health check
                r = await client.get("/healthz")
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.json()["status"], "healthy")

                # UI check
                r_ui = await client.get("/")
                self.assertEqual(r_ui.status_code, 200)
                self.assertIn("Volmigrate", r_ui.text)

                # Create connection
                r_conn = await client.post("/api/connections", json={
                    "name": "Test Docker Unit",
                    "type": "docker",
                    "endpoint": "unix:///var/run/docker.sock"
                })
                self.assertEqual(r_conn.status_code, 201)
                conn_id = r_conn.json()["id"]

                # List connections
                r_list = await client.get("/api/connections")
                self.assertEqual(r_list.status_code, 200)
                self.assertTrue(any(c["id"] == conn_id for c in r_list.json()))

                # Delete connection
                r_del = await client.delete(f"/api/connections/{conn_id}")
                self.assertEqual(r_del.status_code, 204)

                # Create Remote Docker connection via SSH
                r_docker_ssh = await client.post("/api/connections", json={
                    "name": "Production Remote Docker",
                    "type": "docker",
                    "endpoint": "ssh://root@192.168.1.100:22",
                    "config_payload": {
                        "docker_mode": "ssh",
                        "ssh_host": "192.168.1.100",
                        "ssh_port": 22,
                        "ssh_user": "root",
                        "ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----"
                    }
                })
                self.assertEqual(r_docker_ssh.status_code, 201)
                ssh_id = r_docker_ssh.json()["id"]
                # Verify secret is NOT returned in response
                self.assertNotIn("ssh_key", r_docker_ssh.json())

                # Create Remote Kubernetes connection via Kubeconfig
                r_k8s_remote = await client.post("/api/connections", json={
                    "name": "Production K8s Cluster",
                    "type": "kubernetes",
                    "config_payload": {
                        "k8s_mode": "kubeconfig",
                        "kubeconfig_raw": "apiVersion: v1\nclusters: []\ncontexts: []\ncurrent-context: test"
                    }
                })
                self.assertEqual(r_k8s_remote.status_code, 201)
                k8s_id = r_k8s_remote.json()["id"]
                self.assertNotIn("kubeconfig_raw", r_k8s_remote.json())

                # Clean up
                await client.delete(f"/api/connections/{ssh_id}")
                await client.delete(f"/api/connections/{k8s_id}")

        asyncio.run(_run())


