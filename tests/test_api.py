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

        asyncio.run(_run())

