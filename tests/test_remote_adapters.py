import unittest
from unittest.mock import patch, MagicMock
import json

from volmigrate.adapters.sources.docker_source import DockerSourceAdapter
from volmigrate.adapters.destinations.k8s_destination import KubernetesDestinationAdapter
from volmigrate.services.connection_service import ConnectionService
from volmigrate.models import Connection, ConnectionType, ConnectionStatus
from volmigrate.core.security import encrypt_secret


class TestRemoteAdapters(unittest.TestCase):
    @patch("docker.DockerClient")
    def test_docker_remote_ssh_adapter(self, mock_docker_client):
        adapter = DockerSourceAdapter(
            base_url="ssh://ubuntu@10.0.0.5:2222",
            ssh_key="-----BEGIN OPENSSH PRIVATE KEY-----\ntest\n-----END OPENSSH PRIVATE KEY-----",
            ssh_password="secretpassword",
        )
        client = adapter._get_client()
        self.assertIsNotNone(client)
        mock_docker_client.assert_called_once()
        self.assertEqual(adapter.base_url, "ssh://ubuntu@10.0.0.5:2222")

    @patch("docker.DockerClient")
    def test_docker_remote_tls_adapter(self, mock_docker_client):
        adapter = DockerSourceAdapter(
            base_url="tcp://remote-docker.example.com:2376",
            tls_ca_cert="-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----",
            tls_client_cert="-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----",
            tls_client_key="-----BEGIN RSA PRIVATE KEY-----\nkey\n-----END RSA PRIVATE KEY-----",
            tls_verify=True,
        )
        client = adapter._get_client()
        self.assertIsNotNone(client)
        mock_docker_client.assert_called_once()
        # Ensure temporary cert files were tracked
        self.assertGreater(len(adapter._temp_files), 0)

    @patch("volmigrate.adapters.destinations.k8s_destination.client.ApiClient")
    def test_k8s_remote_token_adapter(self, mock_api_client):
        adapter = KubernetesDestinationAdapter(
            api_server_url="https://k8s.example.com:6443",
            bearer_token="my-token-12345",
            insecure_skip_tls=True,
        )
        api_client = adapter._get_api_client()
        self.assertIsNotNone(api_client)
        mock_api_client.assert_called_once()
        # Verify the configuration passed to ApiClient
        conf = mock_api_client.call_args[0][0]
        self.assertEqual(conf.host, "https://k8s.example.com:6443")
        self.assertEqual(conf.api_key["authorization"], "Bearer my-token-12345")
        self.assertFalse(conf.verify_ssl)

    @patch("volmigrate.adapters.destinations.k8s_destination.config.new_client_from_config_dict")
    def test_k8s_remote_kubeconfig_adapter(self, mock_from_dict):
        kubeconfig_data = {
            "apiVersion": "v1",
            "clusters": [{"name": "c1", "cluster": {"server": "https://192.168.1.50:6443"}}],
            "contexts": [{"name": "ctx1", "context": {"cluster": "c1", "user": "u1"}}],
            "current-context": "ctx1",
            "users": [{"name": "u1", "user": {"token": "token123"}}],
        }
        adapter = KubernetesDestinationAdapter(
            kubeconfig_dict=kubeconfig_data,
            context="ctx1",
        )
        api_client = adapter._get_api_client()
        self.assertIsNotNone(api_client)
        mock_from_dict.assert_called_once_with(kubeconfig_data, context="ctx1")

    def test_connection_service_builds_remote_adapters(self):
        # 1. Docker Remote SSH via payload
        docker_payload = {
            "docker_mode": "ssh",
            "ssh_host": "10.0.0.99",
            "ssh_port": 22,
            "ssh_user": "devops",
            "ssh_key": "some-private-key",
        }
        docker_conn = Connection(
            id="conn-docker-test",
            name="Remote Docker SSH",
            type=ConnectionType.DOCKER,
            endpoint="ssh://devops@10.0.0.99:22",
            config_encrypted=encrypt_secret(json.dumps(docker_payload)),
            status=ConnectionStatus.ACTIVE,
        )
        adapter_docker = ConnectionService.get_adapter(docker_conn)
        self.assertIsInstance(adapter_docker, DockerSourceAdapter)
        self.assertEqual(adapter_docker.base_url, "ssh://devops@10.0.0.99:22")
        self.assertEqual(adapter_docker.ssh_key, "some-private-key")

        # 2. K8s Remote Kubeconfig YAML via payload
        k8s_yaml_payload = {
            "k8s_mode": "kubeconfig",
            "kubeconfig_raw": """
apiVersion: v1
clusters:
- cluster:
    server: https://192.168.5.10:6443
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-admin
  name: test-context
current-context: test-context
users:
- name: test-admin
  user:
    token: test-token
""",
            "context": "test-context",
        }
        k8s_conn = Connection(
            id="conn-k8s-test",
            name="Remote K8s Cluster",
            type=ConnectionType.KUBERNETES,
            endpoint=None,
            config_encrypted=encrypt_secret(json.dumps(k8s_yaml_payload)),
            status=ConnectionStatus.ACTIVE,
        )
        adapter_k8s = ConnectionService.get_adapter(k8s_conn)
        self.assertIsInstance(adapter_k8s, KubernetesDestinationAdapter)
        self.assertIsNotNone(adapter_k8s.kubeconfig_dict)
        self.assertEqual(adapter_k8s.kubeconfig_dict["current-context"], "test-context")
        self.assertEqual(adapter_k8s.context, "test-context")
