import unittest
from volmigrate.core.config import get_settings
from volmigrate.core.security import encrypt_secret, decrypt_secret, mask_secret


class TestCore(unittest.TestCase):
    def test_settings_load(self):
        settings = get_settings()
        self.assertEqual(settings.PROJECT_NAME, "Volmigrate")
        self.assertEqual(settings.API_V1_PREFIX, "/api")

    def test_secret_encryption_and_decryption(self):
        plain = "super-secret-kubeconfig-or-docker-token"
        encrypted = encrypt_secret(plain)
        self.assertNotEqual(encrypted, plain)
        decrypted = decrypt_secret(encrypted)
        self.assertEqual(decrypted, plain)

    def test_mask_secret(self):
        self.assertEqual(mask_secret("abcdef123456"), "abc...456")
        self.assertEqual(mask_secret("short"), "******")



