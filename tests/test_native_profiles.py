import unittest
from types import SimpleNamespace as Obj
from unittest.mock import MagicMock, patch

from modules.native_streaming import data_client
from modules.queue_connections import profile
from modules.transport import transport_profile


class NativeProfileTests(unittest.TestCase):
    def test_pool_profile_needs_no_broker_or_vault_credential(self):
        cloud = MagicMock()
        cloud.config = {"region": "example-region"}
        pool = Obj(id="pool", lifecycle_state="ACTIVE")
        with patch("modules.queue_connections.get_resource", return_value=pool) as get:
            result = profile(cloud, {"kind": "oci_streaming", "resource_ocid": "pool"})
        get.assert_called_once_with(cloud, "stream_pool", "pool")
        self.assertEqual(result, {"kind": "oci_streaming", "region": "example-region", "stream_pool_id": "pool", "incarnation": "pool"})
        cloud.vault.get_secret.assert_not_called()

    def test_worker_and_vm_data_access_use_their_own_principal(self):
        with (
            patch("modules.native_streaming.oci.auth.signers.get_resource_principals_signer", return_value="workload") as resource,
            patch("modules.native_streaming.oci.auth.signers.InstancePrincipalsSecurityTokenSigner", return_value="vm") as instance,
            patch("modules.native_streaming.oci.streaming.StreamClient") as client,
        ):
            for worker, signer in ((False, "vm"), (True, "workload")):
                data_client({"region": "example-region"}, "https://stream.example.invalid", resource_principal=worker)
                self.assertEqual(client.call_args.kwargs["signer"], signer)
        resource.assert_called_once();instance.assert_called_once()

    def test_independent_flows_can_share_a_pool_without_shared_capture_state(self):
        config = {"region": "example-region", "streaming": {"kind": "oci_streaming", "stream_pool_id": "pool"}}
        one = transport_profile(config, {"id": 1, "transport": "streaming"})
        two = transport_profile(config, {"id": 2, "transport": "streaming"})
        self.assertEqual(one, two)
        self.assertNotIn("connect_topics", one)


class DirectPoolTests(unittest.TestCase):
    def test_existing_pool_reference_reuses_binding_without_registration(self):
        from modules.queue_connections import pool_reference
        existing = {'id': 3, 'name': 'original', 'kind': 'oci_streaming', 'resource_ocid': 'pool'}
        with patch('modules.queue_connections.profile') as validate, patch('modules.queue_connections.repo.query', return_value=existing) as query:
            self.assertEqual(pool_reference('db', 'cloud', 'pool'), existing)
            validate.assert_called_once_with('cloud', {'kind': 'oci_streaming', 'resource_ocid': 'pool'})
            self.assertEqual(query.call_count, 1)

    def test_new_pool_reference_is_created_only_after_live_scope_validation(self):
        from modules.queue_connections import pool_reference
        with patch('modules.queue_connections.profile'), patch('modules.queue_connections.repo.query', side_effect=[None, 9]) as query:
            row = pool_reference('db', 'cloud', 'new_pool')
            self.assertEqual(row['id'], 9)
            self.assertEqual(query.call_args.args[2][1:], ('oci_streaming', 'new_pool'))
        with patch('modules.queue_connections.profile', side_effect=ValueError('outside compartment')), patch('modules.queue_connections.repo.query') as query:
            with self.assertRaisesRegex(ValueError, 'outside compartment'):
                pool_reference('db', 'cloud', 'foreign_pool')
            query.assert_not_called()
