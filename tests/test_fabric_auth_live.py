"""Opt-in live authentication check; this test only retrieves an item definition."""

import logging
import os
import unittest

from fabric_deployer.api_client import FabricAPIClient
from fabric_deployer.auth import create_credential
from fabric_deployer.validation import validate_definition


WORKSPACE_ID = os.getenv(
    "FABRIC_DEPLOYER_WORKSPACE_ID", "0469cb50-a313-4569-8273-f396841183c7"
)
ITEM_ID = os.getenv(
    "FABRIC_DEPLOYER_ITEM_ID", "d5cfa2d9-3fe3-45f4-b769-537e7a3e9c42"
)
LIVE_READ_ENABLED = os.getenv("FABRIC_DEPLOYER_LIVE_READ") == "1"


@unittest.skipUnless(
    LIVE_READ_ENABLED,
    "Set FABRIC_DEPLOYER_LIVE_READ=1 to enable this read-only live Fabric check",
)
class FabricAuthenticationLiveTests(unittest.TestCase):
    def test_authenticate_and_read_bronze_pipeline_definition(self):
        logging.getLogger("azure").setLevel(logging.ERROR)
        client = FabricAPIClient(create_credential())

        # get_definition uses Fabric's POST .../getDefinition read operation only.
        definition = client.get_definition(WORKSPACE_ID, ITEM_ID)
        validate_definition(definition)

        self.assertTrue(definition.parts, "Fabric returned an empty item definition")


if __name__ == "__main__":
    unittest.main()
