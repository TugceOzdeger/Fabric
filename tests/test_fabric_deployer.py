import base64
import json
import unittest

from fabric_deployer.api_client import FabricAPIError
from fabric_deployer.api_client import FabricAPIClient
from fabric_deployer.auth import AuthenticationError, acquire_access_token
from fabric_deployer.definition import DefinitionError, FabricDefinition
from fabric_deployer.deployment import (
    AmbiguousUpdateError,
    ConcurrentModificationError,
    deploy_definition,
)


def make_definition(environment="dev"):
    document = {
        "properties": {
            "activities": [
                {
                    "name": "BronzeIngestion",
                    "type": "TridentNotebook",
                    "typeProperties": {"parameters": {"environment": environment}},
                    "policy": {"retry": 0},
                },
                {
                    "name": "SilverGoldTransformation",
                    "type": "TridentNotebook",
                    "dependsOn": [{"activity": "BronzeIngestion"}],
                },
            ],
            "parameters": {
                "environment": {"type": "String", "defaultValue": environment},
                "processing_date": {"type": "String", "defaultValue": ""},
            },
        }
    }
    raw = json.dumps(document, indent=2).encode()
    return FabricDefinition.from_api(
        {
            "format": "json",
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": base64.b64encode(raw).decode(),
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "metadata.txt",
                    "payload": base64.b64encode(b"keep unchanged").decode(),
                    "payloadType": "InlineBase64",
                },
            ],
        }
    )


class FakeClient:
    def __init__(self, definitions, *, update_error=None):
        self.definitions = list(definitions)
        self.updates = []
        self.update_error = update_error

    def get_definition(self, workspace_id, item_id):
        if len(self.definitions) > 1:
            return self.definitions.pop(0)
        return self.definitions[0]

    def submit_definition(self, workspace_id, item_id, definition):
        self.updates.append(definition)
        if self.update_error:
            raise self.update_error
        self.definitions[0] = definition
        return None

    def poll_operation(self, operation_url, *, timeout, poll_interval):
        raise AssertionError("No asynchronous operation expected")


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self.body = body
        self.headers = headers or {}

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class FabricDeployerTests(unittest.TestCase):
    def test_round_trip_and_json_edit_preserve_other_pipeline_content(self):
        original = make_definition()
        round_trip = FabricDefinition.from_api(original.to_api())
        self.assertEqual(round_trip.fingerprint(), original.fingerprint())
        client = FakeClient([original])

        result = deploy_definition(
            client,
            "workspace",
            "item",
            {
                "pipeline-content.json": [
                    {
                        "op": "set",
                        "pointer": "/properties/parameters/environment/defaultValue",
                        "value": "test",
                    }
                ]
            },
        )

        self.assertTrue(result.changed)
        self.assertEqual(result.mode, "dry-run")
        self.assertFalse(result.verified)
        self.assertEqual(client.updates, [])
        edited = json.loads(client.definitions[0].parts[0].content)
        self.assertEqual(
            [item["name"] for item in edited["properties"]["activities"]],
            ["BronzeIngestion", "SilverGoldTransformation"],
        )
        self.assertEqual(
            edited["properties"]["parameters"]["processing_date"]["defaultValue"], ""
        )
        self.assertEqual(client.definitions[0].parts[1].content, b"keep unchanged")

    def test_rejects_invalid_base64_and_invalid_json(self):
        with self.assertRaisesRegex(DefinitionError, "invalid Base64"):
            FabricDefinition.from_api(
                {"parts": [{"path": "x.json", "payload": "!", "payloadType": "InlineBase64"}]}
            )
        invalid_json = base64.b64encode(b"{").decode()
        with self.assertRaisesRegex(DefinitionError, "valid UTF-8 JSON"):
            FabricDefinition.from_api(
                {
                    "parts": [
                        {
                            "path": "x.json",
                            "payload": invalid_json,
                            "payloadType": "InlineBase64",
                        }
                    ]
                }
            )

    def test_apply_refetches_before_write_and_verifies_final_state(self):
        original = make_definition()
        client = FakeClient([original, original])
        result = deploy_definition(
            client,
            "workspace",
            "item",
            {
                "pipeline-content.json": [
                    {"op": "set", "pointer": "/properties/parameters/environment/defaultValue", "value": "prod"}
                ]
            },
            apply=True,
        )
        self.assertEqual(len(client.updates), 1)
        self.assertTrue(result.verified)

    def test_detects_concurrent_change_before_writing(self):
        original = make_definition()
        concurrent = make_definition(environment="changed elsewhere")
        client = FakeClient([original, concurrent])
        with self.assertRaises(ConcurrentModificationError):
            deploy_definition(
                client,
                "workspace",
                "item",
                {
                    "pipeline-content.json": [
                        {"op": "set", "pointer": "/properties/parameters/environment/defaultValue", "value": "prod"}
                    ]
                },
                apply=True,
            )
        self.assertEqual(client.updates, [])

    def test_ambiguous_update_checks_live_state_without_retry(self):
        original = make_definition()
        desired = make_definition(environment="prod")
        client = FakeClient(
            [original, original, original],
            update_error=FabricAPIError("unknown", ambiguous=True),
        )
        with self.assertRaises(AmbiguousUpdateError):
            deploy_definition(
                client,
                "workspace",
                "item",
                {
                    "pipeline-content.json": [
                        {"op": "set", "pointer": "/properties/parameters/environment/defaultValue", "value": "prod"}
                    ]
                },
                apply=True,
            )
        self.assertEqual(len(client.updates), 1)
        self.assertNotEqual(desired.fingerprint(), original.fingerprint())

    def test_api_client_reads_definition_and_polls_async_update(self):
        definition = make_definition()
        session = FakeSession(
            [
                FakeResponse(200, {"definition": definition.to_api()}),
                FakeResponse(
                    202,
                    headers={"Location": "https://api.fabric.microsoft.com/v1/operations/op-1"},
                ),
                FakeResponse(200, {"status": "Succeeded"}),
            ]
        )

        class Credential:
            def get_token(self, scope):
                self.scope = scope
                return type("Token", (), {"token": "test-token"})()

        client = FabricAPIClient(Credential(), session=session)
        retrieved = client.get_definition("workspace", "item")
        operation = client.submit_definition(
            "workspace", "item", retrieved
        )
        client.poll_operation(operation, timeout=1, poll_interval=0)

        self.assertEqual(retrieved.fingerprint(), definition.fingerprint())
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(session.calls[0][0], "POST")
        self.assertTrue(session.calls[0][1].endswith("/items/item/getDefinition"))
        self.assertEqual(session.calls[0][2]["headers"]["Authorization"], "Bearer test-token")

    def test_api_client_reads_definition_from_async_result(self):
        definition = make_definition()
        session = FakeSession(
            [
                FakeResponse(
                    202,
                    headers={
                        "Location": "https://api.fabric.microsoft.com/v1/operations/op-read",
                        "Retry-After": "0",
                    },
                ),
                FakeResponse(200, {"status": "Succeeded"}),
                FakeResponse(200, {"definition": definition.to_api()}),
            ]
        )

        class Credential:
            def get_token(self, scope):
                return type("Token", (), {"token": "test-token"})()

        client = FabricAPIClient(Credential(), session=session)
        retrieved = client.get_definition("workspace", "item")

        self.assertEqual(retrieved.fingerprint(), definition.fingerprint())
        self.assertEqual([call[0] for call in session.calls], ["POST", "GET", "GET"])
        self.assertTrue(session.calls[2][1].endswith("/operations/op-read/result"))
        self.assertTrue(all("updateDefinition" not in call[1] for call in session.calls))

    def test_authentication_failure_does_not_expose_provider_error(self):
        class BadCredential:
            def get_token(self, scope):
                raise RuntimeError("provider diagnostic containing a token")

        with self.assertRaises(AuthenticationError) as caught:
            acquire_access_token(BadCredential())
        self.assertNotIn("provider diagnostic", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
