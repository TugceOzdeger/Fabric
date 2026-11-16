"""Microsoft Entra authentication helpers."""

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


class AuthenticationError(RuntimeError):
    """A safe, user-facing authentication failure."""


def create_credential():
    """Create the Azure Identity default credential chain."""
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        raise RuntimeError(
            "azure-identity is required; install the project dependencies"
        ) from exc
    return DefaultAzureCredential()


def acquire_access_token(credential) -> str:
    try:
        token = credential.get_token(FABRIC_SCOPE).token
    except Exception as exc:
        raise AuthenticationError(
            "Microsoft Entra authentication failed; configure a supported Azure Identity credential"
        ) from None
    if not token:
        raise AuthenticationError("Microsoft Entra authentication returned an empty access token")
    return token
