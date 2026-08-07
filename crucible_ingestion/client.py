from crucible import CrucibleClient

_client = None

def get_client():
    global _client
    if _client is None:
        # Locally: url + apikey come from `crucible config init`.
        # In the cloud the consumer builds its own client and calls set_client().
        _client = CrucibleClient()
    return _client


def set_client(client):
    global _client
    _client = client
