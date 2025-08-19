from typing import Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class O2Auth(AuthBase):
    """
    O2 authentication handler
    Currently O2 doesn't have authentication, so this is a no-op implementation
    """

    def __init__(self):
        # O2 doesn't require API keys or secrets yet
        pass

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        O2 doesn't have authentication yet, so this is a pass-through
        :param request: the request to be configured for authenticated interaction
        """
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        O2 doesn't have WebSocket authentication yet, so this is a pass-through
        :param request: the websocket request to be authenticated
        """
        return request

    def header_for_authentication(self) -> Dict[str, str]:
        """
        O2 doesn't require authentication headers yet
        :return: empty dict since no auth headers are needed
        """
        return {}
