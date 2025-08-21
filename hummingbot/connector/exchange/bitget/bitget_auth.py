# TODO: Just a template, needs modifying

import base64
import hashlib
import hmac
import json
import time
from typing import Dict, Any, Optional
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class BitgetAuth(AuthBase):
    """
    Auth class required by Bitget API
    Learn more at https://www.bitget.com/api-doc/common/authentication
    """

    def __init__(self, api_key: str, secret_key: str, passphrase: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions.
        It also adds the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)

        # Get timestamp
        timestamp = str(int(self.time_provider.time() * 1000))

        # Create message to sign
        if request.method == RESTMethod.GET:
            query_string = urlencode(request.params) if request.params else ""
            path_with_query = request.url or ""
            if query_string:
                path_with_query += f"?{query_string}"
            message = timestamp + request.method.value + path_with_query
        else:
            body_str = json.dumps(request.data, separators=(',', ':')) if request.data else ""
            request_url = request.url or ""
            message = timestamp + request.method.value + request_url + body_str

        # Create signature
        signature = self._generate_signature(message)

        headers.update({
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "locale": "en-US"
        })

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated.
        Bitget uses a login message for authentication.
        """
        return request  # Will be implemented in the websocket data source

    def _create_message_to_sign(self, timestamp: str, method: str, path: str, query: Optional[str] = None, body: Optional[Any] = None) -> str:
        """
        Creates the message to sign for the request
        """
        message = timestamp + method + path

        if query:
            message += "?" + query

        if body is not None:
            if isinstance(body, dict):
                message += json.dumps(body, separators=(',', ':'))
            else:
                message += str(body)

        return message

    def _generate_signature(self, message: str) -> str:
        """
        Generates the signature for authentication
        """
        signature = base64.b64encode(
            hmac.new(
                self.secret_key.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        return signature

    def get_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Creates the authentication payload for WebSocket connection
        """
        timestamp = str(int(self.time_provider.time() * 1000))
        message = timestamp + "GET" + "/user/verify"
        signature = self._generate_signature(message)

        return {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": signature
            }]
        }
