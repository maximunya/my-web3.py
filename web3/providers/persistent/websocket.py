import asyncio
import json
import logging
import os
import time

from typing import (
    Any,
    Dict,
    Optional,
    Union,
    Generator,
    List,
    Tuple
)
from eth_typing import URI
from toolz import merge
from websockets import WebSocketClientProtocol
from websockets.client import connect
from websockets.exceptions import ConnectionClosedOK, WebSocketException
from web3.exceptions import PersistentConnectionClosedOK, ProviderConnectionError, Web3ValidationError
from web3.providers.persistent import PersistentConnectionProvider
from web3.types import RPCResponse

DEFAULT_PING_INTERVAL = 30  # 30 seconds
DEFAULT_PING_TIMEOUT = 300  # 5 minutes

VALID_WEBSOCKET_URI_PREFIXES = {"ws://", "wss://"}
RESTRICTED_WEBSOCKET_KWARGS = {"uri", "loop"}
DEFAULT_WEBSOCKET_KWARGS = {
    "ping_interval": DEFAULT_PING_INTERVAL,
    "ping_timeout": DEFAULT_PING_TIMEOUT,
}

def get_default_endpoint() -> URI:
    return URI(os.environ.get("WEB3_WS_PROVIDER_URI", "ws://127.0.0.1:8546"))

class WebSocketProvider(PersistentConnectionProvider):
    logger = logging.getLogger("web3.providers.WebSocketProvider")
    is_async: bool = True

    _ws: Optional[WebSocketClientProtocol] = None

    def __init__(
        self,
        endpoint_uris: Optional[Union[URI, str, List[str], Tuple[str], Generator[str, None, None]]] = None,
        websocket_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self._unavailable_nodes = {}

        if endpoint_uris is None:
            self.endpoint_uris = [get_default_endpoint()]
        elif isinstance(endpoint_uris, str):
            self.endpoint_uris = [URI(endpoint_uris), ]
        elif isinstance(endpoint_uris, (list, tuple, Generator)):
            self.endpoint_uris = [URI(uri) for uri in endpoint_uris]
        else:
            raise Web3ValidationError("Invalid type for endpoint_uris")

        for uri in self.endpoint_uris:
            if not any(uri.startswith(prefix) for prefix in VALID_WEBSOCKET_URI_PREFIXES):
                raise Web3ValidationError(
                    "WebSocket endpoint uri must begin with 'ws://' or 'wss://': "
                    f"{uri}"
                )

        if websocket_kwargs is not None:
            found_restricted_keys = set(websocket_kwargs).intersection(RESTRICTED_WEBSOCKET_KWARGS)
            if found_restricted_keys:
                raise Web3ValidationError(
                    "Found restricted keys for websocket_kwargs: "
                    f"{found_restricted_keys}."
                )

        self.websocket_kwargs = merge(DEFAULT_WEBSOCKET_KWARGS, websocket_kwargs or {})

        super().__init__(**kwargs)

    def __str__(self) -> str:
        return f"WebSocket connection: {self.endpoint_uris}"

    def _get_refreshed_nodes(self):
        """Refresh the list of nodes, cleaning out unavailable ones."""
        self._clean_unavailable_nodes()
        if not self.endpoint_uris:
            raise Exception("All nodes are currently unavailable.")
        return self.endpoint_uris

    def _mark_node_as_unavailable(self, node: URI) -> None:
        """Mark a node as unavailable and retry later."""
        if node in self.endpoint_uris:
            self.endpoint_uris.remove(node)
        retry_after = 60  # Set retry time to 60 seconds
        self._unavailable_nodes[node] = time.time() + retry_after

    def _clean_unavailable_nodes(self) -> None:
        """Clean up nodes that are past their retry period."""
        current_time = time.time()
        nodes_to_remove = [node for node, retry_time in self._unavailable_nodes.items() if current_time > retry_time]
        for node in nodes_to_remove:
            self._unavailable_nodes.pop(node)
            self.endpoint_uris.append(node)

    async def is_connected(self, show_traceback: bool = False) -> bool:
        if not self._ws:
            return False

        try:
            await self._ws.pong()
            return True

        except WebSocketException as e:
            if show_traceback:
                raise ProviderConnectionError(
                    f"Error connecting to endpoint: '{self.endpoint_uris}'"
                ) from e
            return False

    async def socket_send(self, request_data: bytes) -> None:
        if self._ws is None:
            raise ProviderConnectionError(
                "Connection to websocket has not been initiated for the provider."
            )

        await asyncio.wait_for(
            self._ws.send(request_data), timeout=self.request_timeout
        )

    async def socket_recv(self) -> RPCResponse:
        raw_response = await self._ws.recv()
        return json.loads(raw_response)

    async def _provider_specific_connect(self) -> None:
        nodes = self._get_refreshed_nodes()
        for node in nodes:
            try:
                self._ws = await connect(node, **self.websocket_kwargs)
                break
            except (WebSocketException, ConnectionClosedOK) as e:
                self._mark_node_as_unavailable(node)
                self.logger.error(f"Failed to connect to {node}: {e}")

    async def _provider_specific_disconnect(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
            self._ws = None

    async def _provider_specific_socket_reader(self) -> RPCResponse:
        try:
            return await self.socket_recv()
        except ConnectionClosedOK:
            raise PersistentConnectionClosedOK(
                user_message="WebSocket connection received `ConnectionClosedOK`."
            )
