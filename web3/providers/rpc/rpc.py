import logging
import time

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
    Generator
)

from eth_typing import (
    URI,
)
from eth_utils import (
    combomethod,
    to_dict,
)
import requests

from web3._utils.empty import (
    Empty,
    empty,
)
from web3._utils.http import (
    construct_user_agent,
)
from web3.types import (
    RPCEndpoint,
    RPCResponse,
)

from ..._utils.batching import (
    sort_batch_response_by_response_ids,
)
from ..._utils.caching import (
    handle_request_caching,
)
from ..._utils.http_session_manager import (
    HTTPSessionManager,
)
from ..base import (
    JSONBaseProvider,
)
from .utils import (
    ExceptionRetryConfiguration,
    check_if_retry_on_failure,
)
from ...exceptions import Web3ValidationError

if TYPE_CHECKING:
    from web3.middleware.base import (  # noqa: F401
        Middleware,
    )


class HTTPProvider(JSONBaseProvider):
    logger = logging.getLogger("web3.providers.HTTPProvider")
    endpoint_uris = None
    _request_kwargs = None

    def __init__(
        self,
        endpoint_uris: Optional[Union[URI, str, List[str], Tuple[str], Generator[str, None, None]]] = None,
        request_kwargs: Optional[Any] = None,
        session: Optional[Any] = None,
        exception_retry_configuration: Optional[
            Union[ExceptionRetryConfiguration, Empty]
        ] = empty,
        **kwargs: Any,
    ) -> None:
        self._request_session_manager = HTTPSessionManager()
        self._unavailable_nodes = {}

        if endpoint_uris is None:
            self.endpoint_uris = [self._request_session_manager.get_default_http_endpoint()]
        elif isinstance(endpoint_uris, str):
            self.endpoint_uris = [URI(endpoint_uris), ]
        elif isinstance(endpoint_uris, (list, tuple, Generator)):
            self.endpoint_uris = [URI(uri) for uri in endpoint_uris]
        else:
            raise Web3ValidationError("Invalid type for endpoint_uris")

        self._request_kwargs = request_kwargs or {}
        self._exception_retry_configuration = exception_retry_configuration

        if session:
            for uri in self.endpoint_uris:
                self._request_session_manager.cache_and_return_session(uri, session)

        super().__init__(**kwargs)

    def __str__(self) -> str:
        return f"RPC connection {self.endpoint_uris}"

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

    @property
    def exception_retry_configuration(self) -> ExceptionRetryConfiguration:
        if isinstance(self._exception_retry_configuration, Empty):
            self._exception_retry_configuration = ExceptionRetryConfiguration(
                errors=(
                    ConnectionError,
                    requests.HTTPError,
                    requests.Timeout,
                )
            )
        return self._exception_retry_configuration

    @exception_retry_configuration.setter
    def exception_retry_configuration(
        self, value: Union[ExceptionRetryConfiguration, Empty]
    ) -> None:
        self._exception_retry_configuration = value

    @to_dict
    def get_request_kwargs(self) -> Iterable[Tuple[str, Any]]:
        if "headers" not in self._request_kwargs:
            yield "headers", self.get_request_headers()
        yield from self._request_kwargs.items()

    @combomethod
    def get_request_headers(cls) -> Dict[str, str]:
        if isinstance(cls, HTTPProvider):
            cls_name = cls.__class__.__name__
        else:
            cls_name = cls.__name__

        module = cls.__module__

        return {
            "Content-Type": "application/json",
            "User-Agent": construct_user_agent(module, cls_name),
        }

    def _make_request(self, method: RPCEndpoint, request_data: bytes) -> bytes:
        nodes = self._get_refreshed_nodes()
        logging.warning(nodes)
        for node in nodes:
            try:
                logging.info(f"Trying node: {node}")
                response = self._request_session_manager.make_post_request(
                    node, request_data, **self.get_request_kwargs()
                )
                logging.warning(f"Successful connection to: {node}")
                return response
            except Exception as e:
                logging.warning(f"Request to {node} failed: {e}")
                self._mark_node_as_unavailable(node)
        raise Exception("All nodes are currently unavailable.")

    @handle_request_caching
    def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        self.logger.debug(f"Making request HTTP. URIs: {self.endpoint_uris}, Method: {method}")
        request_data = self.encode_rpc_request(method, params)
        raw_response = self._make_request(method, request_data)
        response = self.decode_rpc_response(raw_response)
        self.logger.debug(f"Received response HTTP. URIs: {self.endpoint_uris}, Method: {method}, Response: {response}")
        return response

    def make_batch_request(
        self, batch_requests: List[Tuple[RPCEndpoint, Any]]
    ) -> List[RPCResponse]:
        nodes = self._get_refreshed_nodes()
        for node in nodes:
            self.logger.debug(f"Making batch request HTTP, uri: `{node}`")
            request_data = self.encode_batch_rpc_request(batch_requests)
            try:
                raw_response = self._request_session_manager.make_post_request(
                    node, request_data, **self.get_request_kwargs()
                )
                self.logger.debug("Received batch response HTTP.")
                responses_list = cast(List[RPCResponse], self.decode_rpc_response(raw_response))
                return sort_batch_response_by_response_ids(responses_list)
            except Exception as e:
                self._mark_node_as_unavailable(node)
                self.logger.warning(f"Batch request to {node} failed: {e}")
        raise Exception("All nodes are currently unavailable.")