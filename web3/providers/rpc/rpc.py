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
from venv import logger

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
from ...exceptions import Web3ValidationError, CannotHandleRequest

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
        self._unavailable_providers = {}

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

    def _get_prioritized_providers(self):
        """Return the list of prioritized available providers, cleaning out expired unavailable ones."""
        self._clean_unavailable_providers()

        available_providers = [provider for provider in self.endpoint_uris if provider not in self._unavailable_providers]
        unavailable_providers = [provider for provider in self.endpoint_uris if provider in self._unavailable_providers]
        return available_providers + unavailable_providers

    def _clean_unavailable_providers(self) -> None:
        """Clean up providers that are past their retry period."""
        current_time = time.time()
        providers_to_clean = [provider for provider, retry_time in self._unavailable_providers.items() if current_time > retry_time]
        for provider in providers_to_clean:
            self._unavailable_providers.pop(provider)

    def _mark_provider_as_unavailable(self, provider: URI) -> None:
        """Mark a provider as unavailable and retry later."""
        retry_after = 60
        self._unavailable_providers[provider] = time.time() + retry_after

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
        available_providers = self._get_prioritized_providers()
        if not available_providers:
            raise CannotHandleRequest("Endpoint uris are not provided.")

        for provider in available_providers:
            try:
                response = self._request_session_manager.make_post_request(
                    provider, request_data, **self.get_request_kwargs()
                )
                if provider in self._unavailable_providers:
                    self._unavailable_providers.pop(provider)
                return response
            except Exception as e:
                logging.warning(f"Request to {provider} failed: {e}")
                self._mark_provider_as_unavailable(provider)
        raise CannotHandleRequest("All providers are currently unavailable.")

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
        available_providers = self._get_prioritized_providers()
        if not available_providers:
            raise CannotHandleRequest("Endpoint uris are not provided.")

        for provider in available_providers:
            self.logger.debug(f"Making batch request HTTP, uri: `{provider}`")
            request_data = self.encode_batch_rpc_request(batch_requests)
            try:
                raw_response = self._request_session_manager.make_post_request(
                    provider, request_data, **self.get_request_kwargs()
                )
                self.logger.debug("Received batch response HTTP.")
                if provider in self._unavailable_providers:
                    self._unavailable_providers.pop(provider)
                responses_list = cast(List[RPCResponse], self.decode_rpc_response(raw_response))
                return sort_batch_response_by_response_ids(responses_list)
            except Exception as e:
                self._mark_provider_as_unavailable(provider)
                self.logger.warning(f"Batch request to {provider} failed: {e}")
            raise CannotHandleRequest("All providers are currently unavailable.")
