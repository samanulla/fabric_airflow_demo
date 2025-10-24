import requests
import json
import typing as t
import dataclasses
import os
import logging

# Import exceptions from api_exceptions module
from fabric.airflow.client.share.api_exceptions import (
    APIError,
    ClientError,
    ServerError,
    AuthenticationError,
    ForbiddenError,
    NotFoundError,
    ValidationError
)

# Import AuthenticationProvider from separate module
from fabric.airflow.client.share.authentication_provider import AuthenticationProvider


# ---------- Setup logging for debug mode ----------
logger = logging.getLogger(__name__)

@dataclasses.dataclass
class ApiResponse:
    """Standard response format for all API calls."""
    status: int
    headers: dict
    body: t.Any

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "headers": self.headers,
            "body": self.body
        }


class BaseApiClient:
    """
    Generic base API client class that provides common functionality for derived classes.
    
    Features:
    - Authentication provider integration with token management
    - HTTP methods (GET, POST, PUT, PATCH, DELETE)
    - Error handling with appropriate exceptions
    - Debug mode with request/response logging
    - Standardized response format
    - Preview mode support
    
    This class is designed to be inherited by specific API clients.
    """

    def __init__(
        self,
        auth_provider: AuthenticationProvider,
        base_url: str = "https://api.fabric.microsoft.com/v1",
        token_scheme: str = "Bearer",
        timeout: t.Union[int, float] = 120,
        session: t.Optional[requests.Session] = None,
        debug: t.Optional[bool] = None,
        is_preview_enabled: bool = True,
    ):
        """
        Initialize the BaseApiClient.
        
        Args:
            auth_provider: AuthenticationProvider instance for token management
            base_url: Base URL for the API
            token_scheme: Token scheme (default: Bearer)
            timeout: Request timeout in seconds
            session: Optional requests session
            debug: Enable debug mode (prints requests/responses). If None, checks DEBUG environment variable
            preview: Whether to use preview API endpoints (adds ?preview=true to requests)
        """
        self.auth_provider = auth_provider
        self.base_url = base_url.rstrip("/")
        self.token_scheme = token_scheme
        self.timeout = timeout
        self._session = session or requests.Session()
        self.preview = is_preview_enabled
        
        # Debug mode - check parameter first, then environment variable
        if debug is not None:
            self.debug = debug
        else:
            self.debug = os.getenv('DEBUG', '').lower() in ('true', '1', 'yes', 'on')

    def _add_preview_param(self, params: t.Optional[dict] = None) -> t.Optional[dict]:
        """
        Add preview=true parameter if preview mode is enabled.
        
        Args:
            params: Existing query parameters
            
        Returns:
            Updated parameters with preview=true if needed
        """
        if not self.preview:
            return params
            
        if params is None:
            return {"preview": "true"}
        else:
            # Make a copy to avoid modifying the original
            updated_params = params.copy()
            updated_params["preview"] = "true"
            return updated_params

    # ----- Internal helpers (protected methods for derived classes) -----

    def _url(self, path: str, q: t.Optional[dict] = None) -> str:
        """
        Build full URL with query parameters.
        
        Args:
            path: API path relative to base URL
            q: Query parameters dictionary
            
        Returns:
            str: Complete URL
        """
        if q:
            from urllib.parse import urlencode
            return f"{self.base_url}/{path.lstrip('/')}?{urlencode(q, doseq=True)}"
        return f"{self.base_url}/{path.lstrip('/')}"

    def _headers(self, extra: t.Optional[dict] = None) -> dict:
        """
        Build headers with authentication token and merge with provided headers.
        
        Args:
            extra: Additional headers to include
            
        Returns:
            dict: Complete headers dictionary with authentication and defaults
        """
        # Start with provided headers (make a copy to avoid modifying original)
        headers = extra.copy() if extra else {}
        
        # Add default headers if not already present
        if "Accept" not in headers:
            headers["Accept"] = "application/json"
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
        
        # Add authentication and referer
        token = self.auth_provider.get_token()
        headers["Authorization"] = f"{self.token_scheme} {token}"
        headers["Referer"] = "FabricPythonAirflowClient"
        
        return headers

    def _extract_request_id(self, response: requests.Response, body: t.Any = None) -> t.Optional[str]:
        """
        Extract request ID from response. This method can be overridden by derived classes
        to implement custom request ID extraction logic.
        
        Default implementation checks common header names. Fabric APIs can override this
        to extract request ID from the response body JSON.
        
        Args:
            response: The HTTP response object
            body: Parsed response body (JSON or text)
            
        Returns:
            Optional[str]: Request ID if found, None otherwise
        """
        # Try common header names first
        request_id = (
            response.headers.get("x-request-id") or 
            response.headers.get("x-ms-request-id") or
            response.headers.get("request-id") or
            response.headers.get("x-correlation-id")
        )
        
        # If not found in headers and body is a dict, try to find it in body
        if not request_id and isinstance(body, dict):
            request_id = body.get("requestId") or body.get("request_id")
        
        return request_id

    def _build_exception(self, response: requests.Response) -> APIError:
        """
        Build appropriate exception based on response status code.
        
        Args:
            response: The HTTP response object
            
        Returns:
            APIError: Appropriate exception type (ClientError, ServerError, or specific subtypes)
        """
        message = ""
        body = None
        
        # Try to parse response body
        try:
            body = response.json()
            if isinstance(body, dict):
                message = body.get("description") or body.get("message") or body.get("error") or json.dumps(body)
            else:
                message = json.dumps(body)
        except ValueError:
            message = response.text or f"HTTP {response.status_code}"

        headers = dict(response.headers)
        
        # Extract request ID using overridable method
        request_id = self._extract_request_id(response, body)
        
        # Build appropriate exception based on status code
        status = response.status_code
        
        # Handle specific 4xx client errors
        if status == 400:
            return ValidationError(
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )
        elif status == 401:
            return AuthenticationError(
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )
        elif status == 403:
            return ForbiddenError(
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )
        elif status == 404:
            return NotFoundError(
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )
        elif 400 <= status < 500:
            # Generic client error for other 4xx codes
            return ClientError(
                status=status,
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )
        else:
            # 5xx server errors
            return ServerError(
                status=status,
                message=message,
                request_id=request_id,
                body=body,
                headers=headers
            )

    def _handle_response(self, resp: requests.Response, stream: bool = False, raise_for_status: bool = True) -> ApiResponse:
        """
        Handle HTTP response and return standardized ApiResponse.
        
        Args:
            resp: HTTP response object
            stream: Whether to return raw bytes content
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
            
        Raises:
            ValidationError: For 400 validation errors (if raise_for_status=True)
            AuthenticationError: For 401 authentication errors (if raise_for_status=True)
            ForbiddenError: For 403 permission errors (if raise_for_status=True)
            NotFoundError: For 404 not found errors (if raise_for_status=True)
            ClientError: For other 4xx client errors (if raise_for_status=True)
            ServerError: For 5xx server errors (if raise_for_status=True)
        """
        headers = dict(resp.headers)
        
        # Always create the ApiResponse object
        body = None
        if stream:
            body = resp.content
        elif resp.content:
            # Try JSON first, fallback to text
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype or "text/json" in ctype:
                try:
                    body = resp.json()
                except ValueError:
                    body = resp.text
            else:
                body = resp.text
        
        api_response = ApiResponse(status=resp.status_code, headers=headers, body=body)
        
        # Check if we should raise exceptions for non-success status codes
        if raise_for_status and resp.status_code not in (200, 201, 202, 204):
            raise self._build_exception(resp)
        
        return api_response
    
    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: t.Any = None,
        data: t.Any = None,
        headers: t.Optional[dict] = None,
        stream: bool = False,
        params: t.Optional[dict] = None,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make HTTP request and return standardized response.
        
        Args:
            method: HTTP method
            path: API path
            json_body: JSON body to send
            data: Raw data to send
            headers: Additional headers
            stream: Whether to return raw bytes content
            params: Query parameters
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response with status, headers, and body
        """
        updated_params = self._add_preview_param(params)
        url = self._url(path, q=updated_params)
        request_headers = self._headers(headers)
        
        # Log request in debug mode with better formatting
        if self.debug:
            self._log_request(method, url, request_headers, json_body, data)
        
        resp = self._session.request(
            method=method,
            url=url,
            headers=request_headers,
            json=json_body,
            data=data,
            timeout=self.timeout,
        )
        
        # Log response in debug mode with better formatting
        if self.debug:
            self._log_response(resp, stream)
        
        return self._handle_response(resp, stream=stream, raise_for_status=raise_for_status)

    def _log_request(
        self,
        method: str,
        url: str,
        headers: dict,
        json_body: t.Any = None,
        data: t.Any = None
    ):
        """Log HTTP request in a nicely formatted way."""
        logger.info("=" * 100)
        logger.info(f"🚀 {method} REQUEST")
        logger.info("=" * 100)
        logger.info(f"URL: {url}")
        logger.info("")
        
        # Log headers in a clean format
        logger.info("📋 HEADERS:")
        for key, value in headers.items():
            # Mask sensitive headers for security
            if key.lower() in ('authorization', 'x-api-key'):
                if len(value) > 30:
                    masked_value = f"{value[:15]}...{value[-10:]}"
                else:
                    masked_value = "***MASKED***"
                logger.info(f"  {key}: {masked_value}")
            else:
                logger.info(f"  {key}: {value}")
        logger.info("")
        
        # Log request body
        if json_body:
            logger.info("📦 JSON BODY:")
            try:
                formatted_json = json.dumps(json_body, indent=2, ensure_ascii=False)
                if len(formatted_json) > 3000:
                    lines = formatted_json.split('\n')
                    truncated_lines = lines[:50]  # Show first 50 lines
                    logger.info('\n'.join(truncated_lines))
                    logger.info(f"  ... [JSON truncated - showing first 50 lines of {len(lines)} total lines]")
                else:
                    logger.info(formatted_json)
            except Exception as e:
                logger.info(f"  [JSON serialization failed: {e}]")
                logger.info(f"  {str(json_body)[:1000]}...")
        elif data:
            logger.info("📦 REQUEST DATA:")
            if isinstance(data, bytes):
                try:
                    # Try to decode as UTF-8 text
                    text_data = data.decode('utf-8')
                    if len(text_data) > 2000:
                        logger.info(f"  {text_data[:2000]}...")
                        logger.info(f"  [Data truncated - total size: {len(data)} bytes]")
                    else:
                        logger.info(f"  {text_data}")
                except UnicodeDecodeError:
                    logger.info(f"  [Binary data - {len(data)} bytes]")
            elif isinstance(data, str):
                if len(data) > 2000:
                    logger.info(f"  {data[:2000]}...")
                    logger.info(f"  [Data truncated - total length: {len(data)} characters]")
                else:
                    logger.info(f"  {data}")
            else:
                logger.info(f"  [{type(data).__name__}] - {getattr(data, '__len__', lambda: 'unknown size')()}")
        else:
            logger.info("📦 BODY: [Empty]")
    
        logger.info("=" * 100)

    def _log_response(self, resp: requests.Response, stream: bool = False):
        """Log HTTP response in a nicely formatted way."""
        logger.info("📡 RESPONSE")
        logger.info("=" * 100)
        
        # Status with color-like indicators
        status_indicator = "✅" if 200 <= resp.status_code < 300 else "⚠️" if 400 <= resp.status_code < 500 else "❌"
        logger.info(f"STATUS: {status_indicator} {resp.status_code} {resp.reason}")
        logger.info("")
        
        # Log response headers
        logger.info("📋 RESPONSE HEADERS:")
        for key, value in resp.headers.items():
            logger.info(f"  {key}: {value}")
        logger.info("")
        
        # Log response body
        if not stream and resp.content:
            logger.info("📦 RESPONSE BODY:")
            try:
                # Try to parse as JSON for pretty formatting
                json_response = resp.json()
                formatted_json = json.dumps(json_response, indent=2, ensure_ascii=False)
                if len(formatted_json) > 3000:
                    lines = formatted_json.split('\n')
                    truncated_lines = lines[:50]  # Show first 50 lines
                    logger.info('\n'.join(truncated_lines))
                    logger.info(f"  ... [Response truncated - showing first 50 lines of {len(lines)} total lines]")
                else:
                    logger.info(formatted_json)
            except (ValueError, json.JSONDecodeError):
                # Not JSON, log as text
                text_response = resp.text
                if len(text_response) > 2000:
                    logger.info(f"{text_response[:2000]}...")
                    logger.info(f"  [Response truncated - total length: {len(text_response)} characters]")
                else:
                    logger.info(text_response)
        elif stream:
            logger.info("📦 RESPONSE BODY:")
            logger.info(f"  [Binary/Stream content - {len(resp.content) if resp.content else 0} bytes]")
            content_type = resp.headers.get('Content-Type', 'unknown')
            logger.info(f"  Content-Type: {content_type}")
        else:
            logger.info("📦 RESPONSE BODY: [Empty]")
        
        logger.info("=" * 100)
        logger.info("")

    def get(
        self,
        path: str,
        *,
        params: t.Optional[dict] = None,
        headers: t.Optional[dict] = None,
        stream: bool = False,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make a GET request.
        
        Args:
            path: API path (relative to base_url)
            params: Query parameters
            headers: Additional headers
            stream: Whether to return raw bytes content
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
        """
        return self._request("GET", path, params=params, headers=headers, stream=stream, raise_for_status=raise_for_status)

    def post(
        self,
        path: str,
        *,
        json_body: t.Any = None,
        data: t.Any = None,
        params: t.Optional[dict] = None,
        headers: t.Optional[dict] = None,
        stream: bool = False,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make a POST request.
        
        Args:
            path: API path (relative to base_url)
            json_body: JSON body to send
            data: Raw data to send (alternative to json_body)
            params: Query parameters
            headers: Additional headers
            stream: Whether to return raw bytes content
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
        """
        return self._request("POST", path, json_body=json_body, data=data, params=params, headers=headers, stream=stream, raise_for_status=raise_for_status)

    def put(
        self,
        path: str,
        *,
        json_body: t.Any = None,
        data: t.Any = None,
        params: t.Optional[dict] = None,
        headers: t.Optional[dict] = None,
        stream: bool = False,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make a PUT request.
        
        Args:
            path: API path (relative to base_url)
            json_body: JSON body to send
            data: Raw data to send (alternative to json_body)
            params: Query parameters
            headers: Additional headers
            stream: Whether to return raw bytes content
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
        """
        return self._request("PUT", path, json_body=json_body, data=data, params=params, headers=headers, stream=stream, raise_for_status=raise_for_status)

    def patch(
        self,
        path: str,
        *,
        json_body: t.Any = None,
        data: t.Any = None,
        params: t.Optional[dict] = None,
        headers: t.Optional[dict] = None,
        stream: bool = False,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make a PATCH request.
        
        Args:
            path: API path (relative to base_url)
            json_body: JSON body to send
            data: Raw data to send (alternative to json_body)
            params: Query parameters
            headers: Additional headers
            stream: Whether to return raw bytes content
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
        """
        return self._request("PATCH", path, json_body=json_body, data=data, params=params, headers=headers, stream=stream, raise_for_status=raise_for_status)

    def delete(
        self,
        path: str,
        *,
        params: t.Optional[dict] = None,
        headers: t.Optional[dict] = None,
        raise_for_status: bool = True,
    ) -> ApiResponse:
        """
        Make a DELETE request.
        
        Args:
            path: API path (relative to base_url)
            params: Query parameters
            headers: Additional headers
            raise_for_status: Whether to raise exceptions for non-success status codes
            
        Returns:
            ApiResponse: Standardized response object
        """        
        return self._request("DELETE", path, params=params, headers=headers, raise_for_status=raise_for_status)

