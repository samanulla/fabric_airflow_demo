import typing as t


class APIError(Exception):
    """Base exception for all API errors"""
    
    def __init__(
        self, 
        status: int,
        message: str, 
        request_id: t.Optional[str] = None, 
        body: t.Any = None,
        headers: t.Optional[dict] = None
    ):
        """
        Initialize the base API exception.
        
        Args:
            status: HTTP status code
            message: Error message
            request_id: Request ID from x-request-id or x-ms-request-id header
            body: Response body (parsed JSON or text)
            headers: Response headers dictionary
        """
        self.status = status
        self.message = message
        self.request_id = request_id
        self.body = body
        self.headers = headers
        
        # Build error message
        error_msg = f"[{status}] {message}"
        if request_id:
            error_msg += f" (Request ID: {request_id})"
        
        super().__init__(error_msg)
    
    def __repr__(self):
        return f"{self.__class__.__name__}(status={self.status}, message='{self.message}', request_id='{self.request_id}')"
    
    # Backward compatibility properties
    @property
    def status_code(self) -> int:
        """Alias for status (backward compatibility)"""
        return self.status
    
    @property
    def response_body(self) -> t.Any:
        """Alias for body (backward compatibility)"""
        return self.body


class ClientError(APIError):
    """4xx client errors (400-499)"""
    pass


class ServerError(APIError):
    """5xx server errors (500-599)"""
    pass


class AuthenticationError(ClientError):
    """401 authentication errors"""
    
    def __init__(
        self, 
        message: str = "Authentication failed", 
        request_id: t.Optional[str] = None, 
        body: t.Any = None,
        headers: t.Optional[dict] = None
    ):
        super().__init__(status=401, message=message, request_id=request_id, body=body, headers=headers)


class ForbiddenError(ClientError):
    """403 permission errors"""
    
    def __init__(
        self, 
        message: str = "Forbidden", 
        request_id: t.Optional[str] = None, 
        body: t.Any = None,
        headers: t.Optional[dict] = None
    ):
        super().__init__(status=403, message=message, request_id=request_id, body=body, headers=headers)


class NotFoundError(ClientError):
    """404 not found errors"""
    
    def __init__(
        self, 
        message: str = "Resource not found", 
        request_id: t.Optional[str] = None, 
        body: t.Any = None,
        headers: t.Optional[dict] = None
    ):
        super().__init__(status=404, message=message, request_id=request_id, body=body, headers=headers)


class ValidationError(ClientError):
    """400 validation errors"""
    
    def __init__(
        self, 
        message: str = "Validation failed", 
        request_id: t.Optional[str] = None, 
        body: t.Any = None,
        headers: t.Optional[dict] = None
    ):
        super().__init__(status=400, message=message, request_id=request_id, body=body, headers=headers)