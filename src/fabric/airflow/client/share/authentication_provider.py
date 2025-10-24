from azure.identity import ClientSecretCredential
from azure.identity import InteractiveBrowserCredential
from azure.core.exceptions import ClientAuthenticationError

from datetime import datetime, timedelta
import jwt
import typing as t


class AuthenticationProvider:
    """
    Provides authentication for Airflow API clients.
    
    Supports both Service Principal Name (SPN) authentication and interactive browser authentication.
    Automatically caches tokens and handles token expiry with a 5-minute buffer.
    """

    def __init__(
        self, 
        tenant_id: t.Optional[str] = None,
        client_id: t.Optional[str] = None, 
        client_secret: t.Optional[str] = None, 
        authority: str = "https://login.microsoftonline.com", 
        scope: str = "https://api.fabric.microsoft.com/.default"
    ):
        """
        Initialize the AuthenticationProvider with configuration information.
        
        Args:
            tenant_id (str, optional): The Azure AD tenant ID
            client_id (str, optional): The application client ID for SPN authentication
            client_secret (str, optional): The application client secret for SPN authentication
            authority (str): The authentication authority URL
            scope (str): The default scope for token requests
        """
        self.authority = authority
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

        self._cached_token = None
        self._token_expiry = None
        
    def _is_token_expired(self) -> bool:
        """
        Check if the cached token is expired.
        
        Returns:
            bool: True if token is expired or doesn't exist, False otherwise
        """
        if not self._cached_token or not self._token_expiry:
            return True
        
        # Add a 5-minute buffer before actual expiry
        buffer_time = timedelta(minutes=5)
        return datetime.utcnow() >= (self._token_expiry - buffer_time)

    def _extract_token_expiry(self, token: str) -> datetime:
        """
        Extract expiry time from JWT token.
        
        Args:
            token (str): The JWT access token
            
        Returns:
            datetime: Token expiry time in UTC
        """
        try:
            # Decode token without verification to get expiry
            decoded = jwt.decode(token, options={"verify_signature": False})
            exp_timestamp = decoded.get('exp')
            if exp_timestamp:
                return datetime.utcfromtimestamp(exp_timestamp)
        except Exception:
            # If we can't decode the token, assume it expires in 1 hour
            return datetime.utcnow() + timedelta(hours=1)
        
        # Default to 1 hour if no exp claim found
        return datetime.utcnow() + timedelta(hours=1)

    def get_token(self) -> str:
        """
        Get an access token. If token was cached and not expired, return cached token.
        Otherwise, automatically choose between interactive and SPN authentication.
        
        Returns:
            str: Access token
            
        Raises:
            ValueError: If no authentication credentials are available
            ClientAuthenticationError: If authentication fails
        """
        # Check if we have a valid cached token
        if not self._is_token_expired():
            # Type checker: we know _cached_token is not None because _is_token_expired returned False
            assert self._cached_token is not None
            return self._cached_token
        
        # If token was cached but expired, clear it
        if self._cached_token and self._is_token_expired():
            self.clear_token_cache()
        
        # If no credentials provided, can't authenticate
        if not self.tenant_id:
            raise ValueError(
                "No authentication credentials available. "
                "Please provide tenant_id and either client_secret (for SPN) or use interactive authentication."
            )

        # Use instance scope
        token_scope = self.scope
        
        try:
            # Decide authentication method based on whether client_secret is provided
            if self.client_secret:
                # Validate required SPN credentials
                if not self.tenant_id or not self.client_id:
                    raise ValueError("tenant_id and client_id are required for SPN authentication")
                
                print(f"Attempting SPN authentication...")
                print(f"Tenant ID: {self.tenant_id}")
                print(f"Client ID: {self.client_id}")
                print(f"Scope: {token_scope}")
                
                # Use SPN authentication - type checker knows these are not None due to validation above
                credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,  # type: ignore
                    client_id=self.client_id,  # type: ignore
                    client_secret=self.client_secret
                )
                token_response = credential.get_token(token_scope)
            else:
                print(f"Attempting interactive authentication...")
                print(f"Scope: {token_scope}")
                
                # Use interactive authentication
                credential = InteractiveBrowserCredential(tenant_id=self.tenant_id)
                token_response = credential.get_token(token_scope)
            
            # Cache the token and its expiry
            self._cached_token = token_response.token
            
            # Try to get expiry from token response first, then from JWT
            if hasattr(token_response, 'expires_on') and token_response.expires_on:
                self._token_expiry = datetime.utcfromtimestamp(token_response.expires_on)
            else:
                self._token_expiry = self._extract_token_expiry(self._cached_token)
            
            print(f"Token acquired successfully. Expires: {self._token_expiry}")
            return self._cached_token
            
        except ClientAuthenticationError as e:
            error_msg = str(e)
            print(f"Authentication failed: {error_msg}")
            
            # Provide specific guidance based on error
            if "AADSTS5000224" in error_msg:
                print("\nTroubleshooting AADSTS5000224:")
                print("1. The Fabric API scope might not be available in your tenant")
                print("2. Try using a different scope like 'https://graph.microsoft.com/.default'")
                print("3. Verify your application has the correct API permissions")
                print("4. Check if Microsoft Fabric is enabled in your tenant")
                print("5. The application might need admin consent for the requested permissions")
            elif "AADSTS70011" in error_msg:
                print("\nTroubleshooting: Invalid scope - check the scope format")
            elif "AADSTS7000215" in error_msg:
                print("\nTroubleshooting: Invalid client secret provided")
            elif "AADSTS90002" in error_msg:
                print("\nTroubleshooting: Invalid tenant ID")
            
            raise
        except Exception as e:
            print(f"Unexpected error during authentication: {e}")
            raise

    def clear_token_cache(self) -> None:
        """
        Clear the cached token, forcing a new authentication on next get_token call.
        """
        self._cached_token = None
        self._token_expiry = None

    def get_token_info(self) -> dict:
        """
        Get information about the current cached token.
        
        Returns:
            dict: Token information including expiry status
        """
        return {
            'has_token': self._cached_token is not None,
            'is_expired': self._is_token_expired(),
            'expiry_time': self._token_expiry.isoformat() if self._token_expiry else None,
            'default_scope': self.scope,
            'auth_method': 'SPN' if self.client_secret else 'Interactive'
        }
