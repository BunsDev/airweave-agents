"""The services for handling OAuth2 authentication and token exchange for integrations."""

import base64
from typing import Optional
from urllib.parse import urlencode
from uuid import UUID

import httpx
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api.context import ApiContext
from airweave.core import credentials
from airweave.core.config import settings
from airweave.core.exceptions import NotFoundException, TokenRefreshError
from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import ConnectionStatus
from airweave.db.unit_of_work import UnitOfWork
from airweave.models.integration_credential import IntegrationType
from airweave.platform.auth.schemas import (
    AuthType,
    BaseAuthSettings,
    OAuth2Settings,
    OAuth2TokenResponse,
)
from airweave.platform.auth.settings import integration_settings


class OAuth2Service:
    """Service class for handling OAuth2 authentication and token exchange."""

    @staticmethod
    async def generate_auth_url(
        oauth2_settings: OAuth2Settings,
        client_id: Optional[str] = None,
    ) -> str:
        """Generate the OAuth2 authorization URL for an integration.

        Args:
            oauth2_settings: The OAuth2 settings for the integration
            client_id: Optional client ID to override the default one

        Returns:
            The authorization URL for the OAuth2 flow

        Raises:
            HTTPException: If there's an error generating the authorization URL
        """
        redirect_uri = OAuth2Service._get_redirect_url(oauth2_settings.integration_short_name)

        # if client_id is not provided, get it from the integration settings
        if not client_id:
            client_id = oauth2_settings.client_id

        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            **(oauth2_settings.additional_frontend_params or {}),
        }

        if oauth2_settings.scope:
            params["scope"] = oauth2_settings.scope

        auth_url = f"{oauth2_settings.url}?{urlencode(params)}"

        return auth_url

    @staticmethod
    async def exchange_authorization_code_for_token(
        ctx: ApiContext,
        source_short_name: str,
        code: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> OAuth2TokenResponse:
        """Exchange an authorization code for an OAuth2 token.

        Args:
        ----
            ctx (ApiContext): The API context.
            source_short_name (str): The short name of the integration source.
            code (str): The authorization code to exchange.
            client_id (Optional[str]): Optional client ID to override the default.
            client_secret (Optional[str]): Optional client secret to override the default.

        Returns:
        -------
            OAuth2TokenResponse: The response containing the access token and other details.

        Raises:
        ------
            HTTPException: If settings are not found for the source or token exchange fails.
        """
        # Get the settings for this source to generate the URL
        oauth2_settings = await integration_settings.get_by_short_name(source_short_name)
        if not oauth2_settings:
            raise HTTPException(
                status_code=404, detail=f"Settings not found for source: {source_short_name}"
            )

        redirect_uri = OAuth2Service._get_redirect_url(source_short_name)

        if not client_id and not client_secret:
            client_id = oauth2_settings.client_id
            client_secret = oauth2_settings.client_secret

        return await OAuth2Service._exchange_code(
            logger=ctx.logger,
            code=code,
            redirect_uri=redirect_uri,
            client_id=client_id,
            client_secret=client_secret,
            integration_config=oauth2_settings,
        )

    @staticmethod
    async def refresh_access_token(
        db: AsyncSession,
        integration_short_name: str,
        ctx: ApiContext,
        connection_id: UUID,
        decrypted_credential: dict,
        white_label: Optional[schemas.WhiteLabel] = None,
    ) -> OAuth2TokenResponse:
        """Refresh an access token using a refresh token.

        Rotates the refresh token if the integration is configured to do so.

        Args:
        ----
            db (AsyncSession): The database session.
            integration_short_name (str): The short name of the integration.
            ctx (ApiContext): The API context.
            connection_id (UUID): The ID of the connection to refresh the token for.
            decrypted_credential (dict): The token and optional config fields
            white_label (Optional[schemas.WhiteLabel]): White label configuration to use if
                available.

        Returns:
        -------
            OAuth2TokenResponse: The response containing the new access token and other details.

        Raises:
        ------
            TokenRefreshError: If token refresh fails
            NotFoundException: If the integration is not found

        """
        try:
            # Get and validate refresh token
            refresh_token = await OAuth2Service._get_refresh_token(ctx.logger, decrypted_credential)

            # Get and validate integration config
            integration_config = await OAuth2Service._get_integration_config(
                ctx.logger, integration_short_name
            )

            # Get client credentials
            # TODO: this is the only place we need to check the db for client credentials
            client_id, client_secret = await OAuth2Service._get_client_credentials(
                ctx.logger, integration_config, None, decrypted_credential
            )

            # Override with white label credentials if available
            if white_label and white_label.source_short_name == integration_short_name:
                client_id = white_label.client_id
                client_secret = white_label.client_secret

            # Prepare request parameters
            headers, payload = OAuth2Service._prepare_token_request(
                ctx.logger, integration_config, refresh_token, client_id, client_secret
            )

            # Make request and handle response
            response = await OAuth2Service._make_token_request(
                ctx.logger, integration_config.backend_url, headers, payload
            )

            # Handle rotating refresh tokens if needed
            oauth2_token_response = await OAuth2Service._handle_token_response(
                db, response, integration_config, ctx, connection_id
            )

            return oauth2_token_response

        except Exception as e:
            ctx.logger.error(
                f"Token refresh failed for organization {ctx.organization.id} and "
                f"integration {integration_short_name}: {str(e)}"
            )
            raise

    @staticmethod
    async def _get_refresh_token(logger: ContextualLogger, decrypted_credential: dict) -> str:
        """Get refresh token from decrypted credentials.

        Args:
        ----
            logger (ContextualLogger): The logger to use.
            decrypted_credential (dict): The decrypted credentials containing the refresh token.

        Returns:
        -------
            str: The refresh token.

        Raises:
        ------
            TokenRefreshError: If no refresh token is found
        """
        refresh_token = decrypted_credential.get("refresh_token", None)
        if not refresh_token:
            error_message = "No refresh token found"
            logger.error(error_message)
            raise TokenRefreshError(error_message)
        return refresh_token

    @staticmethod
    async def _get_integration_config(
        logger: ContextualLogger,
        integration_short_name: str,
    ) -> schemas.Source | schemas.Destination | schemas.EmbeddingModel:
        """Get and validate integration configuration exists.

        Args:
        ----
            logger (ContextualLogger): The logger to use.
            integration_short_name (str): The short name of the integration.

        Returns:
        -------
            schemas.Source | schemas.Destination | schemas.EmbeddingModel: The integration
                configuration.

        Raises:
        ------
            NotFoundException: If integration configuration is not found

        """
        integration_config = await integration_settings.get_by_short_name(integration_short_name)
        if not integration_config:
            error_message = f"Configuration for {integration_short_name} not found"
            logger.error(error_message)
            raise NotFoundException(error_message)
        return integration_config

    @staticmethod
    async def _get_client_credentials(
        logger: ContextualLogger,
        integration_config: schemas.Source | schemas.Destination | schemas.EmbeddingModel,
        auth_fields: Optional[dict] = None,
        decrypted_credential: Optional[dict] = None,
    ) -> tuple[str, str]:
        """Get client credentials based on priority ordering.

        Args:
        ----
            logger (ContextualLogger): The logger to use.
            integration_config: The integration configuration.
            auth_fields: Optional additional authentication fields for the connection.
            decrypted_credential: Optional decrypted credentials that may contain client ID/secret.

        Returns:
        -------
            tuple[str, str]: The client ID and client secret.

        Priority order:
        1. From decrypted_credential (if available)
        2. From auth_fields (if available)
        3. From integration_config (as fallback)
        """
        client_id = integration_config.client_id
        client_secret = integration_config.client_secret

        # First check decrypted_credential
        if decrypted_credential:
            client_id = decrypted_credential.get("client_id", client_id)
            client_secret = decrypted_credential.get("client_secret", client_secret)

        # Then check auth_fields
        if auth_fields:
            client_id = auth_fields.get("client_id", client_id)
            client_secret = auth_fields.get("client_secret", client_secret)

        return client_id, client_secret

    @staticmethod
    def _prepare_token_request(
        logger: ContextualLogger,
        integration_config: schemas.Source | schemas.Destination | schemas.EmbeddingModel,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ) -> tuple[dict, dict]:
        """Prepare headers and payload for token refresh request.

        Args:
        ----
            logger (ContextualLogger): The logger to use.
            integration_config (schemas.Source | schemas.Destination | schemas.EmbeddingModel):
                The integration configuration.
            refresh_token (str): The refresh token.
            client_id (str): The client ID.
            client_secret (str): The client secret.

        Returns:
        -------
            tuple[dict, dict]: The headers and payload.

        """
        headers = {
            "Content-Type": integration_config.content_type,
        }

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        if integration_config.client_credential_location == "header":
            encoded_credentials = OAuth2Service._encode_client_credentials(client_id, client_secret)
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            payload["client_id"] = client_id
            payload["client_secret"] = client_secret

        # Log the request details for debugging
        logger.info(
            f"OAuth2 code exchange request - "
            f"URL: {integration_config.backend_url}, "
            f"Redirect URI: {integration_config.backend_url}, "
            f"Client ID: {client_id}, "
            f"Code length: {len(refresh_token)}, "
            f"Grant type: {payload['grant_type']}, "
            f"Credential location: {integration_config.client_credential_location}"
        )

        return headers, payload

    @staticmethod
    async def _make_token_request(
        logger: ContextualLogger, url: str, headers: dict, payload: dict
    ) -> httpx.Response:
        """Make the token refresh request."""
        logger.info(f"Making token request to: {url}")

        try:
            async with httpx.AsyncClient() as client:
                logger.info(f"Sending request to {url}")
                response = await client.post(url, headers=headers, data=payload)

                logger.info(f"Received response: Status {response.status_code}, ")

                response.raise_for_status()
                return response

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error during token request: {e.response.status_code} "
                f"{e.response.reason_phrase}"
            )

            # Try to log the error response
            try:
                error_content = e.response.json()
                logger.error(f"Error response body: {error_content}")
            except Exception:
                logger.error(f"Error response text: {e.response.text}")

            raise
        except Exception as e:
            logger.error(f"Unexpected error during token request: {str(e)}")
            raise

    @staticmethod
    async def _handle_token_response(
        db: AsyncSession,
        response: httpx.Response,
        integration_config: schemas.Source | schemas.Destination | schemas.EmbeddingModel,
        ctx: ApiContext,
        connection_id: UUID,
    ) -> OAuth2TokenResponse:
        """Handle the token response and update refresh token if needed.

        Args:
        ----
            db (AsyncSession): The database session.
            logger (ContextualLogger): The logger to use.
            response (httpx.Response): The response from the token refresh request.
            integration_config (schemas.Source | schemas.Destination | schemas.EmbeddingModel):
                The integration configuration.
            ctx (ApiContext): The API context.
            connection_id (UUID): The ID of the connection to update.

        Returns:
        -------
            OAuth2TokenResponse: The response containing the new access token and other details.
        """
        oauth2_token_response = OAuth2TokenResponse(**response.json())

        if integration_config.auth_type == "oauth2_with_refresh_rotating":
            # Get connection and its credential
            connection = await crud.connection.get(db=db, id=connection_id, ctx=ctx)
            integration_credential = await crud.integration_credential.get(
                db=db, id=connection.integration_credential_id, ctx=ctx
            )

            # Update the credentials with the new refresh token
            current_credentials = credentials.decrypt(integration_credential.encrypted_credentials)
            current_credentials["refresh_token"] = oauth2_token_response.refresh_token

            # Encrypt and update the credentials
            encrypted_credentials = credentials.encrypt(current_credentials)
            await crud.integration_credential.update(
                db=db,
                db_obj=integration_credential,
                obj_in={"encrypted_credentials": encrypted_credentials},
                ctx=ctx,
            )

        return oauth2_token_response

    @staticmethod
    def _encode_client_credentials(client_id: str, client_secret: str) -> str:
        """Encodes the client ID and client secret in Base64.

        Args:
        ----
            client_id (str): The client ID.
            client_secret (str): The client secret.

        Returns:
        -------
            str: The Base64-encoded client credentials.

        """
        credentials = f"{client_id}:{client_secret}"
        credentials_bytes = credentials.encode("ascii")
        base64_bytes = base64.b64encode(credentials_bytes)
        base64_credentials = base64_bytes.decode("ascii")
        return base64_credentials

    @staticmethod
    def _get_redirect_url(integration_short_name: str) -> str:
        """Private method to generate the appropriate redirect URI based on environment.

        Args:
        ----
            integration_short_name: The short name of the integration.

        Returns:
        -------
            str: The redirect URI.

        """
        app_url = settings.app_url

        # if the oauth app cannot redirect to localhost because of http
        # paste: https://redirectmeto.com/ before the redirect uri
        # and set change the redirect uri in the oauth app as well

        return f"{app_url}/auth/callback/{integration_short_name}"

    @staticmethod
    async def generate_auth_url_for_whitelabel(
        db: AsyncSession, white_label: schemas.WhiteLabel
    ) -> str:
        """Generate the OAuth2 authorization URL for a white label integration."""
        source = await crud.source.get_by_short_name(
            db=db, short_name=white_label.source_short_name
        )

        if not source:
            raise NotFoundException(f"Source not found: {white_label.source_short_name}")

        integration_config = await integration_settings.get_by_short_name(source.short_name)

        if not integration_config:
            raise NotFoundException("Integration not found")

        # Use white_label's redirect_url instead of the default one
        params = {
            "response_type": "code",
            "client_id": white_label.client_id,
            "redirect_uri": white_label.redirect_url,
            **(integration_config.additional_frontend_params or {}),
        }

        if integration_config.scope:
            params["scope"] = integration_config.scope

        auth_url = f"{integration_config.url}?{urlencode(params)}"
        return auth_url

    @staticmethod
    async def exchange_code_for_whitelabel(
        logger: ContextualLogger,
        code: str,
        white_label: schemas.WhiteLabel,
    ) -> OAuth2TokenResponse:
        """Exchange OAuth2 authorization code for tokens using white label credentials.

        Args:
        ----
            logger: The logger to use.
            code: The authorization code to exchange
            white_label: The white label configuration to use

        Returns:
        -------
            OAuth2TokenResponse: The response containing the access token and other token details.

        Raises:
        ------
            NotFoundException: If the integration is not found
        """
        integration_config = await integration_settings.get_by_short_name(
            white_label.source_short_name
        )
        if not integration_config:
            raise NotFoundException(f"Integration {white_label.source_short_name} not found.")

        logger.info(f"Exchanging code for white label: {white_label.source_short_name}")
        logger.info(f"Client ID: {white_label.client_id}")
        return await OAuth2Service._exchange_code(
            code=code,
            redirect_uri=white_label.redirect_url,
            client_id=white_label.client_id,
            client_secret=white_label.client_secret,
            integration_config=integration_config,
        )

    @staticmethod
    async def _exchange_code(
        *,
        logger: ContextualLogger,
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
        integration_config: schemas.Source | schemas.Destination | schemas.EmbeddingModel,
    ) -> OAuth2TokenResponse:
        """Core method to exchange an authorization code for tokens.

        Args:
        ----
            logger: The logger to use.
            code: The authorization code to exchange
            redirect_uri: The redirect URI used in the authorization request
            client_id: The OAuth2 client ID
            client_secret: The OAuth2 client secret
            integration_config: The integration configuration

        Returns:
        -------
            OAuth2TokenResponse: The response containing the access token and other token details.

        Raises:
        ------
            HTTPException: If the token exchange fails
        """
        headers = {
            "Content-Type": integration_config.content_type,
        }

        payload = {
            "grant_type": integration_config.grant_type,
            "code": code,
            "redirect_uri": redirect_uri,
        }

        if integration_config.client_credential_location == "header":
            encoded_credentials = OAuth2Service._encode_client_credentials(client_id, client_secret)
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            payload["client_id"] = client_id
            payload["client_secret"] = client_secret

        # Log the request details for debugging
        logger.info(
            f"OAuth2 code exchange request - "
            f"URL: {integration_config.backend_url}, "
            f"Redirect URI: {redirect_uri}, "
            f"Client ID: {client_id}, "
            f"Code length: {len(code)}, "
            f"Grant type: {integration_config.grant_type}, "
            f"Credential location: {integration_config.client_credential_location}"
        )

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    integration_config.backend_url, headers=headers, data=payload
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Log the actual error response from the OAuth provider
            logger.error(
                f"OAuth2 token exchange failed - Status: {e.response.status_code}, "
                f"Response text: {e.response.text}"
            )
            raise HTTPException(status_code=400, detail=e.response.text) from e
        except Exception as e:
            logger.error(f"Failed to exchange authorization code: {str(e)}")
            raise HTTPException(
                status_code=400, detail="Failed to exchange authorization code"
            ) from e

        data = response.json()

        # Normalize provider-specific responses (e.g., Slack nests tokens under authed_user)
        access_token = data.get("access_token") or (data.get("authed_user") or {}).get(
            "access_token"
        )
        refresh_token = data.get("refresh_token") or (data.get("authed_user") or {}).get(
            "refresh_token"
        )
        token_type = data.get("token_type") or (data.get("authed_user") or {}).get("token_type")
        expires_in = data.get("expires_in") or (data.get("authed_user") or {}).get("expires_in")
        scope = data.get("scope") or (data.get("authed_user") or {}).get("scope")

        if access_token:
            # Construct a normalized OAuth2TokenResponse while preserving extra fields for audit
            response = OAuth2TokenResponse(
                access_token=access_token,
                token_type=token_type,
                expires_in=expires_in,
                refresh_token=refresh_token,
                scope=scope,
                extra_fields=data,
            )
        else:
            # Fallback: attempt direct parsing (for providers that already match the schema)
            response = OAuth2TokenResponse(**data)

        return response

    @staticmethod
    async def create_oauth2_connection_for_whitelabel(
        db: AsyncSession,
        white_label: schemas.WhiteLabel,
        code: str,
        ctx: ApiContext,
    ) -> schemas.Connection:
        """Create a new OAuth2 connection using white label credentials.

        Args:
        ----
            db: Database session
            white_label: The white label configuration to use
            code: The authorization code to exchange
            ctx: The API context

        Returns:
        -------
            schemas.Connection: The created connection
        """
        source = await crud.source.get_by_short_name(db, white_label.source_short_name)
        if not source:
            raise NotFoundException(f"Source not found: {white_label.source_short_name}")

        settings = await integration_settings.get_by_short_name(source.short_name)
        if not settings:
            raise NotFoundException("Integration not found")

        if not OAuth2Service._supports_oauth2(source.auth_type):
            raise HTTPException(status_code=400, detail="Source does not support OAuth2")

        # Exchange code for token using white label credentials
        oauth2_response = await OAuth2Service.exchange_code_for_whitelabel(
            ctx.logger, code=code, white_label=white_label
        )

        return await OAuth2Service._create_connection(
            db=db,
            source=source,
            settings=settings,
            oauth2_response=oauth2_response,
            ctx=ctx,
        )

    @staticmethod
    def _supports_oauth2(auth_type: AuthType) -> bool:
        """Check if the auth type supports OAuth2."""
        return auth_type in (
            AuthType.oauth2,
            AuthType.oauth2_with_refresh,
            AuthType.oauth2_with_refresh_rotating,
        )

    @staticmethod
    async def _create_connection(
        db: AsyncSession,
        source: schemas.Source,
        settings: BaseAuthSettings,
        oauth2_response: OAuth2TokenResponse,
        ctx: ApiContext,
    ) -> schemas.Connection:
        """Create a new connection with OAuth2 credentials."""
        # Prepare credentials based on auth type
        decrypted_credentials = (
            {"access_token": oauth2_response.access_token}
            if settings.auth_type == AuthType.oauth2
            else {
                "refresh_token": oauth2_response.refresh_token,
                "access_token": oauth2_response.access_token,
            }
        )

        encrypted_credentials = credentials.encrypt(decrypted_credentials)

        async with UnitOfWork(db) as uow:
            # Create integration credential
            integration_credential_in = schemas.IntegrationCredentialCreate(
                name=f"{source.name} - {ctx.organization.id}",
                description=(f"OAuth2 credentials for {source.name} - {ctx.organization.id}"),
                integration_short_name=source.short_name,
                integration_type=IntegrationType.SOURCE,
                auth_type=source.auth_type,
                encrypted_credentials=encrypted_credentials,
            )

            integration_credential = await crud.integration_credential.create(
                uow.session, obj_in=integration_credential_in, ctx=ctx, uow=uow
            )

            await uow.session.flush()

            # Create connection
            connection_in = schemas.ConnectionCreate(
                name=f"Connection to {source.name}",
                integration_type=IntegrationType.SOURCE,
                status=ConnectionStatus.ACTIVE,
                integration_credential_id=integration_credential.id,
                short_name=source.short_name,
            )

            connection = await crud.connection.create(
                uow.session, obj_in=connection_in, ctx=ctx, uow=uow
            )

            await uow.commit()
            await uow.session.refresh(connection)

        return connection


oauth2_service = OAuth2Service()
