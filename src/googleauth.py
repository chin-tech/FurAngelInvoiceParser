import os
import pickle
import base64
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.cloud import secretmanager
from utils import error_logger
from typing import Optional
from pathlib import Path

from google_auth_oauthlib.flow import Flow

class SecretManager:
    def __init__(self, project_id: str, secret_names: list[str]) -> None:
        self.id = project_id
        self.secret_paths = {}
        self.secret_names = secret_names
        for secret in secret_names:
            self.secret_paths[secret] = f'projects/{self.id}/secrets/{secret}/versions'
        

    def retrieve_secret_from_info(self, secret_name: str, account: dict, version: str = "latest",) -> Credentials:
        client = secretmanager.SecretManagerServiceClient.from_service_account_info(
            account,
        )
        secret_path = f'{self.secret_paths[secret_name]}/{version}'
        response = client.access_secret_version(name=secret_path)
        return pickle.loads(response.payload.data)

    @error_logger(reraise=True)
    def retrieve_secret_from_file(self, secret_name: str, account_file: str, version: str = "latest",) -> Credentials:
        client = secretmanager.SecretManagerServiceClient.from_service_account_file(
            account_file,
        )
        secret_path = f'{self.secret_paths[secret_name]}/{version}'
        response = client.access_secret_version(name=secret_path)
        return pickle.loads(response.payload.data)

    @error_logger()
    def update_secret(self, secret_name: str, account: dict, new_value) -> None:
        client = secretmanager.SecretManagerServiceClient.from_service_account_info(
            account,
        )
        payload = secretmanager.SecretPayload(data=pickle.dumps(new_value))
        secret_path = self.secret_paths[secret_name]
        new_version = client.add_secret_version(
            parent=secret_path,
            payload = payload
        )
        old_versions = client.list_secret_versions(parent=secret_path)
        for v in old_versions:
            if v.name != new_version.name:
                client.destroy_secret_version(name=v.name)

class CredentialManager:
    def __init__(self, scopes: list[str], oauth_client_config_path: str):
        self.scopes = scopes
        if not Path(oauth_client_config_path).exists():
            raise FileNotFoundError
        with open(oauth_client_config_path, 'rb') as f:
            self.client_config_json = json.load(f)
        self.oauth_client_config_path = oauth_client_config_path
        self._creds: Optional[Credentials] = None # Internal storage for credentials

    @property
    def credentials(self) -> Optional[Credentials]:
        """Returns the current credentials."""
        return self._creds

    @credentials.setter
    def credentials(self, creds: Credentials):
        """Sets the current credentials."""
        self._creds = creds


    @error_logger(reraise=False)
    def load_from_service_account_file(self, service_account_key_path: str) -> Optional[Credentials]:
        """
        Loads credentials from a Google Service Account JSON key file.
        This is typically used for server-to-server communication.
        """
        if not service_account_key_path or not Path(service_account_key_path).exists():
            print(f"Service account key file not found: {service_account_key_path}")
            return None

        try:
            creds = service_account.Credentials.from_service_account_file(
                service_account_key_path, scopes=self.scopes
            )
            # Service account credentials do not expire in the same way user credentials do,
            # and they manage their own token refreshing internally.
            if creds:
                self._creds = creds
                print(f"Credentials loaded from service account: {service_account_key_path}")
                return creds
        except Exception as e:
            print(f"Error loading service account credentials from {service_account_key_path}: {e}")
            self._creds = None
        return None

    @error_logger(reraise=False)
    def load_from_token_file(self, token_path: str, secure: bool = True) -> Optional[Credentials]:
        """
        Loads credentials from a local token file, refreshes if expired, or initiates
        a local server flow if not found/invalid.
        """
        creds: Optional[Credentials] = None
        if token_path and os.path.exists(token_path):
            try:
                with open(token_path, "rb") as tk:
                    creds = pickle.load(tk)
            except Exception as e:
                print(f"Error loading pickled credentials from {token_path}: {e}")
                creds = None

        if creds and creds.valid:
            self._creds = creds
            return creds

        # If not valid, try to refresh or initiate new flow
        if creds and creds.expired and creds.refresh_token:
            print("Credentials expired, attempting to refresh...")
            try:
                creds.refresh(Request())
                self._creds = creds
                if not secure:
                    self._save_to_token_file(token_path, creds)
                return creds
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                creds = None # Force a new flow if refresh fails

        if not creds: # No valid creds, or refresh failed, initiate local flow
            print("No valid credentials found, initiating local server flow...")
            flow = InstalledAppFlow.from_client_config(self.client_config_json, self.scopes)
            try:
                creds = flow.run_local_server(port=9999)
                self._creds = creds
                if not secure:
                    self._save_to_token_file(token_path, creds)
                return creds
            except Exception as e:
                print(f"Error running local server flow: {e}")
                return None
        return None # Should not be reached if previous steps succeeded or returned None

    @error_logger(reraise=False)
    def load_from_session_data(self, session_dict: dict) -> Optional[Credentials]:
        """
        Loads credentials from Flask session data.
        Assumes credentials are base64 encoded and pickled.
        """
        pickled_creds = session_dict.get('user_creds')
        if pickled_creds:
            creds = pickle.loads(base64.b64decode(pickled_creds))
            if creds and creds.valid:
                self._creds = creds
                return creds
            elif creds and creds.expired and creds.refresh_token:
                print("Session credentials expired, attempting to refresh...")
                creds.refresh(Request())
                self._creds = creds
                self._save_to_session_data(session_dict, creds) # Update refreshed creds in session
                return creds
        return None

    @error_logger()
    def _save_to_token_file(self, token_path: str, creds: Credentials) -> None:
        """Helper to save credentials to a token file."""
        with open(token_path, "wb") as tk:
            pickle.dump(creds, tk)
        print(f"Credentials saved to {token_path}")

    def _save_to_session_data(self, session_dict: dict, creds: Credentials) -> None:
        """Helper to save credentials to session data."""
        session_dict['user_creds'] = base64.b64encode(
            pickle.dumps(creds)
        ).decode()
        self._creds = creds

    def create_web_flow(self, redirect_url: str,  state: Optional[str] = None) -> Flow:
        """Creates an OAuth flow for web applications."""
        flow = Flow.from_client_config(self.client_config_json, scopes=self.scopes, state=state)
        flow.redirect_uri = redirect_url
        return flow

    



