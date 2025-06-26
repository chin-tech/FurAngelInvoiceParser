import datetime
import pytest
import tempfile
import pickle
import base64
import json
from google.oauth2.credentials import Credentials
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path

from googleauth import CredentialManager  # Replace with your actual module name

# Sample values
SCOPES = ["https://www.googleapis.com/auth/drive"]


class DummyCreds(Credentials):
    def __init__(self, *, is_valid=True, is_expired=False, refresh_token="dummy-token"):
        super().__init__(token="dummy-token")
        self._is_valid = is_valid
        self._is_expired = is_expired
        self._refresh_token = refresh_token
        self.expiry = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=1)

    @property
    def valid(self):
        return self._is_valid

    @property
    def expired(self):
        return self._is_expired

    @property
    def refresh_token(self):
        return self._refresh_token

    def refresh(self, request):
        self._is_valid = True
        self._is_expired = False

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


@pytest.fixture
def temp_oauth_config():
    with tempfile.NamedTemporaryFile("w+", delete=False) as f:
        json.dump({"installed": {"client_id": "abc"}}, f)
        f.flush()
        yield f.name

@pytest.fixture
def dummy_creds():
    return DummyCreds()

def test_init_valid_oauth_path(temp_oauth_config):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    assert isinstance(cm.client_config_json, dict)
    assert cm.scopes == SCOPES


def test_init_invalid_oauth_path():
    with pytest.raises(FileNotFoundError):
        CredentialManager(SCOPES, "/nonexistent/path.json")


@patch("googleauth.service_account.Credentials.from_service_account_file")
def test_load_from_service_account_file_valid(mock_from_file, temp_oauth_config):
    mock_from_file.return_value = dummy_creds
    cm = CredentialManager(SCOPES, temp_oauth_config)

    with tempfile.NamedTemporaryFile("w+", delete=False) as sa_file:
        creds = cm.load_from_service_account_file(sa_file.name)
        assert creds is dummy_creds
        assert cm.credentials is dummy_creds


def test_load_from_service_account_file_invalid_path(temp_oauth_config):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    creds = cm.load_from_service_account_file("/nonexistent/key.json")
    assert creds is None


def test_load_from_token_file_valid(temp_oauth_config, dummy_creds):
    cm = CredentialManager(SCOPES, temp_oauth_config)

    with tempfile.NamedTemporaryFile("wb+", delete=False) as tf:
        pickle.dump(dummy_creds, tf)
        tf.flush()

        loaded_creds = cm.load_from_token_file(tf.name)
        assert isinstance(loaded_creds, DummyCreds)
        assert loaded_creds.valid


@patch("googleauth.pickle.load", side_effect=Exception("broken pickle"))
@patch("googleauth.os.path.exists", return_value=True)
def test_load_from_token_file_broken_pickle(mock_exists, mock_pickle, temp_oauth_config):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    creds = cm.load_from_token_file("mock_token.pkl")
    assert creds is None


@patch("googleauth.pickle.dump")
def test_save_to_token_file(mock_pickle_dump, temp_oauth_config):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    with tempfile.NamedTemporaryFile("wb", delete=False) as tk:
        cm._save_to_token_file(tk.name, dummy_creds)
        mock_pickle_dump.assert_called_once()


def test_save_to_session_data(temp_oauth_config):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    session = {}
    cm._save_to_session_data(session, dummy_creds)
    assert "user_creds" in session
    assert isinstance(session["user_creds"], str)


def test_load_from_session_data_valid(temp_oauth_config, dummy_creds):
    cm = CredentialManager(SCOPES, temp_oauth_config)
    encoded = base64.b64encode(pickle.dumps(dummy_creds)).decode()
    
    loaded = cm.load_from_session_data({"user_creds": encoded})
    assert isinstance(loaded, DummyCreds)
    assert loaded.valid


@patch("googleauth.InstalledAppFlow.from_client_config")
def test_load_from_token_file_runs_flow(mock_from_config, temp_oauth_config):
    mock_flow = MagicMock()
    mock_creds = MagicMock()
    mock_creds.valid = True
    mock_flow.run_local_server.return_value = mock_creds
    mock_from_config.return_value = mock_flow

    cm = CredentialManager(SCOPES, temp_oauth_config)
    creds = cm.load_from_token_file("/nonexistent.pkl", secure=True)
    assert creds.valid
    mock_flow.run_local_server.assert_called_once()


@patch("googleauth.Flow.from_client_config")
def test_create_web_flow(mock_flow, temp_oauth_config):
    flow_instance = MagicMock()
    mock_flow.return_value = flow_instance

    cm = CredentialManager(SCOPES, temp_oauth_config)
    redirect = "https://example.com/oauth"
    flow = cm.create_web_flow(redirect_url=redirect, state="xyz")
    assert flow.redirect_uri == redirect
    mock_flow.assert_called_once()
