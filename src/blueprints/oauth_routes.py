from flask import Blueprint, session, request, redirect, url_for, Response, current_app
import logging
from constants.project import REDIRECT_URI

REDIRECT_CALLBACK="/auth/oauth_callback"
FULL_REDIRECT_URL = REDIRECT_URI + REDIRECT_CALLBACK


log = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

@auth_bp.route("/")
def start_oauth_process():
    # Ensure REDIRECT_URI is base URL, and build full redirect here
    flow = current_app.web_creds_manager.create_web_flow(FULL_REDIRECT_URL)
    auth_url, state = flow.authorization_url(prompt="consent")
    session['state'] = state
    return redirect(auth_url)

@auth_bp.route("/oauth_callback")
def oauth_callback():
    if request.args.get('state') != session.get('state'):
        return Response("State Mismatch", 400)
    full_redirect_url = REDIRECT_URI + "/auth/oauth_callback"
    flow = current_app.web_creds_manager.create_web_flow(
        redirect_url=full_redirect_url,
        state=session.get('state'),
    )
    flow.fetch_token(authorization_response=request.url)
    current_app.web_creds_manager._save_to_session_data(session, flow.credentials)
    next_url = session.pop('next_url', None)
    if next_url:
        return redirect(next_url)
    else:
        return redirect(url_for("process_failed_invoices"))

