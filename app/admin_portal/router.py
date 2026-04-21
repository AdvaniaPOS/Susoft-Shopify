"""
Admin Portal Router
====================
Serves the admin web interface using Jinja2 templates.

All HTML pages require a valid portal session. Login is performed at
/portal/login with credentials from settings (ADMIN_USERNAME / ADMIN_PASSWORD).
"""

import hmac
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.core.config import settings


# Set up templates directory
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

router = APIRouter(prefix="/portal", tags=["portal"])


# ===================
# Session helpers
# ===================


SESSION_USER_KEY = "portal_user"


def is_portal_authenticated(request: Request) -> bool:
    """Return True if the request carries a valid portal session."""
    return bool(request.session.get(SESSION_USER_KEY))


def require_portal_session(request: Request):
    """Dependency that redirects unauthenticated requests to /portal/login."""
    if not is_portal_authenticated(request):
        next_url = request.url.path
        if request.url.query:
            next_url = f"{next_url}?{request.url.query}"
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": f"/portal/login?next={next_url}"},
        )


def _verify_credentials(username: str, password: str) -> bool:
    expected_user = settings.admin_username or ""
    expected_pass = (
        settings.admin_password.get_secret_value() if settings.admin_password else ""
    )
    if not expected_user or not expected_pass:
        return False
    user_ok = hmac.compare_digest(
        username.encode("utf-8"), expected_user.encode("utf-8")
    )
    pass_ok = hmac.compare_digest(
        password.encode("utf-8"), expected_pass.encode("utf-8")
    )
    return user_ok and pass_ok


# ===================
# Login / logout
# ===================


@router.get("/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    next: Optional[str] = "/portal/",
    error: Optional[str] = None,
):
    """Render the portal login page."""
    if is_portal_authenticated(request):
        return RedirectResponse(
            url=next or "/portal/", status_code=status.HTTP_303_SEE_OTHER
        )
    return templates.TemplateResponse(
        request,
        "login.html",
        {"next": next or "/portal/", "error": error},
    )


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/portal/"),
):
    """Handle login form submission."""
    if not _verify_credentials(username, password):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"next": next, "error": "Feil brukernavn eller passord."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    request.session[SESSION_USER_KEY] = username
    if not next.startswith("/"):
        next = "/portal/"
    return RedirectResponse(url=next, status_code=status.HTTP_303_SEE_OTHER)


@router.post("/logout")
async def logout(request: Request):
    """Clear the portal session."""
    request.session.clear()
    return RedirectResponse(
        url="/portal/login", status_code=status.HTTP_303_SEE_OTHER
    )


# ===================
# HTML pages (require session)
# ===================


@router.get(
    "/", response_class=HTMLResponse, dependencies=[Depends(require_portal_session)]
)
async def dashboard(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse(request, "dashboard.html")


@router.get(
    "/tenants",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def tenants_page(request: Request):
    """Tenants management page."""
    return templates.TemplateResponse(request, "tenants.html")


@router.get(
    "/tenants/{tenant_id}",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def tenant_detail_page(request: Request, tenant_id: str):
    """Tenant detail page."""
    return templates.TemplateResponse(
        request, "tenant_detail.html", {"tenant_id": tenant_id}
    )


@router.get(
    "/logs",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def logs_page(request: Request):
    """Sync logs page."""
    return templates.TemplateResponse(request, "logs.html")


@router.get(
    "/errors",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def errors_page(request: Request):
    """Dead letter queue / errors page."""
    return templates.TemplateResponse(request, "errors.html")


@router.get(
    "/sync",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def sync_page(request: Request):
    """Manual sync page."""
    return templates.TemplateResponse(request, "sync.html")


@router.get(
    "/system-logs",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def system_logs_page(request: Request):
    """System logs page - real-time application logs."""
    return templates.TemplateResponse(request, "system_logs.html")


@router.get(
    "/settings",
    response_class=HTMLResponse,
    dependencies=[Depends(require_portal_session)],
)
async def settings_page(request: Request):
    """Settings page."""
    return templates.TemplateResponse(request, "settings.html")
