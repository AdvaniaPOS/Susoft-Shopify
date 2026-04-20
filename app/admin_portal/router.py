"""
Admin Portal Router
====================
Serves the admin web interface using Jinja2 templates.
"""

from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Set up templates directory
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

router = APIRouter(prefix="/portal", tags=["portal"])


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/tenants", response_class=HTMLResponse)
async def tenants_page(request: Request):
    """Tenants management page."""
    return templates.TemplateResponse("tenants.html", {"request": request})


@router.get("/tenants/{tenant_id}", response_class=HTMLResponse)
async def tenant_detail_page(request: Request, tenant_id: str):
    """Tenant detail page."""
    return templates.TemplateResponse("tenant_detail.html", {
        "request": request,
        "tenant_id": tenant_id
    })


@router.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    """Sync logs page."""
    return templates.TemplateResponse("logs.html", {"request": request})


@router.get("/errors", response_class=HTMLResponse)
async def errors_page(request: Request):
    """Dead letter queue / errors page."""
    return templates.TemplateResponse("errors.html", {"request": request})


@router.get("/sync", response_class=HTMLResponse)
async def sync_page(request: Request):
    """Manual sync page."""
    return templates.TemplateResponse("sync.html", {"request": request})


@router.get("/system-logs", response_class=HTMLResponse)
async def system_logs_page(request: Request):
    """System logs page - real-time application logs."""
    return templates.TemplateResponse("system_logs.html", {"request": request})


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Settings page."""
    return templates.TemplateResponse("settings.html", {"request": request})
