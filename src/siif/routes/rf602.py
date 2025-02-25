from fastapi import APIRouter

from ..services import Rf602ServiceDependency

rf602_router = APIRouter(prefix="/rf602", tags=["SIIF - rf602"])


@rf602_router.post("/siif_download/")
async def siif_download(
    ejercicio: str,
    service: Rf602ServiceDependency,
):
    return await service.download_report(ejercicio=ejercicio)


# @rf602_router.post("/start_playwright")
# def start_playwright():
#     """Inicia Playwright en modo síncrono (evita problemas en Windows)"""
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=True)
#         page = browser.new_page()
#         page.goto("https://example.com")
#         title = page.title()
#         browser.close()

#     return {"message": "Playwright ejecutado", "title": title}
