from fastapi import APIRouter
from app.routers import auth_routes,upload_routes,display_routes

router = APIRouter()

router.include_router(auth_routes.router,tags=["Authentication"])
router.include_router(upload_routes.router,tags=["Excel upload"])
router.include_router(display_routes.router,tags=["Display"])