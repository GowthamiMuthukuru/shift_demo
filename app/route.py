from fastapi import APIRouter
from app.routers import (auth_routes, client_comparision_routes,upload_routes,display_routes,
                         summary_routes,get_excel_routes,search_routes,
                         get_interval_summary_routes,dashboard_routes,
                         search_month_routes,client_summary_routes,department_summary_routes,client_summary_download_routes)


router = APIRouter()

router.include_router(auth_routes.router,tags=["Authentication"])
router.include_router(upload_routes.router,tags=["Excel upload"])
router.include_router(display_routes.router,tags=["Display"])
router.include_router(summary_routes.router, tags=["Summary"])
router.include_router(get_excel_routes.router, tags=["Excel Data"])
router.include_router(search_routes.router, tags=["Search Details"])
router.include_router(search_month_routes.router,tags=["Payroll Monthly Search"])
router.include_router(get_interval_summary_routes.router, tags=["Range Summary"])
router.include_router(dashboard_routes.router, tags=["Dashboard"])
router.include_router(client_comparision_routes.router, tags=["Client Comparision"])
router.include_router(client_summary_routes.router, tags=["Client Summary"])
router.include_router(department_summary_routes.router, tags=["Department Summary"])
router.include_router(client_summary_download_routes.router,tags=["Client summary download"])