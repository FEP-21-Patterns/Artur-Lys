from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from controller.app_controller import AppController
import uvicorn

# Create FastAPI app
app = FastAPI(title="SmartApp IoT System")

#  Load HTML templates (for dashboard)
templates = Jinja2Templates(directory="old/web/templates")

#  Mount static files (CSS, images, etc.)
app.mount("/static", StaticFiles(directory="old/web/static"), name="static")

#  Initialize our main application controller
controller = AppController()

#  Main dashboard page
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Show the main dashboard with all device statuses.
    """
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status}
    )

#  Toggle Smart Speaker power
@app.post("/toggle_speaker")
async def toggle_speaker(request: Request):
    speaker_status = controller.toggle_speaker()
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status, "updated_device": speaker_status}
    )

#  Toggle Smart Light power
@app.post("/toggle_light")
async def toggle_light(request: Request):
    light_status = controller.toggle_light()
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status, "updated_device": light_status}
    )

#  Set Smart Speaker volume
@app.post("/set_volume")
async def set_volume(request: Request, volume: int = Form(...)):
    controller.set_speaker_volume(volume)
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status}
    )

#  Set Smart Light brightness
@app.post("/set_brightness")
async def set_brightness(request: Request, brightness: int = Form(...)):
    controller.set_light_brightness(brightness)
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status}
    )

#  Toggle Smart Curtains power
@app.post("/toggle_curtains")
async def toggle_curtains(request: Request):
    curtains_status = controller.toggle_curtains()
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status, "updated_device": curtains_status}
    )

#  Set Smart Curtains position
@app.post("/set_position")
async def set_position(request: Request, position: int = Form(...)):
    controller.set_curtain_position(position)
    status = controller.get_all_status()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "devices": status}
    )

#  Entry point
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)