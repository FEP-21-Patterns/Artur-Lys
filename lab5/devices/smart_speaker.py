from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import uvicorn
from devices.base_device import Device

class SpeakerState(BaseModel):
    is_on: bool = False
    volume: int = 50
    playing: bool = False
    current_track: str = ""

class SmartSpeakerDevice(Device):
    def __init__(self, device_id: str, host="127.0.0.1", port=8001):
        super().__init__(device_id, host, port)
        self.state = SpeakerState()
        self.app = FastAPI(title=f"Smart Speaker {device_id}")
        self._setup_routes()

    def _setup_routes(self):
        app = self.app

        @app.get("/status")
        async def get_status():
            return self.get_status()

        @app.post("/power/{state}")
        async def set_power(state: str):
            if not self.perform_action("power", state=state):
                raise HTTPException(status_code=400, detail="Invalid power state")
            return {"status": "success"}

        @app.post("/volume/{level}")
        async def set_volume(level: int):
            if not self.perform_action("volume", level=level):
                raise HTTPException(status_code=400, detail="Invalid volume level")
            return {"status": "success"}

    def get_status(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "type": "smart_speaker",
            "is_on": self.state.is_on,
            "volume": self.state.volume,
            "playing": self.state.playing,
            "current_track": self.state.current_track,
            "connection": f"{self.host}:{self.port}"
        }

    def perform_action(self, action: str, **kwargs) -> bool:
        if action == "power":
            state = kwargs.get("state")
            if state == "on":
                self.state.is_on = True
                return True
            elif state == "off":
                self.state.is_on = False
                self.state.playing = False
                return True
        elif action == "volume":
            level = kwargs.get("level")
            if 0 <= level <= 100:
                self.state.volume = level
                return True
        return False

    def run_server(self):
        uvicorn.run(self.app, host=self.host, port=self.port, log_level="info")

if __name__ == "__main__":
    speaker = SmartSpeakerDevice("speaker_001")
    speaker.run_server()