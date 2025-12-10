
from abc import ABC, abstractmethod
from fastapi import FastAPI
from typing import Dict, Any


class Device(ABC):
    """Base class for all IoT devices"""
    
    def __init__(self, device_id: str, host: str, port: int):
        self.device_id = device_id
        self.host = host
        self.port = port
        self.app = FastAPI(title=f"Smart {device_id.replace('_', ' ').title()}")

    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """Return current device status"""
        pass

    @abstractmethod
    def perform_action(self, action: str, **kwargs) -> bool:
        """Perform an action on the device"""
        pass

    def run_server(self):
        """Run the FastAPI server for this device"""
        import uvicorn
        uvicorn.run(self.app, host=self.host, port=self.port, log_level="info")


class LoggingDeviceDecorator(Device):
    """Decorator to add logging functionality to devices"""
    
    def __init__(self, device: Device):
        self._device = device
        super().__init__(device.device_id, device.host, device.port)
        self.app = device.app  # Share the same FastAPI app

    def get_status(self) -> Dict[str, Any]:
        print(f"Logging: Getting status for {self.device_id}")
        return self._device.get_status()

    def perform_action(self, action: str, **kwargs) -> bool:
        print(f"Logging: Performing {action} on {self.device_id} with params {kwargs}")
        return self._device.perform_action(action, **kwargs)

    def run_server(self):
        self._device.run_server()
