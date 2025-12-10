from typing import Dict, List, Any
import requests
from devices.base_device import Device


class IOTFacade:
    """Facade for handling HTTP communication with IoT devices"""

    def __init__(self):
        self.devices: Dict[str, tuple] = {}  # device_id: (host, port)

    def register_device(self, device: Device) -> str:
        """Register a device with the system"""
        self.devices[device.device_id] = (device.host, device.port)
        return f"Device {device.device_id} registered successfully"

    def get_device_status(self, device_id: str) -> Dict[str, Any]:
        """Get status of a specific device"""
        if device_id not in self.devices:
            return None
            
        host, port = self.devices[device_id]
        url = f"http://{host}:{port}/status"
        
        try:
            response = requests.get(url, timeout=2)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return None

    def perform_device_action(self, device_id: str, action: str, **kwargs) -> bool:
        """Perform an action on a specific device"""
        if device_id not in self.devices:
            return False
            
        host, port = self.devices[device_id]
        params = [str(v) for v in kwargs.values()]
        url_path = f"/{action}"
        if params:
            url_path += "/" + "/".join(params)
        full_url = f"http://{host}:{port}{url_path}"
        
        try:
            response = requests.post(full_url, timeout=2)
            response.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def get_all_status(self) -> List[Dict[str, Any]]:
        """Get status of all registered devices"""
        statuses = []
        for device_id in self.devices:
            status = self.get_device_status(device_id)
            if status:
                statuses.append(status)
        return statuses