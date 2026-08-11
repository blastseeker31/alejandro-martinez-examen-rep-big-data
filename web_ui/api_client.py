"""Cliente HTTP centralizado para la interfaz Streamlit."""

from __future__ import annotations

import os
from typing import Any

import requests


class ApiClient:
    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or os.getenv("API_BASE_URL", "http://localhost:8000")).rstrip("/")

    def get(self, path: str, **params: Any) -> Any:
        response = requests.get(f"{self.base_url}{path}", params=params or None, timeout=15)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: dict[str, Any]) -> Any:
        response = requests.post(f"{self.base_url}{path}", json=payload, timeout=300)
        response.raise_for_status()
        return response.json()
