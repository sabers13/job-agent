# app/fetching/http_client.py

import httpx


async def fetch(url: str, *, timeout: float = 20.0, headers: dict | None = None) -> str:
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text
