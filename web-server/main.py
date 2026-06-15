from fastapi import FastAPI, WebSocket
from pydantic import BaseModel

from schema import GDELT_SCHEMA

app = FastAPI(
    title="NL2Pipeline API",
    description="Backend API for GDELT schema metadata and chat WebSocket",
    version="1.0.0",
)


class ChatMessage(BaseModel):
    text: str


class ChatResponse(BaseModel):
    text: str


@app.get("/api", response_model=dict, tags=["metadata"])
def get_schema():
    """Return GDELT dataset schema for the frontend."""
    return GDELT_SCHEMA


@app.websocket("/ws/chat")
async def chat(websocket: WebSocket):
    """Simple chat WebSocket. Receives {"text": "..."}, returns {"text": "function"}."""
    await websocket.accept()
    while True:
        data = await websocket.receive_json()
        message = ChatMessage(**data)
        # placeholder: real processing will go here
        _ = message.text
        await websocket.send_json(ChatResponse(text="function").model_dump())
