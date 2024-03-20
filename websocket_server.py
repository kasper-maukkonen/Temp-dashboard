# websocket_server.py
import asyncio
import websocket
import json

async def handle_connection(websocket, path):
    try:
        while True:
            message = await websocket.recv()
            # Process the received message and send responses as needed
            await websocket.send(response)
    except websocket.exceptions.ConnectionClosedError:
        # Handle closed connections gracefully
        pass

if __name__ == "__main__":
    server = websocket.serve(handle_connection, "localhost", 3000)
    asyncio.get_event_loop().run_until_complete(server)
    asyncio.get_event_loop().run_forever()
