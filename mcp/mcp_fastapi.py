from fastapi import FastAPI, Request
from mcp.server import Server
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount


# Let's monkeypatch Server so it has the .tool() decorator expected by the existing server scripts
def patched_tool(self, name=None, description=None):
    if callable(name):
        fn = name
        if not hasattr(self, "_mcp_tools"):
            self._mcp_tools = []
        self._mcp_tools.append({
            "name": None,
            "description": None,
            "fn": fn
        })
        return fn

    def decorator(fn):
        if not hasattr(self, "_mcp_tools"):
            self._mcp_tools = []
        self._mcp_tools.append({
            "name": name,
            "description": description,
            "fn": fn
        })
        return fn
    return decorator

# Apply the patch to the low-level Server class
Server.tool = patched_tool

def create_mcp_server(server: Server) -> FastAPI:
    # 1. Create a FastMCP instance with the server name
    mcp_server = FastMCP(server.name)

    # 2. Add all collected tools to FastMCP for automatic JSON Schema generation and type validation
    for tool in getattr(server, "_mcp_tools", []):
        mcp_server.add_tool(
            tool["fn"],
            name=tool["name"],
            description=tool["description"]
        )

    # 3. Create our custom FastAPI app where GET / handles SSE directly,
    # and POST /messages handles post messages.
    mcp_app = FastAPI(title=f"{server.name}-transport")
    sse = SseServerTransport("messages")

    @mcp_app.get("/")
    async def handle_sse(request: Request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as (read_stream, write_stream):
            # Run the underlying low-level Server from FastMCP
            await mcp_server._mcp_server.run(
                read_stream,
                write_stream,
                mcp_server._mcp_server.create_initialization_options()
            )

    mcp_app.router.routes.append(Mount("/messages", app=sse.handle_post_message))
    return mcp_app
