import hashlib

from aiohttp import web


async def new_file(request: web.Request) -> web.Response:
    """Starting calculating file hash"""
    reader = await request.multipart()
    field = await reader.next()
    assert field.name == "file"
    h = hashlib.new("sha1")
    while True:
        chunk = await field.read_chunk()
        if not chunk:
            break
        h.update(chunk)
    result = h.hexdigest()
    return web.Response(text=result)


def init() -> web.Application:
    app = web.Application()
    app.add_routes([web.post("/new_file", new_file)])
    return app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="distributed worker server")
    parser.add_argument("--path")
    parser.add_argument("--port")
    args = parser.parse_args()

    app = init()

    web.run_app(app, path=args.path, port=args.port)
