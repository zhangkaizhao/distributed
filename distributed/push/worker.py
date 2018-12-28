from aiohttp import web

from distributed.workers import collect_worker_state


async def new_file(request):
    """Starting calculating file hash"""
    pass


async def status(request):
    """Reporting worker state"""
    state = collect_worker_state()
    return web.Response(json=state)


if __name__ == "__main__":
    app = web.Application()
    app.add_routes(
        [web.post("/new_file", new_file), web.get("/status", status)]
    )
    # app["files_queue"] = {}
    web.run_app(app)
