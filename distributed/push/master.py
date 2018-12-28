from aiohttp import web

# # home -> state
# homes = {}
#
#
# async def new_home(request):
#     """Starting new home distributed task"""
#     # 0. Check home state
#     # 1. Read home dir
#     # 2. Send file task to workers
#     pass


async def update_file_hash(request):
    """For worker to report file hash"""
    pass


async def home_status(request):
    """Reporting home files status"""
    pass


async def monitor_workers_status(app):
    """Checking workers state in background"""
    pass


async def start_background_tasks(app):
    app["workers_status_monitor"] = app.loop.create_task(
        monitor_workers_status(app)
    )


async def cleanup_background_tasks(app):
    app["workers_status_monitor"].cancel()
    await app["workers_status_monitor"]


def init():
    app = web.Application()
    app.add_routes(
        [
            # web.post("/new_home", new_home),
            web.post("/update_file_hash", update_file_hash),
            web.get("/home_status", home_status),
        ]
    )
    # https://docs.aiohttp.org/en/stable/web_advanced.html#background-tasks
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app


if __name__ == "__main__":
    app = init()
    web.run_app(app)
