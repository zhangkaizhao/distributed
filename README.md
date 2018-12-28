# distributed demo or in action

> Warning: This a work in process project. Anything may be changed often.

Task: calculating sha1 hex digest of files in a directory across (virtual) machines.

## Why

Keep it as simple as possible!

Complex libraries are for complex systems.

Here, just distributed tasks over HTTP.

## How

recipes:

- simple push
- pull
- push

can be considered as TODO:

- files in batch
- (recipe: multiprocessing manager without HTTP)

### simple push

```text
              ----------
              | master |
              ----------
              /    |   \
             /     |    \
            /      |     \
           /       |      \ push and wait for result
          /        |       \
         /         |        \
        \/        \ /       \/
  ----------  ----------  ----------
  | worker |  | worker |  | worker |
  | server |  | server |  | server |
  ----------  ----------  ----------
```

Just a task once in both master and worker.

master: just pushes jobs to workers via HTTP (just one file per worker at the same time)

worker: serves as HTTP server (receives task, handle task and then respond back result to master)

- `/new_file`

### pull

```text
          -----------------
          | master server |
          -----------------
             /\  / \   /\
             /    |     \
            /     |      \ pull, report result
           /      |       \
          /       |        \
  ----------  ----------  ----------
  | worker |  | worker |  | worker |
  ----------  ----------  ----------
```

Wait tasks all the time. For simplicity just one task in this demo.

master: serves as HTTP server

- `/worker_register`
- `/get_file` (populate file from files queue, and then send back to worker)
- `/update_file_hash`
- `/home_status`

worker: synchronous pull tasks and report back result to master until no tasks (in multiprocessing mode)

### push

(TODO not implemented yet)

Wait tasks all the time. For simplicity just one task in this demo.

master: serves as HTTP server (just pushes jobs to workers via HTTP, runs a background task to collect works' status)

- `/update_file_hash`
- `/home_status`

worker: serves as HTTP server (receives task, handle task and then respond back result to master)

- `/new_file`
- `/status` (report processes total/idle)

## Local development

First install Python and poetry:

- [Python](https://www.python.org/)
- [poetry](https://poetry.eustace.io/)

Then install all dependencies and dev-dependencies:

```sh
poetry install
```

And edit `distributed/config.py` to modify settings.

Here is an example to use poetry and Supervisor for local development.

For simple push recipe, start worker servers firstly:

```sh
supervisord -c development/conf_supervisor_simple.conf
```

and then run the master:

```sh
poetry run python -m distributed.simple.master
```

For pull recipe, start master server firstly:

```sh
supervisord -c development/conf_supervisor_pull.conf
```

and then run some workers:

```sh
poetry run python -m distributed.pull.worker --name worker-1
```

```sh
poetry run python -m distributed.pull.worker --name worker-2
```

For futher usage of Supervisor, please see [Supervisor's documentation](http://supervisord.org/).

## Testing

After project dependencies and dev-dependencies installed, running:

```sh
make test
```

or directly:

```sh
poetry run pytest tests
```

For futher usage of pytest, please see [pytest's documentation](https://docs.pytest.org/).

## Questions and Answers

Q: Serious? Why not [Celery](http://www.celeryproject.org/) or [huey](https://github.com/coleifer/huey), etc?

A: I know these task queues. In Chinese idioms: 杀鸡焉用牛刀？

Q: Why HTTP?

A: HTTP is the most popular and stable internet protocol. Most of the needed features are mature.

Q: Why Python?

A: This is nearly a proof of concept. I think Python is quick and fit for such use cases. Other languages should be OK too.

Q: Why latest Python 3?

A: See [@dabeaz](https://github.com/dabeaz)'s words: [![Compatibility](assets/github-dabeaz-me-tal-compatibility.png)](https://github.com/dabeaz/me-al#compatibility)

Q: Why aiohttp?

A: Well, asynchronous I/O is the future and aiohttp is the best asynchronous I/O library with both HTTP server and client support at the moment. But it should not be too hard for all using standard libraries or other third party libraries. For others please see answer of question "Why latest Python 3" too.

Q: Why not [trio](https://github.com/python-trio/trio)?

A: [trio](https://github.com/python-trio/trio) is better but its community is smaller so far. There is [trio-asyncio](https://github.com/python-trio/trio-asyncio) but more complex. It may be worth a try.

Q: Why poetry for local project management, instead of pipenv or flit, etc?

A: Believe me. Poetry is better for local project management. But others should be OK too.

Q: Why Supervisor for local subprocess management?

A: Supervisor is written in Python and porting to Python 3 in the process.

Q: Why not be a library?

A: May be in the future.

Q: Are there any other documentation?

A: No. Since the source code is short, I don't think any more documentation is needed.

Q: Why there are `small`, `medium` and `large` directories under `tests` directory?

A: See [Test Sizes](https://testing.googleblog.com/2010/12/test-sizes.html) and [How Google Tests Software - Part Five](https://testing.googleblog.com/2011/03/how-google-tests-software-part-five.html).

Q: The name "distributed" is used and may cause conflict.

A: Yes. I know [distributed](https://pypi.org/project/distributed/). But I am not using it now.
