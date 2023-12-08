import os
import socket
import time
from collections import Counter
import asyncio
import random

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle
import uvicorn
from fastapi import FastAPI, Request

app = FastAPI()

ACTOR_NUMS = os.environ.get("ACTOR_NUMS", 18)
ACTOR_NAMES = [f"actor-{i:03}" for i in range(ACTOR_NUMS)]

ACTOR_NUM_CPU = 0.5


@ray.remote
def f():
    time.sleep(0.001)
    print(
        """This cluster consists of
        {} nodes in total
        {} CPU resources in total
    """.format(
            len(ray.nodes()),
            ray.cluster_resources()["CPU"],
        ),
    )
    # Return IP address.
    return socket.gethostbyname(socket.gethostname())


@ray.remote
class SayHello:
    def __init__(self):
        time.sleep(1)
        print(socket.gethostbyname(socket.gethostname()), "Initialized SayHello")
        self.data = None

    async def hello(self, name: str) -> str:
        actor_id = ray.get_runtime_context().get_actor_id()
        task_id = ray.get_runtime_context().get_task_id()
        print(f"{name=:13} from '{actor_id=}' | '{task_id=}'")
        await asyncio.sleep(1)
        return f"Hello {name}!"


@app.get("/")
async def root():
    object_ids = [f.remote() for _ in range(2)]
    ip_addresses = ray.get(object_ids)

    print("Tasks executed")
    for ip_address, num_tasks in Counter(ip_addresses).items():
        print(f"    {num_tasks} tasks on {ip_address}")
    return {"message": "Hello World"}


@app.get("/actors-init")
async def actors_init(request: Request):
    actors = [
        SayHello.options(
            name=name,
            num_cpus=ACTOR_NUM_CPU,
            lifetime="detached",
            namespace="say_hello",
            get_if_exists=True,
        ).remote()
        for name in ACTOR_NAMES
    ]
    print(actors)

    return str(actors)


@app.get("/actors-kill")
async def actors_kill(request: Request):
    for name in ACTOR_NAMES:
        actor_handle = ray.get_actor(
            name=name,
            namespace="say_hello",
        )
        print(actor_handle)
        ray.kill(actor_handle)

    return "OK"


@app.get("/hello")
async def say_hello(name: str, request: Request):
    print(request.__dict__)
    print(f"{name=}")
    say_hello = SayHello.options(
        name=random.choice(ACTOR_NAMES),
        num_cpus=ACTOR_NUM_CPU,
        namespace="say_hello",
        lifetime="detached",
        get_if_exists=True,
    ).remote()
    message = await say_hello.hello.remote(name)
    return message


# ////////////////////////////////////
@serve.deployment
class Downstream:
    def __init__(self):
        time.sleep(1)
        print(socket.gethostbyname(socket.gethostname()), "Initialized Downstream")
        self.data = None

    async def hello(self, name: str) -> str:
        actor_id = ray.get_runtime_context().get_actor_id()
        task_id = ray.get_runtime_context().get_task_id()
        print(f"Hello {name=:50} from '{actor_id=}' | '{task_id=}'")
        await asyncio.sleep(1)

        return f"Hello {name}!"


@serve.deployment
@serve.ingress(app)
class Ingress:
    _handle: Downstream

    def __init__(self, handle: DeploymentHandle):
        self._downstream_handle = handle

    @app.get("/hello2")
    async def say_hello2(self, name: str, request: Request):
        print(request.__dict__)
        print(request)
        print(f"name = {name}")
        response = await self._downstream_handle.hello.remote(name)
        return response


downstream_app = Downstream.options(
    num_replicas=ACTOR_NUMS, ray_actor_options={"num_cpus": ACTOR_NUM_CPU}
).bind()
ray_app = Ingress.bind(downstream_app)


if __name__ == "__main__":
    print("main")
    ray.init(address="auto")
    print(
        f"""This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()} CPU resources in total
    """,
    )
    uvicorn.run(app, host="0.0.0.0", port=8008)
