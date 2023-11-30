import socket
import time
from collections import Counter
from subprocess import check_call
import asyncio
import random

import ray
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.util.actor_pool import ActorPool

app = FastAPI()
ACTOR_NAMES = [f"actor-{i}" for i in range(5)]


@ray.remote
def f():
    time.sleep(0.001)
    print(
        """This cluster consists of
        {} nodes in total
        {} CPU resources in total
    """.format(
            len(ray.nodes()), ray.cluster_resources()["CPU"],
        ),
    )
    # Return IP address.
    return socket.gethostbyname(socket.gethostname())


@ray.remote(lifetime="detached")
class SayHello:
    def __init__(self):
        time.sleep(1)
        print(socket.gethostbyname(socket.gethostname()), "Initialized SayHello")
        self.data = None

    async def hello(self, name: str) -> str:
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

@app.get("/hello")
async def say_hello(name: str, request: Request):
    print(request.__dict__)
    print(f"{name=}")
    say_hello = SayHello.options(name=random.choice(ACTOR_NAMES), get_if_exists=True).remote()
    message = await say_hello.hello.remote(name)
    return message

if __name__ == "__main__":
    check_call(["ray", "start", "--head", "--num-cpus", "0", "--dashboard-host", "0.0.0.0"])
    print("main")
    ray.init(address="auto")
    print(
        f"""This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()} CPU resources in total
    """,
    )
    uvicorn.run(app, host="0.0.0.0", port=8001)
