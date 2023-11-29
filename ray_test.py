import socket
import time
from collections import Counter
from subprocess import check_call

import ray
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.util.actor_pool import ActorPool

app = FastAPI()
pool = ActorPool([])
DESIRED_ACTOR_COUNT = 2

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


@ray.remote
class SayHello:
    def __init__(self):
        print(socket.gethostbyname(socket.gethostname()), "Initialized SayHello")
        self.data = None

    async def hello(self, name: str) -> str:
        time.sleep(1)
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

    def handle_pool_submit(actor, arg):
        print("handle_pool_submit", actor, arg)
        return actor.hello.remote(arg)
    pool.submit(handle_pool_submit, name)
    print("submit", name)
    message = pool.get_next()
    print(name, message)
    return message

def update_actor_pool() -> int:
    actor_count = len(pool._idle_actors) + len(pool._future_to_actor.values())
    # TODO: Instead of DESIRED_ACTOR_COUNT, use https://docs.ray.io/en/latest/ray-core/api/doc/ray.available_resources.html
    if actor_count > DESIRED_ACTOR_COUNT:
        print("Popping actor")
        pool.pop()
        actor_count -= 1
    elif actor_count < DESIRED_ACTOR_COUNT:
        print("Pushing actor")
        remote = SayHello.remote()
        print(ray.get(remote.hello.remote("healthcheck")))
        pool.push(remote)
        actor_count += 1

    print(actor_count)
    return actor_count

@app.get("/healthcheck")
def run_healthcheck():
    actors_count = update_actor_pool()
    if not actors_count:
        raise HTTPException(status_code=503, detail="No actor available")
    return {
        "actors_count": actors_count
    }

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
