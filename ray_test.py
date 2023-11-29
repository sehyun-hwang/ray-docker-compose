import socket
import time
from collections import Counter
import asyncio
from subprocess import check_call

import ray
from ray import serve
from ray.serve.config import HTTPOptions

import uvicorn
from fastapi import FastAPI, Request
from ray import serve
from ray.serve.handle import DeploymentHandle

app = FastAPI()


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
        time.sleep(1)
        print(socket.gethostbyname(socket.gethostname()), "Initialized SayHello")
        self.data = None

    async def hello(self, name: str) -> str:
        actor_id = ray.get_runtime_context().get_actor_id()
        task_id = ray.get_runtime_context().get_task_id()
        print(f"Hello {name=:50} from '{actor_id=}' | '{task_id=}'")
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

@app.get("/stateful")
async def root():
    # https://docs.ray.io/en/latest/serve/api/doc/ray.serve.handle.DeploymentHandle.html
    ray_response = handle.remote("world")
    ray_response2 = ray.get(ray_response)
    response = ray.get(ray_response2)
    print(response)
    assert response == "Hello world!"
    return "please"


@app.get("/hello")
async def say_hello(name: str, request: Request):
    print(request.__dict__)
    print(request)
    print(f"name = {name}")
    say_hello = SayHello.options(lifetime="detached").remote()
    print(type(say_hello))
    print(dir(say_hello))
    # object_id = say_hello.hello.remote(name)
    # print(f"{object_id=}")
    # message = ray.get(object_id)
    message = await say_hello.hello.remote(name)
    return message



downstream_app = Downstream.options(
    num_replicas=14,
    ray_actor_options={"num_cpus": 1, "memory": 600 * (1 << 20)}
).bind()
ray_app = Ingress.bind(downstream_app)

if __name__ == "__main__":
    check_call(["ray", "start", "--head", "--dashboard-host", "0.0.0.0"])
    print("main")
    ray.init(address="auto", namespace="count_car")
    
    # http_options = HTTPOptions(host="0.0.0.0", port=8001, location="HeadOnly")
    # serve.start(detached=True, http_options=http_options)

    # ray_app = Ingress.bind(Downstream.bind())
    # handle: DeploymentHandle = serve.run(ray_app)
    # say_hello = SayHello.remote()
    print(
        f"""This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()} CPU resources in total
    """,
    )
    uvicorn.run(app, host="0.0.0.0", port=8008)

    """
Woker 노드를 붙일 머신들에서 모두 조인한 후
Head 컨테이너에서 아래 명령어로 deployment 수행.

serve start --http-host "0.0.0.0" --http-port 8001 --proxy-location "HeadOnly"
serve run ray_test:ray_app 
    """