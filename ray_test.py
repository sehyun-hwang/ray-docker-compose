import socket
import time
from collections import Counter
from subprocess import check_call

import ray
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
    print(request)
    print(f"name = {name}")
    print(type(say_hello))
    print(dir(say_hello))
    object_id = say_hello.hello.remote(name)
    print(f"{object_id=}")
    message = ray.get(object_id)
    return message

@serve.deployment
class Downstream:
    def say_hi(self, message: str):
        return f"Hello {message}!"

@serve.deployment
class Ingress:
    _handle: Downstream
    def __init__(self, handle: DeploymentHandle):
        self._downstream_handle = handle

    async def __call__(self, name: str) -> str:
        response = self._downstream_handle.say_hi.remote(name)
        return await response

@app.get("/stateful")
async def root():
    # https://docs.ray.io/en/latest/serve/api/doc/ray.serve.handle.DeploymentHandle.html
    ray_response = handle.remote("world")
    ray_response2 = ray.get(ray_response)
    response = ray.get(ray_response2)
    print(response)
    assert response == "Hello world!"
    return "please"


if __name__ == "__main__":
    check_call(["ray", "start", "--head", "--num-cpus", "0", "--dashboard-host", "0.0.0.0"])
    print("main")
    ray.init(address="auto")
    ray_app = Ingress.bind(Downstream.bind())
    handle: DeploymentHandle = serve.run(ray_app)
    # say_hello = SayHello.remote()
    print(
        f"""This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()} CPU resources in total
    """,
    )
    uvicorn.run(app, host="0.0.0.0", port=8001)
