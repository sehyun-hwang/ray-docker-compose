from collections import Counter
import socket
import time
from subprocess import check_call

import uvicorn
import ray
from fastapi import FastAPI

app = FastAPI()


@ray.remote
def f():
    time.sleep(0.001)
    print(
        """This cluster consists of
        {} nodes in total
        {} CPU resources in total
    """.format(
            len(ray.nodes()), ray.cluster_resources()["CPU"]
        )
    )
    # Return IP address.
    return socket.gethostbyname(socket.gethostname())


@app.get("/")
async def root():
    object_ids = [f.remote() for _ in range(2)]
    ip_addresses = ray.get(object_ids)

    print("Tasks executed")
    for ip_address, num_tasks in Counter(ip_addresses).items():
        print("    {} tasks on {}".format(num_tasks, ip_address))
    return {"message": "Hello World"}


if __name__ == "__main__":
    check_call(["ray", "start", "--head"])
    print("main")
    ray.init(address="auto")
    print(
        """This cluster consists of
        {} nodes in total
        {} CPU resources in total
    """.format(
            len(ray.nodes()), ray.cluster_resources()["CPU"]
        )
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
