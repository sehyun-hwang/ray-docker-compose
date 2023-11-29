import time
import random
import asyncio
import aiohttp
from icecream import ic

word_list = ["apple", "book", "desk", "pen", "cat", "dog", "tree", "house", "car", "phone",
             "computer", "laptop", "keyboard", "mouse", "chair", "table", "door", "window", "wall", "floor"]

def get_random_word():
    return "_".join(random.choices(word_list, k=2))

async def get_say_hello(name: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://localhost:8000/hello?name={name}") as resp:
            ic(resp.status)
            js = await resp.json(content_type=resp.content_type)
            return js


async def main():
    tasks = []
    for _ in range(20):
        name = get_random_word()
        ic(name)
        tasks.append(asyncio.create_task(get_say_hello(name)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    ic(results)


if __name__ == "__main__":
    start = time.monotonic()

    asyncio.run(main())

    end = time.monotonic()

    print(f"Elapsed: {end - start:00.4f}s")
