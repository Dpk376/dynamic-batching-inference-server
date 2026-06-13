#!/usr/bin/env python3
"""
Load generator for dynamic-batching-inference-server.

Usage:
    python load_test.py --url http://localhost:8080 --rps 50 --duration 60
"""
import argparse
import asyncio
import random
import statistics
import time

import aiohttp

PROMPTS = [
    "Explain quantum computing in simple terms.",
    "What is the capital of France?",
    "Write a haiku about machine learning.",
    "Summarise the French Revolution in two sentences.",
    "What are the key principles of SOLID design?",
    "Describe how neural networks learn.",
    "What is the difference between TCP and UDP?",
    "Explain the CAP theorem.",
    "What is a B-tree and when would you use one?",
    "How does the attention mechanism in transformers work?",
]


async def send_request(
    session: aiohttp.ClientSession,
    url: str,
    latencies: list,
    errors: list,
    tokens: list,
) -> None:
    payload = {
        "model": "llama3-8b",
        "prompt": random.choice(PROMPTS),
        "maxTokens": random.randint(64, 256),
    }
    start = time.monotonic()
    try:
        async with session.post(
            f"{url}/v1/infer",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
            latencies.append((time.monotonic() - start) * 1000)
            tokens.append(data.get("outputTokens", 0))
    except Exception as exc:
        errors.append(str(exc))


async def run(url: str, rps: int, duration: int) -> None:
    latencies: list = []
    errors: list = []
    tokens: list = []

    interval = 1.0 / rps
    deadline = time.monotonic() + duration

    connector = aiohttp.TCPConnector(limit=rps * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        while time.monotonic() < deadline:
            tasks.append(asyncio.create_task(
                send_request(session, url, latencies, errors, tokens)
            ))
            await asyncio.sleep(interval)
        await asyncio.gather(*tasks, return_exceptions=True)

    total = len(latencies) + len(errors)
    if not latencies:
        print("No successful requests.")
        return

    sorted_lat = sorted(latencies)
    print("\n=== Load Test Results ===")
    print(f"Target RPS:     {rps}")
    print(f"Duration:       {duration}s")
    print(f"Total requests: {total}")
    print(f"Errors:         {len(errors)} ({100 * len(errors) / total:.1f}%)")
    print(f"p50 latency:    {statistics.median(latencies):.1f} ms")
    print(f"p95 latency:    {sorted_lat[int(0.95 * len(sorted_lat))]:.1f} ms")
    print(f"p99 latency:    {sorted_lat[int(0.99 * len(sorted_lat))]:.1f} ms")
    print(f"Throughput:     {sum(tokens) / duration:.1f} tokens/sec")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load generator for the dynamic-batching inference server"
    )
    parser.add_argument("--url", default="http://localhost:8080", help="Base URL of the server")
    parser.add_argument("--rps", type=int, default=20, help="Target requests per second")
    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.rps, args.duration))


if __name__ == "__main__":
    main()
