import argparse
import json
import os
import uuid
from pathlib import Path

REQUEST_FIFO = Path("/tmp/ipc_calc_requests")


def request(expression: str):
    if not REQUEST_FIFO.exists():
        raise RuntimeError("FIFO server is not running")

    response_fifo = Path(f"/tmp/ipc_calc_response_{os.getpid()}_{uuid.uuid4().hex}")
    os.mkfifo(response_fifo, 0o600)

    try:
        payload = {
            "expression": expression,
            "response_fifo": str(response_fifo),
        }
        with REQUEST_FIFO.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(payload) + "\n")
            stream.flush()

        with response_fifo.open("r", encoding="utf-8") as stream:
            response = json.loads(stream.readline())

        if response["ok"]:
            return response["result"]
        raise RuntimeError(response["error"])
    finally:
        try:
            response_fifo.unlink()
        except FileNotFoundError:
            pass


def main():
    parser = argparse.ArgumentParser(description="FIFO calculator client")
    parser.add_argument("expression", help='for example: "(10 + 2) * 3"')
    args = parser.parse_args()

    try:
        print(f"Result: {request(args.expression)}")
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
