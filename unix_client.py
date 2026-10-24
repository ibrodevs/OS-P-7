import argparse
import json
import socket
from pathlib import Path

SOCKET_PATH = Path("/tmp/ipc_calc.sock")


def request(expression: str):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(SOCKET_PATH))

    with client:
        client.sendall((json.dumps({"expression": expression}) + "\n").encode("utf-8"))
        data = b""
        while b"\n" not in data:
            chunk = client.recv(4096)
            if not chunk:
                break
            data += chunk

    response = json.loads(data.decode("utf-8").strip())
    if response["ok"]:
        return response["result"]
    raise RuntimeError(response["error"])


def main():
    parser = argparse.ArgumentParser(description="Unix-socket calculator client")
    parser.add_argument("expression", help='for example: "100 / 4 + 7"')
    args = parser.parse_args()

    try:
        print(f"Result: {request(args.expression)}")
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
