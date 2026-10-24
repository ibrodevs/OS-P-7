import json
import multiprocessing as mp
import os
import signal
import socket
from pathlib import Path

from calc_common import safe_calculate

SOCKET_PATH = Path("/tmp/ipc_calc.sock")
_running = True
_workers = []


def handle_client(connection):
    with connection:
        try:
            data = b""
            while b"\n" not in data:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                data += chunk
            request = json.loads(data.decode("utf-8").strip())
            result = safe_calculate(request["expression"])
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        connection.sendall((json.dumps(response) + "\n").encode("utf-8"))


def stop_server(*_):
    global _running
    _running = False


def cleanup():
    for worker in _workers:
        worker.join(timeout=1)
        if worker.is_alive():
            worker.terminate()
            worker.join()
    try:
        SOCKET_PATH.unlink()
    except FileNotFoundError:
        pass


def main():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()

    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(SOCKET_PATH))
    os.chmod(SOCKET_PATH, 0o600)
    server.listen()
    server.settimeout(0.5)

    print(f"Unix-socket server is listening on {SOCKET_PATH}")
    print("Each client is handled by a separate process. Press Ctrl+C to stop.")

    try:
        while _running:
            try:
                connection, _ = server.accept()
            except socket.timeout:
                _workers[:] = [w for w in _workers if w.is_alive()]
                continue

            worker = mp.Process(target=handle_client, args=(connection,))
            worker.start()
            connection.close()
            _workers.append(worker)
    finally:
        server.close()
        cleanup()
        print("Unix-socket server stopped; socket file removed.")


if __name__ == "__main__":
    main()
