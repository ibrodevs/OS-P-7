import json
import os
import signal
from pathlib import Path

from calc_common import safe_calculate

REQUEST_FIFO = Path("/tmp/ipc_calc_requests")
_running = True


def cleanup(*_):
    global _running
    _running = False
    try:
        REQUEST_FIFO.unlink()
    except FileNotFoundError:
        pass


def ensure_fifo():
    if REQUEST_FIFO.exists():
        if not REQUEST_FIFO.is_fifo():
            raise RuntimeError(f"{REQUEST_FIFO} exists and is not a FIFO")
        REQUEST_FIFO.unlink()
    os.mkfifo(REQUEST_FIFO, 0o600)


def process_request(line: str):
    response_fifo = None
    try:
        request = json.loads(line)
        expression = request["expression"]
        response_fifo = Path(request["response_fifo"])
        result = safe_calculate(expression)
        response = {"ok": True, "result": result}
    except Exception as exc:
        response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    if response_fifo is not None:
        try:
            with response_fifo.open("w", encoding="utf-8") as stream:
                stream.write(json.dumps(response) + "\n")
                stream.flush()
        except FileNotFoundError:
            print("Client response FIFO disappeared before the reply was sent")


def main():
    ensure_fifo()
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    print(f"FIFO server is listening on {REQUEST_FIFO}")
    print("Press Ctrl+C to stop it.")

    try:
        while _running:
            # Opening for reading waits for a writer. After all writers close,
            # this loop reopens the FIFO for the next client.
            with REQUEST_FIFO.open("r", encoding="utf-8") as stream:
                for line in stream:
                    if line.strip():
                        process_request(line)
                    if not _running:
                        break
    finally:
        cleanup()
        print("FIFO server stopped; request FIFO removed.")


if __name__ == "__main__":
    main()
