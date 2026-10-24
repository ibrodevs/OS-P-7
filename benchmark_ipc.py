import argparse
import multiprocessing as mp
from array import array
from multiprocessing import shared_memory
from time import perf_counter

DEFAULT_COUNT = 10_000_000


def verify_buffer(buffer, count):
    view = memoryview(buffer).cast("I")
    try:
        return (view[0], view[count // 2], view[count - 1])
    finally:
        view.release()


def shared_memory_worker(name, byte_size, count, result_connection):
    shm = shared_memory.SharedMemory(name=name)
    try:
        check = verify_buffer(shm.buf[:byte_size], count)
        result_connection.send(check)
    finally:
        shm.close()
        result_connection.close()


def pipe_worker(data_connection, result_connection, count):
    try:
        payload = data_connection.recv_bytes()
        check = verify_buffer(payload, count)
        result_connection.send(check)
    finally:
        data_connection.close()
        result_connection.close()


def benchmark_shared_memory(source, count):
    byte_size = len(source) * source.itemsize
    result_parent, result_child = mp.Pipe(duplex=False)
    shm = shared_memory.SharedMemory(create=True, size=byte_size)
    process = None

    try:
        start = perf_counter()
        shm.buf[:byte_size] = memoryview(source).cast("B")
        process = mp.Process(
            target=shared_memory_worker,
            args=(shm.name, byte_size, count, result_child),
        )
        process.start()
        result_child.close()
        check = result_parent.recv()
        process.join()
        elapsed = perf_counter() - start
        return elapsed, check
    finally:
        result_parent.close()
        if process is not None and process.is_alive():
            process.terminate()
            process.join()
        shm.close()
        shm.unlink()


def benchmark_pipe(source, count):
    data_receiver, data_sender = mp.Pipe(duplex=False)
    result_parent, result_child = mp.Pipe(duplex=False)
    process = mp.Process(target=pipe_worker, args=(data_receiver, result_child, count))
    process.start()
    data_receiver.close()
    result_child.close()

    try:
        start = perf_counter()
        data_sender.send_bytes(source)
        data_sender.close()
        check = result_parent.recv()
        process.join()
        elapsed = perf_counter() - start
        return elapsed, check
    finally:
        if not data_sender.closed:
            data_sender.close()
        result_parent.close()
        if process.is_alive():
            process.terminate()
            process.join()


def main():
    parser = argparse.ArgumentParser(description="Compare SharedMemory and Pipe IPC")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    args = parser.parse_args()

    if args.count <= 0:
        raise SystemExit("--count must be positive")

    print(f"Creating an array of {args.count:,} unsigned 32-bit integers...")
    source = array("I", range(args.count))
    expected = (0, args.count // 2, args.count - 1)
    mib = len(source) * source.itemsize / (1024 * 1024)
    print(f"Payload size: {mib:.2f} MiB")

    shm_time, shm_check = benchmark_shared_memory(source, args.count)
    pipe_time, pipe_check = benchmark_pipe(source, args.count)

    if shm_check != expected or pipe_check != expected:
        raise RuntimeError(
            f"validation failed: expected={expected}, shm={shm_check}, pipe={pipe_check}"
        )

    print("\nResults")
    print(f"SharedMemory: {shm_time:.6f} s")
    print(f"Pipe:         {pipe_time:.6f} s")
    if shm_time > 0:
        print(f"Pipe / SharedMemory ratio: {pipe_time / shm_time:.2f}x")
    print("\nShared memory is always unlinked in a finally block.")


if __name__ == "__main__":
    main()
