import asyncio
import datetime
import os
import random
import shutil
import socket
import sys
import uuid
from typing import List

import aioconsole

from aioretry import retry, RetryInfo, RetryPolicyStrategy
import asyncio_dgram
from asyncio_dgram.aio import DatagramClient
import shared

files = {}
request_logs = []
file_peers: List[str]
tracker_datagram_endpoint: DatagramClient
ID: uuid.UUID
send_alive_task: asyncio.Task


def retry_policy_send_alive(info: RetryInfo) -> RetryPolicyStrategy:
    print(info.exception)
    return info.fails > 2, 0.1


def retry_policy_forever(info: RetryInfo) -> RetryPolicyStrategy:
    print(info.exception)
    return False, 0.2


@retry(retry_policy_forever)
async def download(file_name, listen_ip, listen_port):
    if len(file_peers) == 0:
        print("no peer available to download")
        exit(0)
    rand_idx = random.randint(0, len(file_peers) - 1)
    peer_id, peer_addr = file_peers[rand_idx].split("/")
    file_peers.remove(file_peers[rand_idx])
    ip, port = shared.address_deserializer(peer_addr)
    client_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    loop = asyncio.get_event_loop()
    await loop.sock_connect(client_socket, address=(ip, port))
    await loop.sock_sendall(client_socket, data=f"'{file_name}' {ID}".encode())
    data = await loop.sock_recv(client_socket, shared.BUFFER_SIZE)

    msg = data.decode()
    if msg.split(" -")[0] != "success":
        raise Exception(f"download failed with message {msg}")

    await loop.sock_sendall(client_socket, "ack".encode())

    file_path = os.path.join(final_directory, file_name)

    with open(file_path, "wb") as f:
        while True:
            buffer_bytes = await loop.sock_recv(client_socket, shared.BUFFER_SIZE)
            if not buffer_bytes:
                break
            f.write(buffer_bytes)
    client_socket.close()

    await tracker_datagram_endpoint.send(f"get_finished {ID} '{file_name}'".encode())

    files[file_name] = file_path
    asyncio.create_task(seed_mode(listen_ip, listen_port))


@retry(retry_policy=shared.retry_policy)
async def initialize(operation):
    msg = await send_and_recv_tracker_message(f"init {ID} {operation} {listen_ip}:{listen_port}")
    if msg != "success":
        raise Exception("couldn't initialize")


async def handle_client(client, addr):
    loop = asyncio.get_event_loop()
    data = await loop.sock_recv(client, shared.BUFFER_SIZE)
    msg, peer_id = data.decode().split()
    download_file_name = msg.strip("'")
    if download_file_name not in files:
        request_logs.append(
            f"{datetime.datetime.now()}, peer_id: {peer_id}, peer_addr: {addr}, op: get, logs: downloading file {download_file_name} but not found")
        await loop.sock_sendall(client, "failed-file not found".encode())
        return
    await loop.sock_sendall(client, "success".encode())

    data = await loop.sock_recv(client, shared.BUFFER_SIZE)
    msg = data.decode()

    if msg != "ack":
        raise Exception("download didn't acknowledge")

    request_logs.append(
        f"{datetime.datetime.now()}, peer_id: {peer_id}, peer_addr: {addr}, op: get, logs: downloading file {download_file_name}")

    with open(files[download_file_name], "rb") as f:
        while True:
            buffer_bytes = f.read(shared.BUFFER_SIZE)
            if not buffer_bytes:
                break
            await loop.sock_sendall(client, buffer_bytes)
    client.close()


async def seed_mode(listen_ip, listen_port):
    server_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server_socket.bind((listen_ip, listen_port))
    server_socket.listen()
    server_socket.setblocking(False)
    loop = asyncio.get_running_loop()
    while True:
        client, addr = await loop.sock_accept(server_socket)
        asyncio.create_task(handle_client(client, addr))


@retry(retry_policy_forever)
async def send_alive():
    await tracker_datagram_endpoint.send(f"alive {ID}".encode())


async def keep_alive():
    while True:
        if len(asyncio.all_tasks()) == 1:
            print("all tasks are done(or failed) exiting")
            exit(0)
        try:
            await send_alive()
        except Exception as e:
            print(f"couldn't send heart beat {e}")
            exit(0)
        await asyncio.sleep(shared.ALIVE_INTERVAL)


@retry(shared.retry_policy)
async def send_and_recv_tracker_message(msg: str):
    await asyncio.wait_for(tracker_datagram_endpoint.send(msg.encode()), shared.TIMEOUT)
    data , _ = await asyncio.wait_for(tracker_datagram_endpoint.recv(), shared.TIMEOUT)
    return data.decode()


@retry(shared.retry_policy)
async def handle_share():
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    new_path = os.path.join(final_directory, file_name)
    shutil.copyfile(file_path, new_path)
    msg = await send_and_recv_tracker_message(f"{operation} {ID} '{file_name}' {file_size}")
    if msg != "success":
        raise Exception("failed with message ", msg)
    files[file_name] = file_path
    asyncio.create_task(seed_mode(listen_ip, listen_port))


@retry(shared.retry_policy)
async def handle_get():
    global file_peers
    file_name = file_path
    msg = await send_and_recv_tracker_message(f"{operation} {ID} '{file_name}'")
    status, file_size, file_peers = msg.split('%')
    if status != "success":
        raise Exception("failed with message ", msg)
    file_peers = msg.split('%')[2].split()
    await download(file_name, listen_ip, listen_port)


@retry(shared.retry_policy)
async def start_peer():
    global ID, send_alive_task, final_directory
    ID = uuid.uuid4()
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, str(ID))
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)
    print(f"trying to start peer {ID}")
    await initialize(operation=operation)
    send_alive_task = asyncio.create_task(keep_alive())
    if operation == "share":
        await handle_share()
    elif operation == "get":
        await handle_get()


async def run_server():
    global tracker_datagram_endpoint
    tracker_datagram_endpoint = await asyncio_dgram.connect((tracker_ip, tracker_port))
    try:
        await start_peer()
    except Exception:
        print("couldn't start peer")
        exit(0)


async def main():
    asyncio.create_task(run_server())
    while True:
        command = await aioconsole.ainput("please enter command:\n")
        if command == 'request logs':
            print('\n'.join(request_logs))
        else:
            print("incorrect command")


if len(sys.argv) != 5:
    print(
        "invalid argument size, format: python peer.py [share|get] <FILE_NAME> <TRACKER_ADDRESS> <LISTEN_ADDRESS>")
    exit(-1)

operation = sys.argv[1]
file_path = sys.argv[2]
tracker_address = sys.argv[3]
listen_address = sys.argv[4]

if operation == "share" and not os.path.isfile(file_path):
    print("path to file doesn't exist")
    exit(-1)

if operation not in ("share", 'get'):
    print("invalid operation, only get or share are acceptable")
    exit(-1)

tracker_ip, tracker_port = shared.address_deserializer(tracker_address)
listen_ip, listen_port = shared.address_deserializer(listen_address)

asyncio.run(main())
