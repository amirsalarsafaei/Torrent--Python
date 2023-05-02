import asyncio
import sys
from asyncio import transports
from datetime import timedelta
from typing import Any

import aioconsole

import shared
from models import *
from shared import *

logs = []
all_file_logs = []
peers: List[Peer] = []
files: List[File] = []


def get_peer_by_id(peer_id):
    for peer in peers:
        if peer.ID == peer_id:
            return peer
    return None


def get_file_by_name(file_name):
    for file in files:
        if file.name == file_name:
            return file
    return None


def peer_is_dead(peer: Peer):
    if peer.mode == PeerMode.Receive:
        while len(peer.downloading) > 0:
            downloading_file = peer.downloading.pop()
            all_file_logs.append(
                f"{datetime.datetime.now()}, peer_id: {peer.ID}, was downloading {downloading_file.name} but failed because is dead")
            downloading_file.logs.append(
                f"{datetime.datetime.now()}, peer_id: {peer.ID}, was downloading {downloading_file.name} but failed because is dead")

    peer.mode = PeerMode.Dead
    print(f"peer {peer.ID} died from {peer.address}")
    for file in peer.files:
        file.logs.append(f"{datetime.datetime.now()}, peer_id: {peer.ID}, had this file but died")
        all_file_logs.append(f"{datetime.datetime.now()}, peer_id: {peer.ID}, had file_name: {file.name} but died")
        file.peers.remove(peer)


class TrackerUDPServer(asyncio.DatagramProtocol):

    async def share(self, peer: Peer, msg: str):
        file_name, file_size = msg.split()
        file_name = file_name.strip("'")
        file = File(file_name, file_size, [], [peer])
        files.append(file)
        peer.files.append(file)
        self.transport.sendto("success".encode(), peer.get_addr())
        all_file_logs.append(
            f"{datetime.datetime.now()} shared from address {peer.get_addr()} with peer_id {peer.ID} with name {file_name} and size {file_size}"
        )
        file.logs.append(
            f"{datetime.datetime.now()} shared from address {peer.get_addr()} with peer_id {peer.ID} with name {file_name} and size {file_size}"
        )

    async def alive(self, peer: Peer, msg: str):
        peer.last_alive = datetime.datetime.now()
        logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: alive, log: success"
        )

    async def get(self, peer: Peer, msg: str):
        file_name = msg.strip("'")

        file = get_file_by_name(file_name)
        if file is None:
            self.transport.sendto("failed%file doesn't exist%nothing".encode(), peer.get_addr())
            logs.append(
                f"{datetime.datetime.now()}"
                f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
                f"operation: get, log: wanted to get {file_name} but didnt exist"
            )
            return
        res = f"success%{file.size}%" + ' '.join([f"{peer.ID}/{peer.listen_address}" for peer in file.peers])
        self.transport.sendto(res.encode(), peer.get_addr())
        peer.downloading.add(file)

        logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: get, log: started downloading {file_name}, peers that have it {','.join([peer.ID for peer in file.peers])}"
        )

        file.logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: get, log: started downloading {file_name}"
        )

    async def get_finished(self, peer: Peer, msg: str):
        file_name = msg.strip("'")
        file = get_file_by_name(file_name)
        if file is None:
            self.transport.sendto("failed-file doesn't exist".encode(), peer.get_addr())
            logs.append(
                f"{datetime.datetime.now()}"
                f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
                f"operation: get_finished, log: says finished downloading {file_name} but file doesn't exist"
            )
            return
        if file not in peer.downloading:
            self.transport.sendto("failed-you don't have the file".encode(), peer.get_addr())
            logs.append(
                f"{datetime.datetime.now()}"
                f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
                f"operation: get_finished, log: says finished downloading {file_name} but peer hasn't get the file peers yet"
            )
            return
        peer.downloading.remove(file)
        peer.mode = PeerMode.Seed
        file.peers.append(peer)
        peer.files.append(file)
        self.transport.sendto("success-nothing".encode(), peer.get_addr())

        logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: get_finished, log: finished downloading {file_name}"
        )

        all_file_logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: get_finished, log: finished downloading {file_name} and has it"
        )

        file.logs.append(
            f"{datetime.datetime.now()}"
            f" message: {msg}, address: {peer.get_addr()}, peer: {peer.ID}, "
            f"operation: get_finished, log: finished downloading {file_name} and has it"
        )

    async def initialize(self, msg, addr):
        peer_id, operation, listen_addr = msg.split()
        if get_peer_by_id(peer_id) is not None:
            self.transport.sendto("success".encode(), addr)
            logs.append(
                f"{datetime.datetime.now()} message: {msg}, address: {addr}, peer: {peer_id}, operation: init, log: peer already initialized")

            return
        print(f"New peer {peer_id} from {shared.address_serializer(addr)}")
        if operation == "get":
            peer = Peer([], peer_id, PeerMode.Receive, shared.address_serializer(addr), listen_addr,
                        datetime.datetime.now(), set())
        elif operation == "share":
            peer = Peer([], peer_id, PeerMode.Seed, shared.address_serializer(addr), listen_addr,
                        datetime.datetime.now(), set())
        else:
            logs.append(
                f"{datetime.datetime.now()} message: {msg}, address: {addr}, peer: {peer_id}, operation: init, log: invalid operation")
            self.transport.sendto("failed".encode(), addr)
            return
        peers.append(peer)
        self.transport.sendto("success".encode(), peer.get_addr())

    def __init__(self):
        self.transport = None
        self.op_to_func = {
            'share': self.share,
            'get': self.get,
            'alive': self.alive,
            'get_finished': self.get_finished,
            'init': self.initialize
        }

    def connection_made(self, transport: transports.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        msg = data.decode()
        op, peer_id = msg.split()[:2]
        if op not in self.op_to_func:
            logs.append(
                f"{datetime.datetime.now()} message: {msg}, address: {addr}, peer: {peer_id}, operation: {op}, log: invalid operation")
            return

        if op == "init":
            asyncio.create_task(self.op_to_func[op](' '.join(msg.split()[1:]), addr))
            return
        peer = get_peer_by_id(peer_id)
        if peer is None:
            logs.append(
                f"{datetime.datetime.now()} message: {msg}, address: {addr}, peer: {peer_id}, operation: {op}, log: invalid operation peer not initialized")
            return
        asyncio.create_task(self.op_to_func[op](peer, ' '.join(msg.split()[2:])))

    def error_received(self, exc: Exception) -> None:
        logs.append(f"{datetime.datetime.now()} UDP error received {exc}")


async def check_peers():
    while True:
        for peer in peers:
            if peer.last_alive + timedelta(seconds=ALIVE_THRESHOLD) < datetime.datetime.now():
                peer_is_dead(peer)
                peers.remove(peer)
        await asyncio.sleep(ALIVE_CHECK_INTERVAL)


async def run_server(ip: str, port: int):
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: TrackerUDPServer(),
        local_addr=(ip, port),
    )
    print(f"Tracker is running on {ip}:{port}")
    asyncio.create_task(check_peers())
    while True:
        command: str = await aioconsole.ainput("please enter your command:\n")
        if command == "request logs":
            print('\n'.join(logs))
        if command == "file_logs-all":
            print('\n'.join(all_file_logs))
        if command.split(">", 1)[0] == "file_logs":
            file_name = command.split(">", 1)[1]
            file = get_file_by_name(file_name)
            if file is None:
                print("file not found")
                continue
            print('\n'.join(file.logs))


if len(sys.argv) != 2:
    print("invalid argument size, format: python tracker.py <TRACKER_ADDRESS>")
    exit(-1)

tracker_address = sys.argv[1]

tracker_ip, tracker_port = tracker_address.split(":")
tracker_port = int(tracker_port)
asyncio.run(run_server(tracker_ip, tracker_port))
