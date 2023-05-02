import dataclasses
import datetime
from typing import List, Set
import enum


class PeerMode(enum.Enum):
    Seed = 1
    Receive = 2
    Dead = 3


@dataclasses.dataclass
class File:
    name: str
    size: int
    logs: List[str]
    peers: list

    def __hash__(self):
        return self.name.__hash__()


@dataclasses.dataclass
class Peer:
    files: List[File]
    ID: str
    mode: PeerMode
    address: str
    listen_address: str
    last_alive: datetime.datetime
    downloading: Set[File]

    def get_addr(self) -> (str, int):
        ip, port = self.address.split(":")
        port = int(port)
        return ip, port
