from typing import Tuple

from aioretry import (
    RetryPolicyStrategy,
    RetryInfo
)

BUFFER_SIZE = 1500
ALIVE_INTERVAL = 3
ALIVE_THRESHOLD = 60
ALIVE_CHECK_INTERVAL = 10
RETRY_THRESHOLD = 3
TIMEOUT = 30


def address_deserializer(address: str) -> (str, int):
    if len(address.split(":")) != 2:
        raise Exception("ip address should have format ip:port")
    ip, port = address.split(":")
    try:
        port = int(port)
    except KeyError:
        raise Exception("port must be integer")
    return ip, port


def address_serializer(addr: Tuple[str, int]):
    return f"{addr[0]}:{addr[1]}"


def retry_policy(info: RetryInfo) -> RetryPolicyStrategy:
    print(info.exception)
    return info.fails > RETRY_THRESHOLD, 0.2


def always_retry(info: RetryInfo) -> RetryPolicyStrategy:
    print(info.exception)
    return False, 0.1
