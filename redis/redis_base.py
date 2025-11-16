import subprocess
import os
import platform
from utils.paths import load_env_variables
import redis


def initialise_redis_backend(
    path_dotenv_file, project_name: str = "CeleryplusPython", silent: bool = True
):
    """
    Function to check whether the redis container is running or not.
    """
    if not silent:
        print("Checking that redis is running... ", end="")
    # load redis port mapping from environment file
    load_env_variables(path_dotenv_file=path_dotenv_file, project_name=project_name)

    # get environment variables
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT_HOST")

    if redis_port is None or redis_host is None:
        raise AssertionError(
            f"Environment variables for the redis container are not found:"
            f"redis_port={redis_port}, redis_host={redis_host}"
        )

    # assert that redis can be reached
    if platform.system() == "Windows" or platform.system() == "Linux":
        try:
            client = redis.Redis(
                host=redis_host,
                port=int(redis_port),
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            if not client.ping():
                raise AssertionError(
                    f"Redis did not respond to PING at {redis_host}:{redis_port}"
                )
        except Exception as exc:
            raise AssertionError(
                f"Cannot reach Redis at {redis_host}:{redis_port}: {exc}"
            ) from exc
    else:
        raise NotImplementedError(f"Unsupported platform: {platform.system()}")
    if not silent:
        print("Success!")
