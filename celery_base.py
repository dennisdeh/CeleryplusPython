from typing import Union
from celery import Celery
import os


def get_celery_parameters():
    return {
        "backend": "redis",
        "backend_host": os.environ.get("REDIS_HOST", "localhost"),
        "backend_port": os.environ.get("REDIS_PORT_HOST", "9997"),
    }


def get_celery_app(
    app_name: Union[str, None] = "celery",
    test_connectivity: bool = False,
    config: Union[dict, None] = None,
):
    """
    Initialises and returns a Celery application instance.

    This function loads environment variables, sets up the message broker and result
    backend for Celery, and configures the Celery application. The backend can either be
    Redis or RabbitMQ, and the configuration details are retrieved from environment
    variables. If the required environment variables are not found, an appropriate exception
    is raised.

    Parameters:
    app_name (Union[str, None]): Name of the Celery application
    test_connectivity (bool): Whether to check that the backend is running before starting the app
    config: Config module to load. If None, the default config is used.

    Raises:
    AssertionError: If required environment variables for the backend (Redis or RabbitMQ)
    are not set.
    ValueError: If an invalid backend is specified.

    Returns:
    Celery: A configured Celery application instance.
    """
    # 0: Initialisation
    # initial assertions
    assert isinstance(app_name, str) or app_name is None, "app_name must be a string"
    assert isinstance(test_connectivity, bool), "test_connectivity must be a boolean"
    assert isinstance(config, dict) or config is None, "config must be a dictionary"
    # get env variables
    d_params = get_celery_parameters()
    backend = d_params["backend"]
    backend_host = d_params["backend_host"]
    backend_port = d_params["backend_port"]
    # assertions on env variables
    assert backend is not None, "Backend environment variable not set"
    assert backend_host is not None, "Backend host environment variable not set"
    assert backend_port is not None, "Backend port environment variable not set"

    # 1: Initialise backend and ensure that the environment variables are set
    if backend == "redis":
        str_broker = f"redis://{backend_host}:{backend_port}/0"
        str_backend = f"redis://{backend_host}:{backend_port}/1"
    elif backend == "rabbitmq":
        rabbitmq_user = os.getenv("RABBITMQ_USER")
        rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
        assert rabbitmq_user is not None, "RABBITMQ_USER environment variable not set"
        assert (
            rabbitmq_password is not None
        ), "RABBITMQ_PASSWORD environment variable not set"
        str_broker = (
            f"amqp://{rabbitmq_user}:{rabbitmq_password}@{backend_host}:{backend_port}/"
        )
        str_backend = (
            f"rpc://{rabbitmq_user}:{rabbitmq_password}@{backend_host}:{backend_port}/"
        )

    else:
        raise ValueError("Invalid backend")

    # 2: Optional test of connectivity
    if test_connectivity:
        try:
            if backend == "redis":
                import redis

                test_timeout_seconds = 10
                # Ping and simple set/get on the result backend DB (db=1)
                client = redis.Redis(
                    host=backend_host,
                    port=int(backend_port),
                    db=1,
                    socket_connect_timeout=test_timeout_seconds,
                    socket_timeout=test_timeout_seconds,
                )
                # Ping
                if not client.ping():
                    raise ConnectionError("Redis ping failed")
                # Set/Get/Delete round-trip
                test_key = f"celery_healthcheck:test_{os.getpid()}"
                client.set(test_key, "ok", ex=10)
                value = client.get(test_key)
                client.delete(test_key)
                if value != b"ok":
                    raise ConnectionError("Redis set/get round-trip failed")
                else:
                    print(f"{str_backend}: Celery backend connection OK!")
            elif backend == "rabbitmq":
                # Use kombu to test broker connectivity
                try:
                    from kombu import Connection
                except Exception as e:
                    raise ConnectionError(f"kombu import failed: {e}") from e
                try:
                    with Connection(str_broker, connect_timeout=10) as conn:
                        conn.connect()
                        if not conn.connected:
                            raise ConnectionError(
                                "RabbitMQ broker connection not established"
                            )
                except Exception as e:
                    raise ConnectionError(
                        f"RabbitMQ broker connection failed: {e}"
                    ) from e
                print(f"{str_backend}: Celery backend connection OK!")
            else:
                # Should not happen due to earlier validation
                raise ValueError("Unsupported backend for connectivity test")
        except Exception as e:
            raise ConnectionError(
                f"Backend connectivity test failed for '{backend}': {e}"
            ) from e

    # 3: Initialise Celery app
    app = Celery(
        app_name,
        broker=str_broker,
        backend=str_backend,
    )
    # Configure Celery app
    if config is None:
        pass
    elif isinstance(config, dict) and len(config) > 0:
        app.conf.update(**config)
    else:
        raise ValueError("Invalid input for config")

    return app


app = get_celery_app()
