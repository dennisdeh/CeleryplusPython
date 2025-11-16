import subprocess
import sys
import uuid
import os
import time
import socket
from typing import Union, Dict, Optional, Any
from celery import Celery
from celery.result import AsyncResult
from redis.redis_base import initialise_redis_backend
from utils.paths import get_project_path, load_env_variables
from rich.progress import Progress

# In-process singleton state
_WORKER_PROC: Optional[subprocess.Popen] = None
_WORKER_SIGNATURE: Optional[tuple] = (
    None  # (project_root, tasks_module, queue, pool, concurrency)
)


def celery_workers_start(
    tasks_module: str,
    concurrency: int = 32,
    pool: str = None,
    worker_prefix: str = None,
    queue: str = None,
    path_dotenv_file: str = "modules/p00_databases/.env",
    project_name: str = "Investio",
    silent: bool = True,
    debug: Union[bool, str] = False,
) -> subprocess.Popen:
    """
    Starts a Celery worker process with specific configurations and environment setup.
    Ensures only one worker is started per-process/signature and reuses it if already running.
    """
    global _WORKER_PROC, _WORKER_SIGNATURE

    if not silent:
        print("Starting Celery worker... ", end="")

    # Resolve project root and environment
    project_root = get_project_path(name=project_name)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    load_env_variables(path_dotenv_file=path_dotenv_file, project_name=project_name)
    initialise_redis_backend(path_dotenv_file=None)

    # Choose defaults
    if worker_prefix is None:
        worker_prefix = "celery"
    else:
        worker_prefix = worker_prefix.replace(" ", "_")
    if pool is None:
        # Prefer compatible default; change to "prefork" later if desired
        pool = "prefork"
    if pool not in ["solo", "threads", "gevent", "eventlet", "prefork"]:
        raise ValueError(
            f"Invalid pool type: {pool}. Must be one of: solo, threads, gevent, eventlet, prefork"
        )
    if queue is None:
        queue = "default"

    # Build a signature of intended worker settings
    signature = (project_root, tasks_module, queue, pool, int(concurrency))

    # If we already have a running worker with the same signature, reuse it
    if (
        _WORKER_PROC is not None
        and _WORKER_PROC.poll() is None
        and _WORKER_SIGNATURE == signature
    ):
        if not silent or debug:
            print("Celery worker already running (reusing existing instance).")
        return _WORKER_PROC

    # If an old worker handle exists but died or signature changed, don't reuse
    _WORKER_SIGNATURE = None
    _WORKER_PROC = None

    # Build command to execute
    worker_name = f"{worker_prefix}-{uuid.uuid4().hex[:8]}@{socket.gethostname()}"
    cmd = [
        "celery",
        "-A",
        tasks_module,
        "worker",
        f"--pool={pool}",
        f"--concurrency={concurrency}",
        f"--hostname={worker_name}",
    ]
    if queue:
        cmd.extend(["-Q", queue])

    # Log level selection
    if debug == "info":
        cmd.extend(["--loglevel", "INFO"])
    elif debug:
        cmd.extend(["--loglevel", "DEBUG"])
    elif not silent:
        cmd.extend(["--loglevel", "INFO"])

    # Environment
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    try:
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env.setdefault("C_FORCE_ROOT", "true")
    except Exception:
        pass

    if not silent or debug:
        print(f"Starting worker with PYTHONPATH: {env['PYTHONPATH']}")
        print(f"Working directory: {project_root}")
        print(f"Command: {' '.join(cmd)}")

    # Spawn
    proc = subprocess.Popen(cmd, env=env, cwd=project_root)

    # Quick sanity check: if it crashes immediately, surface error early
    time.sleep(0.3)
    if proc.poll() is not None:
        # Process exited; avoid caching a dead handle
        raise RuntimeError(
            "Celery worker exited immediately after start. Check logs above for the cause."
        )

    # Cache singleton
    _WORKER_PROC = proc
    _WORKER_SIGNATURE = signature
    return proc


def celery_workers_stop(worker_processes):
    """
    Stop all Celery workers gracefully and clear singleton handle.
    """
    global _WORKER_PROC, _WORKER_SIGNATURE

    if worker_processes is not None:
        try:
            worker_processes.terminate()
            worker_processes.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker_processes.kill()
        except Exception:
            pass

    # If singleton differs, try to terminate it as well
    if _WORKER_PROC is not None and _WORKER_PROC is not worker_processes:
        try:
            _WORKER_PROC.terminate()
            _WORKER_PROC.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                _WORKER_PROC.kill()
            except Exception:
                pass
        except Exception:
            pass

    _WORKER_PROC = None
    _WORKER_SIGNATURE = None


def celery_workers_running(worker_processes):
    """
    Check if the Celery workers are running
    """
    try:
        return worker_processes is not None and worker_processes.returncode is None
    except AttributeError:
        return False


def submit_task(task_name: str, app: Celery, *args, **kwargs) -> AsyncResult:
    """
    Submits a task to the Celery app using the provided task name and arguments.

    This function dynamically locates a task based on its name and dispatches it
    for asynchronous execution via the Celery framework. It ensures that the task
    name is resolvable within the application context and passes optional positional
    and keyword arguments to the task for further processing.

    Parameters:
        task_name (str): The name of the task to be invoked.
        app: The Celery application instance used to locate and execute the task.
        *args: Optional positional arguments to be passed to the task.
        **kwargs: Optional keyword arguments to be passed to the task.

    Returns:
        AsyncResult: An object representing the asynchronous execution of the task.
    """
    # Resolve task name: accept fully-qualified names and try to resolve short names
    registered = getattr(app, "tasks", {})
    resolved_name = None
    if task_name in registered:
        resolved_name = task_name
    else:
        # Try suffix match for short names
        if "." not in task_name:
            candidates = [n for n in registered.keys() if n.endswith(f".{task_name}")]
            # Filter out celery built-ins
            candidates = [n for n in candidates if not n.startswith("celery.")]
            if len(candidates) == 1:
                resolved_name = candidates[0]
            elif len(candidates) > 1:
                raise ValueError(
                    f"Ambiguous task name '{task_name}'. Candidates: {candidates}. "
                    f"Please use a fully-qualified task name."
                )
        else:
            # If a fully-qualified name was given but not found, try its short tail
            tail = task_name.split(".")[-1]
            if tail in registered:
                resolved_name = tail
            else:
                # Also try suffix match for the tail
                candidates = [n for n in registered.keys() if n.endswith(f".{tail}")]
                candidates = [n for n in candidates if not n.startswith("celery.")]
                if len(candidates) == 1:
                    resolved_name = candidates[0]
                elif len(candidates) > 1:
                    raise ValueError(
                        f"Ambiguous task name '{task_name}' (tail '{tail}'). "
                        f"Candidates: {candidates}. Please use an unambiguous name."
                    )
    # if resolved_name is None:
    #     # Fall back to given name; this may fail if worker doesn't know the name
    #     resolved_name = task_name
    if resolved_name is not None:
        # Task is registered in this producer app: use a bound signature
        task_sig = app.signature(resolved_name)
        return task_sig.delay(*args, **kwargs)
    else:
        # Task not registered in this producer app: route by name via send_task
        # This is the robust path when producer and worker use distinct Celery apps.
        return app.send_task(task_name, args=args, kwargs=kwargs)


def wait_for_task(result: AsyncResult, timeout: Optional[float] = None) -> Any:
    """
    Wait for a task to complete and return its result.

    Args:
        result (AsyncResult): The task result object
        timeout (float, optional): Maximum time to wait in seconds. Defaults to None (wait forever).

    Returns:
        Any: The task result
    """
    return result.get(timeout=timeout)


def wait_for_tasks(
    results: Dict[Any, AsyncResult], timeout: Optional[float] = None
) -> Dict[Any, Any]:
    """
    Wait for multiple tasks to complete and return their results.

    Args:
        results (Dict[Any, AsyncResult]): Dictionary of task result objects
        timeout (float, optional): Maximum time to wait in seconds. Defaults to None (wait forever).

    Returns:
        Dict[Any, Any]: Dictionary of task results
    """
    # Check if all tasks are done
    all_done = False
    start_time = time.time()

    while not all_done:
        all_done = True
        for key in results:
            all_done = all_done and results[key].ready()

        if not all_done:
            time.sleep(0.1)
            if timeout and (time.time() - start_time > timeout):
                break

    # Get results
    task_results = {}
    for key in results:
        if results[key].ready():
            task_results[key] = results[key].get(timeout=1)
        else:
            task_results[key] = None

    return task_results


def celery_download_status(
    d: dict,
    combine_level0: bool = False,
    check_interval: float = 0.5,
):
    """
    A more robust version of celery_download_status that doesn't rely on the .ready() check.
    Instead, it uses the task ID to directly query the backend with timeout protection.
    Supports arbitrary nesting levels, with AsyncResult objects always at the final level.

    Args:
        d: Dictionary of tasks with arbitrary nesting levels, AsyncResult at final level
        combine_level0: If True, combine level 0 dictionaries
        check_interval: Seconds to wait between status checks
    """
    # optional combination of level 0 dictionaries
    if combine_level0:
        d_level1 = {}
        for k, v in d.items():
            d_level1 = {**d_level1, **v}
        d = d_level1

    # Helper function to recursively extract AsyncResult objects and their paths
    def extract_tasks(data, path=None, result=None):
        if result is None:
            result = {}
        if path is None:
            path = []

        if isinstance(data, AsyncResult):
            # Found an AsyncResult at the final level
            category = path[0] if path else "default"
            if category not in categories:
                categories[category] = 0
                task_by_category[category] = set()
            categories[category] += 1
            task_id = data.id
            task_ids.add(task_id)
            task_map[task_id] = (path, data)
            task_by_category[category].add(task_id)
            return

        if isinstance(data, dict):
            for key, value in data.items():
                extract_tasks(value, path + [key], result)

    # Data structures to track tasks
    task_ids = set()
    task_map = {}  # Maps task_id -> (path, AsyncResult)
    categories = {}  # Maps top-level categories to count of tasks
    task_by_category = {}  # Maps categories to set of task_ids

    # Extract all AsyncResult objects
    extract_tasks(d)

    if not task_ids:
        print("No tasks found in the provided dictionary.")
        return

    # Monitor progress until all have been downloaded
    done_tasks = set()
    d_progress = {}
    time_start = time.time()
    print(" *** Downloading data *** ")

    with Progress() as progress:
        # Add progress bars for top-level categories
        for category in categories:
            d_progress[category] = progress.add_task(
                f"[red]{category}", total=categories[category]
            )

        # Continue until all tasks are done
        while len(done_tasks) < len(task_ids):
            for task_id in list(task_ids - done_tasks):
                try:
                    path, task = task_map[task_id]
                    # Use the task ID directly to check status
                    state = task.backend.get_state(task_id)

                    if state in task.backend.READY_STATES:
                        done_tasks.add(task_id)
                except Exception as e:
                    print(f"Error checking task {task_id}: {e}")
                    # Mark as done to avoid hanging
                    done_tasks.add(task_id)

            # Update progress bars
            for category in categories:
                completed = len(done_tasks.intersection(task_by_category[category]))
                progress.update(d_progress[category], completed=completed)

            # Small delay to avoid hammering the backend
            time.sleep(check_interval)

    time_end = time.time()
    print(f"Completed! (in {round((time_end - time_start) / 60, 2)}m)")


def celery_process_results(
    x: [dict, AsyncResult], timeout: int = 10, forget: bool = True
):
    """
    Processes the results from a Celery task or a dictionary of Celery tasks.

    If the input is a dictionary, the method is called recursively on each value of the
    dictionary, returning the dictionary with processed results. If the input is an
    AsyncResult object from Celery, the method fetches its status and result, optionally
    setting the result to "FAILURE" if the status is not "SUCCESS", and forgets the task
    if required.

    Parameters:
    x ([dict, AsyncResult])
        The input to be processed, which can be either a dictionary containing
        AsyncResult objects or a single AsyncResult object
    timeout (int)
        The timeout in seconds to wait for the result of the AsyncResult
    forget (bool)
        A flag indicating whether to forget the AsyncResult task after retrieving
        its result

    Returns:
    dict or str
        If the input is a dictionary, returns a dictionary with processed results.
        If the input is an AsyncResult, returns its processed result as a string.

    Raises:
    ValueError
        If the input is neither a dictionary nor an AsyncResult object.
    """
    if isinstance(x, dict):
        # recursively call the method on one level lower
        for k, v in x.items():
            x[k] = celery_process_results(v, timeout=timeout, forget=forget)
        # Return the updated dictionary
        return x
    elif isinstance(x, AsyncResult):
        status, result = x.status, x.get(timeout=timeout)
        if status != "SUCCESS":
            result = "FAILURE"
        if forget:
            x.forget()
        return result
    else:
        raise ValueError(f"The input is not a dictionary or AsyncResult: {x}.")


if __name__ == "__main__2":
    # Example workflow
    # Start a worker
    # NOTE: Provide the tasks module so Celery knows where to discover app/tasks
    tasks_module = "modules.p00_task_queueing.celery_json.main_celery"
    worker_process = celery_workers_start(
        tasks_module=tasks_module, concurrency=4, pool="threads"
    )

    try:
        # First, let's check if we can directly import the tasks module
        print("\nTrying to directly import tasks module...")
        try:
            from modules.p00_task_queueing.celery_json.tasks import add
            from modules.p00_task_queueing.celery_base import get_celery_app

            print("✓ Successfully imported the add task")
            task_path = "add"  # Short name ok if unique/registered
        except ImportError as e:
            print(f"✗ Could not import tasks module: {e}")
            print("Falling back to full task path")
            task_path = (
                "modules.p00_task_queueing.celery_json.tasks.add"  # Correct full path
            )

        # Build a Celery app for producing tasks (ensures correct broker/backend)
        try:
            from modules.p00_task_queueing.celery_base import get_celery_app

            app = get_celery_app(app_name="app")
        except Exception as e:
            raise RuntimeError(f"Failed to create Celery app for producer: {e}")

        # Submit a simple addition task
        print("\nSubmitting addition task...")
        result = submit_task(task_path, app, 4, 6)

        # Wait for the result
        print("Waiting for result...")
        output = wait_for_task(result)
        print(f"Result: {output}")

        # Submit multiple tasks
        print("\nSubmitting multiple tasks...")
        results = {}
        for i in range(5):
            results[i] = submit_task(task_path, app, i, i * 2)

        # Wait for all results
        print("Waiting for all results...")
        outputs = wait_for_tasks(results)
        for i, output in outputs.items():
            print(f"Task {i} result: {output}")

    finally:
        # Terminate the worker process
        print("\nTerminating worker...")
        worker_process.terminate()
        worker_process.wait()
        print("Worker terminated.")
