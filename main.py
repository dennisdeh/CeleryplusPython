import os
import subprocess
import time
import celery_json.tasks as tasks  # imports from tasks functions to use


# 1: commands to start worker (in folder where tasks.py is): https://celery.school/celery-on-windows
"""
start workers:
    celery_json -A tasks worker --pool=solo -l info
    celery_json -A tasks worker --pool=gevent -l info
"""
# os.system("celery_json -A tasks worker --pool=solo")
# p = subprocess.Popen(
#     [
#         "celery",
#         "-A",
#         str(tasks).split("'")[1],
#         "--workdir",
#         os.path.abspath(os.path.join(os.getcwd())),
#         "-q",
#         "worker",
#         "--pool=solo",
#         "broker_connection_retry_on_startup=True",
#     ]
# )
p = subprocess.Popen(
    [
        "celery",
        "-A",
        str(tasks).split("'")[1],
        "--workdir",
        os.path.abspath(os.path.join(os.getcwd())),
        "-q",
        "worker",
        "--pool=threads",
        "--concurrency=24",
        "broker_connection_retry_on_startup=True",
    ]
)


# 2: send tasks, wait, get the result
# 2.1: add
result = tasks.add.delay(4, 6)
result.ready()
result.get(timeout=1)
result.get(propagate=False)
result.traceback
# 2.2: FMP request
n = 100
url = "https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?from=2000-03-12&to=2019-03-01&apikey=cb7254651d9869392a3f78f382dedbb2"
results = {}
# submit tasks
for i in range(n):
    results[i] = tasks.json_request.delay(url)

# Method 1: wait on results for a single task
print("Getting results for a specific task... ", end="")
res = results[50].wait(propagate=True)
results[50].forget()  # after result has been stored
print("Done!")

# Method 2: check that all tasks are done
all_done = False
time0 = time.time()
while not all_done:
    all_done = True
    for i in range(100):
        all_done = all_done and results[i].ready()
    if not all_done:
        time.sleep(0.1)
        # print("Not done...")
    else:
        time1 = time.time()
        print(f"Done! (in {round(time1 - time0, 2)}s)")

results_get = {}
for i in range(n):
    results_get[i] = results[i].get(timeout=1)
    result.get(propagate=False)
    result.traceback


# 3: kill the process after
p.terminate()
p.kill()
