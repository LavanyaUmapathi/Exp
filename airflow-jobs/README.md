# Introduction

[![Build Status](https://ci.caspian.rax.io/job/Airflow%20jobs/badge/icon)](https://ci.caspian.rax.io/job/Airflow%20jobs/)

Repository with airflow jobs. Airflow addresses are [production](https://airflow-prod.caspian.rax.io/) and [staging](https://airflow-staging.caspian.rax.io/).

# Process

When someone is about to make some changes to airflow jobs (either fix or add a new one) one should follow the [github workflow process](https://guides.github.com/introduction/flow/). That requires you to fork repository, create a separate branch and do all of your modifications there. Once you finish create a pull request (PR) to get merged into `master` branch.

We use `Jenkins` to run tests. Please make sure you introduce your changes with a separate PR as only it triggers the jenkins build and test run.

# Workflow description

Each workflow (=`airflow` job) should consist of one or more python modules, zero or more  config files, and possibly unit tests. All utility functions or common tasks should be in `common` directory.

## Directory structure

 * DAG directory: `dags`. Use airflow.DAGS_FOLDER to get it in runtime. Place your jobs here.
 * Config directory: `configs`. Place your configs here. Configs should be in `yaml` format.
 * Unittest directory: `tests`. Should be discoverable by `py.test`.

## Deployment and versioning

Airflow jobs deployment is done with `ansible`. See http://github.com/racker/caspian-deploy
You have to specify SHA1 in ansible configs:

```
airflow_dag_repo_version: d65af638e9a5517bde13ed79ee14f152da65cbbb
```

See configs in `ansible/group_vars/*/var.yml`

Following option is suitable for testing branch which is in another repository, like pull request:

```
airflow_dag_repo_url: git@github.com:racker/airflow-jobs.git
```

See config `ansible/roles/airflow-common/defaults/main.yml`

To actually deploy it to servers make sure `ansible` is installed and do the following:

    git clone https://github.com/racker/caspian-deploy/
    cd caspian-deploy/ansible
    ansible-playbook -i staging airflows.yml -t airflow-jobs -u YOUR_SSH_USERNAME
    # or
    ansible-playbook -i prod airflows.yml -t airflow-jobs -u YOUR_SSH_USERNAME

## 3rd party package dependencies

In case your job requires some other python library to run you should update [caspain-deploy](http://github.com/racker/caspian-deploy) repo as all deployment is done with `ansible`.

See `ansible/roles/airflow-common/tasks/main.yml` and `ansible/roles/airflow-common/defaults/main.yml`. You should only change `airflow_extra_pip_packages` setting in `default/main.yml` file.

## Current API
### Credentials

All possible credentials should be stored in PasswordSafe. Airflow installation uses its own service account (`EBI_CASPIAN_PROD`, `EBI_CASPIAN_QA`) to access it.

Here is how you access it:

```python
from common.safepass import get_safepass

(host, login, passwd) = get_safepass(PROJECT_ID, CREDENTIAL_ID)


print "Username is " + login
print "Hostname is " + host
print "Password is " + passwd
```

### Configs

Here is how you access your configs:

```python
from common.configs import get_config

config_name = "folder1/folder2/folder3/config_name.yaml" # should be unique
config = get_config(config_name) # returns an object as pyyaml does

print "Timeout is " + config['timeout'] # Just an example
print "Timeout is " + config.timeout
```

### Other

`get_log_path` generates path for log file (date-based) and also create `latest.log` pointing to last log created.

``` python
from common.log import get_log_path
log_path = get_log_path(base_dir, prefix) # prefix is some short string, describing your job
```

`alert_task` generates alert task (PagerDuty) that should be dependent on your job result

``` python
from common.tasks import alert_task
t2 = alert_task(log=LOG_PATH,
                prefix=LOG_PREFIX,
                dag=YOUR_DAG)

t2.set_upstream(YOUR_DAG_PREVIOUS_TASK)
```

# Troubleshooting

## Start date

Airflow has a special notion of `start_date`. It schedules task run only after:
 1. `start_date + schedule_interval > now()`, i.e. after interval **ends**.
 2. Its dependencies are **met**: parents either succeded, skipped or failed.

Each task is independent and airflow won't schedule dependent task the same time as parent task.

If you don't want to specify some start date and just want you dag start running on deploy (without backcfilling) it is possible to specify dynamic `start_date` with `datetime.today()`, but there are several pitfalls you should be aware of:

 1. Scheduler reloads dag on file update **and** also every 10 heartbeats (say every 50 sec). So `start_date` will constantly update. So, if you specify `start_date=datetime.today()` and `schedule_interval=timedelta(1)` it will never run it because `datetime.today() + timedelta(1)` will be always greater `datetime.today()`. Use rounding (setting hour:minute to 00:00) or make `start_date` be yesterday.
 2. Scheduler remembers last execution date and checks it with `schedule_interval` if task is ready to run. So once you got your task running volatile `start_date` won't affect subsequent runs.
 3. **Important!** Use time rounding! Otherwise dependent task won't be able to find their parent execution times and will never run.

The best options of dealing with `start_date` are:

 1. Use static start_date, or
 2. Use dynamic start_date, **round** it to your `schedule_interval` and make it already in the past!
```python
from datetime import datetime, timedelta, time

today = datetime.today()
'start_date': datetime.combine(today, time(0, 15, 0)) - timedelta(1)
...
schedule_interval=timedelta(1)
```
