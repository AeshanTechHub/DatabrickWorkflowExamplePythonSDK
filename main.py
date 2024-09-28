# Databricks notebook source
# DBTITLE 1,Install Dependant Libraries
!cp ./requirements.txt ~/.
%pip install -r ~/requirements.txt

# COMMAND ----------

# DBTITLE 1,Restart the kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os

# COMMAND ----------

# DBTITLE 1,Setup the root path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(notebook_path)
os.chdir(f"/Workspace/{repo_root}/")
%pwd

# COMMAND ----------

# DBTITLE 1,Import modules
from src.create_workflow_jobs import *

# COMMAND ----------

# DBTITLE 1,Add Configurations
job_name = 'Add your job name here'
job_timeout_seconds = 'Add the timeout in seconds here (integer type)'
max_concurrent_runs = 'Add the max concurrent runs here (integer type)'
cluster_id = 'Add the existing cluster id here'
notebook_path = 'Add the notebook path here'
cron_schedule = 'Add the cron schedule syntax here'
timezone = 'Add the timezone here'

# COMMAND ----------

# DBTITLE 1,Create jobs in workflow - Pass the Parameters
create_jobs = CreateWorkflowJobs(job_name, job_timeout_seconds, max_concurrent_runs, cluster_id, notebook_path, cron_schedule, timezone)

create_jobs.create_workflow_jobs()
