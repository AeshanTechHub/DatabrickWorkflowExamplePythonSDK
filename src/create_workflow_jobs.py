from databricks.sdk import WorkspaceClient  # Import the Databricks Workspace Client to interact with the Databricks workspace
from databricks.sdk.service.jobs import *  # Import all job-related services from Databricks SDK


class CreateWorkflowJobs:
    """
    A class to create and manage Databricks workflow jobs, automating task scheduling using specified configurations.
    """

    def __init__(self, job_name: str, job_timeout_seconds: int, max_concurrent_runs: int, cluster_id: str, notebook_path: str, cron_schedule: str, timezone:str):
        """
        Initializes the CreateWorkflowJobs class with the necessary parameters.
        
        :param job_name: Name of the Databricks job
        :param job_timeout_seconds: Maximum time (in seconds) a job is allowed to run
        :param max_concurrent_runs: Maximum number of concurrent runs for the job
        :param cluster_id: Cluster ID where the notebook task will be executed
        :param notebook_path: Path to the notebook in the Databricks workspace
        :param cron_schedule: Cron expression to schedule the job execution
        :param timezone: Timezone for the cron job scheduling
        """
        self.job_name = job_name
        self.job_timeout_seconds = job_timeout_seconds
        self.max_concurrent_runs = max_concurrent_runs
        self.cluster_id = cluster_id
        self.notebook_path = notebook_path
        self.cron_schedule = cron_schedule
        self.timezone = timezone

    def create_workflow_jobs(self):
        """
        Creates a Databricks workflow job with a notebook task and schedules it according to a cron expression.

        :return: The created job object
        """
        # Instantiate the Databricks Workspace client to access the workspace
        w = WorkspaceClient()

        # Create the job using Databricks Jobs API
        job = w.jobs.create(
            name=self.job_name,  # Set the name of the job
            timeout_seconds=self.job_timeout_seconds,  # Set the job timeout duration
            max_concurrent_runs=self.max_concurrent_runs,  # Set the max number of concurrent runs
            tasks=[
                Task(
                    description=f"{self.job_name}: Data Ingestion",  # Task description indicating data ingestion
                    existing_cluster_id=self.cluster_id,  # Use an existing cluster for task execution
                    notebook_task=NotebookTask(
                        notebook_path=self.notebook_path,  # Path to the notebook to be executed
                        source=Source("WORKSPACE")  # Indicate that the source of the notebook is the workspace
                    ),
                    task_key=f"{self.job_name}_task",  # A unique key for the task
                )
            ],
            schedule=CronSchedule(  # Schedule the job based on the provided cron expression and timezone
                quartz_cron_expression=self.cron_schedule,  # Cron expression to define the job execution schedule
                timezone_id=self.timezone  # Timezone for scheduling
            )
        )

        return job  # Return the created job object
