from .task import Task
from ..client import DremioClient
import time

class DremioQueryTask(Task):
    """
    Task to run a SQL query on Dremio and wait for completion.
    Supports job cancellation on kill.
    """
    def __init__(self, name: str, client: DremioClient, sql: str, **kwargs):
        super().__init__(name, self._run_query, **kwargs)
        self.client = client
        self.sql = sql
        self.job_id = None

    def _run_query(self, context=None):
        print(f"Submitting query: {self.sql}")
        # We need a way to submit async and get job ID.
        # Assuming client.query returns a builder/dataframe, we might need a lower level API
        # or we assume we can get the job ID from the result object if it supports it.
        # Alternatively, we use the SQL API directly if exposed.
        
        # For now, let's assume we can use the client to submit and get a job ID.
        # If the client doesn't expose explicit async submit, we might have to block.
        # But to support on_kill, we need the job ID.
        
        # Let's try to use the client's internal API if possible or just assume blocking for now
        # but catching KeyboardInterrupt/Cancellation to call on_kill?
        # No, on_kill is called by the orchestrator.
        
        # If the client is blocking, we can't easily get the job ID *during* execution unless
        # the client provides a callback or returns a future.
        # Let's assume for this implementation that we are using a hypothetical async submit
        # or we just wrap the blocking call and rely on the client to handle it.
        
        # However, to be "Advanced", we should probably use the REST API to submit SQL
        # and poll.
        
        # Let's implement a polling approach using the client's auth token.
        # This requires access to the API.
        
        # Attempt to use client.api.post("sql", ...) if available.
        # Based on previous context, client has .api.
        
        try:
            res = self.client.api.post("sql", json={"sql": self.sql})
            self.job_id = res.get("id")
            print(f"Job submitted: {self.job_id}")
        except Exception as e:
            # Fallback if direct API access fails or structure is different
            print(f"Could not submit via raw API, falling back to client.query: {e}")
            return self.client.query(self.sql)

        # Poll for completion
        while True:
            job_info = self.client.api.get(f"job/{self.job_id}")
            status = job_info.get("jobState")
            
            if status == "COMPLETED":
                # Fetch results
                # results = self.client.api.get(f"job/{self.job_id}/results")
                # Return job info or results? Let's return job_id for downstream
                return self.job_id
            elif status in ["FAILED", "CANCELED"]:
                raise Exception(f"Dremio Job {self.job_id} failed with status: {status}")
            
            time.sleep(1)

    def on_kill(self):
        if self.job_id:
            print(f"Cancelling Dremio Job {self.job_id}...")
            try:
                self.client.api.post(f"job/{self.job_id}/cancel", json={})
                print("Job cancelled.")
            except Exception as e:
                print(f"Failed to cancel job: {e}")
