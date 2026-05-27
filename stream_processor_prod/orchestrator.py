import os
import re
import requests
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common import Configuration

FLINK_API = "http://jobmanager:8081/jobs"

class FlinkJobExecutor:
    def __init__(self, job_name=None, savepoint_path=None):    
        
        checkpoint_interval = os.getenv("CHECKPOINT_INTERVAL")

        config = Configuration()
        config.set_string("execution.checkpointing.interval", checkpoint_interval)
        config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
        if savepoint_path:
            config.set_string("execution.savepoint.path", savepoint_path)
       
        env_settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode() \
            .with_configuration(config) \
            .build()
            
        self.t_env = TableEnvironment.create(env_settings)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.job_name = job_name

    def _inject_env_vars(self, sql_text):
        """Automatically finds {ANY_VAR} and replaces it with os environment variables."""
        pattern = re.compile(r'\{([A-Za-z0-9_]+)\}')
        
        def replacer(match):
            var_name = match.group(1)
            var_value = os.getenv(var_name)
            if var_value is None:
                raise ValueError(f"FATAL ERROR: SQL requires '{{{var_name}}}', but no environment variable was found!")
            return var_value
            
        return pattern.sub(replacer, sql_text)

    def execute_sql_file(self, filename):
        """Reads, compiles, and executes a single SQL file."""
        sql_file_path = os.path.join(self.base_dir, filename)
        print(f"\n--- Compiling: {filename} ---")
        
        try:
            with open(sql_file_path, 'r') as file:
                raw_sql = file.read()
        except FileNotFoundError:
            print(f"Error: Could not find SQL file at {sql_file_path}")
            return

        if self.job_name:
            self.t_env.get_config().set("pipeline.name", self.job_name)

        compiled_sql = self._inject_env_vars(raw_sql)
        commands = []
        buffer = []
        in_statement_set = False
        
        raw_splits = compiled_sql.split(';')
        
        for raw_cmd in raw_splits:
            cmd = raw_cmd.strip()
            if not cmd:
                continue
                
            buffer.append(cmd)
            

            if re.search(r'EXECUTE\s+STATEMENT\s+SET', cmd, re.IGNORECASE):
                in_statement_set = True
            
            if in_statement_set and re.search(r'\bEND\b', cmd, re.IGNORECASE):
                in_statement_set = False
            

            if not in_statement_set:
                final_command = ';\n'.join(buffer)
                
            
                if "EXECUTE STATEMENT SET" in final_command.upper():
                    final_command += ";"
                    
                commands.append(final_command)
                buffer = []

        for command in commands:

            preview = command[:80].replace('\n', ' ') + "..."
            print(f"\nExecuting: {preview}")
            
            try:

                result = self.t_env.execute_sql(command)
                

                if command.upper().startswith("SELECT"):
                    print("Streaming Results:")
                    result.print()
                else:

                    job_client = result.get_job_client()
                    
                    if job_client is not None:

                        job_id = job_client.get_job_id()
                        print("✅ SUCCESS: Job submitted to cluster!")
                        print(f"🔗 Job ID: {job_id}")
                    else:

                        print("✅ Executed successfully (Metadata/Catalog operation).")
                        
            except Exception as e:
                print("❌ ERROR executing command:")
                print(e)


def get_latest_savepoint(job_name: str) -> str:
    """
    Scans the specific job's savepoint directory and returns the exact path
    to the most recently created Flink savepoint folder.
    """
    base_dir = f"/opt/flink/data/savepoints/{job_name}"

    if not os.path.exists(base_dir):
        return None

    valid_savepoints = []

    for item in os.listdir(base_dir):
        full_path = os.path.join(base_dir, item)
        if os.path.isdir(full_path) and item.startswith("savepoint-"):
            valid_savepoints.append(full_path)

    if not valid_savepoints:
        return None

    latest_savepoint = max(valid_savepoints, key=os.path.getmtime)

    return f"file://{latest_savepoint}"
if __name__ == '__main__':
    stream_seperator_job = FlinkJobExecutor(job_name = "Stream Separator", savepoint_path = get_latest_savepoint("Stream Separator"))
    clean_position_report_job = FlinkJobExecutor(job_name= "Clean Position Report", savepoint_path = get_latest_savepoint("Clean Position Report"))
    clean_shipstatic_metadata_job = FlinkJobExecutor(job_name= "Clean Shipstatic Metadata", savepoint_path = get_latest_savepoint("Clean Shipstatic Metadata"))
    clean_basestation_report_job = FlinkJobExecutor(job_name= "Clean Basestation Report", savepoint_path = get_latest_savepoint("Clean Basestation Report"))
    iceberg_ingestor_positionreport_job = FlinkJobExecutor(job_name = "Iceberg Ingestor Position Report", savepoint_path = get_latest_savepoint("Iceberg Ingestor Position Report"))
    iceberg_ingestor_shipstatic_job = FlinkJobExecutor(job_name = "Iceberg Ingestor Shipstatic", savepoint_path = get_latest_savepoint("Iceberg Ingestor Shipstatic"))
    iceberg_ingestor_basestation_job = FlinkJobExecutor(job_name = "Iceberg Ingestor Basestation", savepoint_path = get_latest_savepoint("Iceberg Ingestor Basestation"))
    presentation_live_ships = FlinkJobExecutor(job_name = "Presentation Live Ships", savepoint_path = get_latest_savepoint("Presentation Live Ships"))
    presentation_basestations = FlinkJobExecutor(job_name = "Presentation Basestations", savepoint_path = get_latest_savepoint("Presentation Basestations"))

    stream_seperator_job.execute_sql_file("stream_seperator.sql")
    clean_position_report_job.execute_sql_file("look_up_def.sql")
    clean_position_report_job.execute_sql_file("clean_position_report.sql")
    clean_shipstatic_metadata_job.execute_sql_file("look_up_def.sql")
    clean_shipstatic_metadata_job.execute_sql_file("clean_shipstatic_metadata.sql")
    clean_basestation_report_job.execute_sql_file("look_up_def.sql")
    clean_basestation_report_job.execute_sql_file("clean_basestation_report.sql")
    iceberg_ingestor_positionreport_job.execute_sql_file("iceberg_ingestor_positionreport.sql")
    iceberg_ingestor_shipstatic_job.execute_sql_file("iceberg_ingestor_shipstatic.sql")
    iceberg_ingestor_basestation_job.execute_sql_file("iceberg_ingestor_basestation.sql")
    presentation_live_ships.execute_sql_file("presentation_live_ships.sql")
    presentation_basestations.execute_sql_file("presentation_basestations.sql")
    

    while True:
        try:
            response = requests.get(FLINK_API).json()
            
            active_jobs = [
                job for job in response.get('jobs', []) 
                if job['status'] in ('RUNNING', 'CREATED', 'RESTARTING')
            ]
            
            if not active_jobs:
                print("✅ All jobs have been paused or finished.")
                print("Shutting down orchestrator and tearing down the Application Mode cluster...")
                break 
                
        except requests.exceptions.RequestException:
            pass
            
        time.sleep(10)