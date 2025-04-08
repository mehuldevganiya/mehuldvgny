# mehuldvgny

import re
from datetime import datetime

CRON_FILE = 'cronjob.txt'
OUTPUT_FILE = 'output.jil'

JOB_NAME_BASE = 'IBIT_THOR_1861_BATCH_REPORTS_TM_DAO_XLS_P'
COMMAND_PREFIX = '/sbcimp/dyn/data/Thor/THOR_BATCH/REPORTS/runREPORTS_sftp.ksh'
MACHINE = 'xpb5f353186wch2.ubslcloud-prod.msad.ubs.net'
OWNER = 'svc_agv thorprod'
STDOUT_PATH = '/sbclocal/dyn/logfiles/Thor/Batch'
STDERR_PATH = '/sbclocal/dyn/logfiles/Thor/Batch'
GROUP = 'DP01'
APPLICATION = '1861_APP_AT5134'
TIMEZONE = 'Europe/Zurich'
DAYS_OF_WEEK = 'mo,tu,we,th,fr'
PERMISSION = 'gx,mx'
DESCRIPTION = 'THOR job to run DAO REPORTS feed in PROD. Reach out to DL.TS-ASR-IB-FX-Derivs for any issues.'

def cron_to_time(cron_line):
    match = re.match(r'^(\d+)\s+(\d+)', cron_line)
    if match:
        minute, hour = match.groups()
        return f"{int(hour):02d}:{int(minute):02d}"
    return None

def extract_command(cron_line):
    parts = cron_line.split(None, 5)
    return parts[5].strip() if len(parts) >= 6 else ''

def generate_jil(job_name, start_times, command):
    timestamp = "`date +%d-%m-%y.%H:%M`"
    return f"""\
/* ----------------- {job_name} ----------------- */
/* To insert JIL for the first time to Autosys server use insert_job instead of update_job */

insert_job: {job_name}
job_type: CMD
command: "{command}"
machine: {MACHINE}
owner: {OWNER}
permission: {PERMISSION}
date_conditions: 1
days_of_week: {DAYS_OF_WEEK}
start_times: {",".join(start_times)}
description: "{DESCRIPTION}"
std_out_file: "{STDOUT_PATH}/{job_name}.{timestamp}.log"
std_err_file: "{STDERR_PATH}/{job_name}.{timestamp}.err"
alarm_if_fail: 1
alarm_if_terminated: 1
timezone: {TIMEZONE}
group: {GROUP}
application: {APPLICATION}

"""

def main():
    with open(CRON_FILE, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    job_map = {}
    
    for line in lines:
        time = cron_to_time(line)
        command = extract_command(line)
        if command not in job_map:
            job_map[command] = []
        job_map[command].append(time)
    
    with open(OUTPUT_FILE, 'w') as f:
        for idx, (cmd, times) in enumerate(job_map.items(), start=1):
            job_name = JOB_NAME_BASE if len(job_map) == 1 else f"{JOB_NAME_BASE}_{idx}"
            jil = generate_jil(job_name, times, cmd)
            f.write(jil + '\n')

    print(f"JIL file written to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
