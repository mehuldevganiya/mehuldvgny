import os
import re
import argparse
import json

CRON_DAY_TO_AUTOSYS = {
    '0': 'su', '1': 'mo', '2': 'tu', '3': 'we',
    '4': 'th', '5': 'fr', '6': 'sa', '7': 'su'
}

TIMESTAMP = "`date +%d-%m-%y.%H:%M`"

def expand_day_range(expr):
    if ',' in expr:
        parts = expr.split(',')
    else:
        parts = [expr]

    days = []
    for part in parts:
        if '-' in part:
            start, end = map(int, part.split('-'))
            days.extend(range(start, end + 1))
        else:
            days.append(int(part))
    return sorted(set(days))

def cron_days_to_autosys(dow_field):
    if dow_field == '*':
        return 'su,mo,tu,we,th,fr,sa'

    day_nums = expand_day_range(dow_field)
    return ','.join(CRON_DAY_TO_AUTOSYS[str(d % 7)] for d in day_nums)

def parse_cron_line(line):
    parts = line.strip().split(None, 5)
    if len(parts) < 6:
        return None

    minute, hour, _, _, dow_field = parts[:5]
    schedule = f"{int(hour):02d}:{int(minute):02d}"
    days_of_week = cron_days_to_autosys(dow_field)

    command_and_redirect = parts[5]
    match = re.match(r'(.*?)(?:\s*>\s*(\S+))?$', command_and_redirect)
    if match:
        command = match.group(1).strip()
        redirection = match.group(2) if match.group(2) else None
    else:
        command = command_and_redirect
        redirection = None

    return schedule, command, redirection, days_of_week

def extract_folder_and_script(command):
    script_path = command.split()[0]
    parts = script_path.strip('/').split('/')
    folder = parts[-2]
    script = os.path.basename(script_path).replace('.ksh', '')
    return folder.upper(), script

def generate_job_name(folder, script):
    return f"IBIT_THOR_1861_BATCH_{folder}_{script}_P"

def generate_jil(job_name, command, time, log_path, days_of_week, config):
    stdout = f"{log_path}.{TIMESTAMP}.log" if log_path else f"/tmp/{job_name}.{TIMESTAMP}.log"
    stderr = stdout.replace(".log", ".err")

    return f"""\
/* ----------------- {job_name} ----------------- */
/* To insert JIL for the first time to Autosys server use insert_job instead of update_job */

insert_job: {job_name}
job_type: CMD
command: "{command}"
machine: {config['machine']}
owner: {config['owner']}
permission: {config['permission']}
date_conditions: 1
days_of_week: {days_of_week}
start_times: {time}
description: "{config['description']}"
std_out_file: "{stdout}"
std_err_file: "{stderr}"
alarm_if_fail: 1
alarm_if_terminated: 1
timezone: {config['timezone']}
group: {config['group']}
application: {config['application']}
"""

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description="Convert cron jobs to AutoSys JILs.")
    parser.add_argument("cron_file", help="Input cronjob.txt file")
    parser.add_argument("config_file", help="JSON file with JIL metadata config")

    args = parser.parse_args()
    config = load_config(args.config_file)

    with open(args.cron_file, 'r') as file:
        lines = [line.strip() for line in file if line.strip()]

    for line in lines:
        parsed = parse_cron_line(line)
        if not parsed:
            continue

        time, command, log_path, days_of_week = parsed
        folder, script = extract_folder_and_script(command)
        job_name = generate_job_name(folder, script)
        jil = generate_jil(job_name, command, time, log_path, days_of_week, config)

        output_filename = f"{job_name}.jil"
        with open(output_filename, 'w') as out_file:
            out_file.write(jil)

        print(f"[✓] Created: {output_filename}")

if __name__ == '__main__':
    main()



{
  "machine": "xpb5f353186wch2.ubslcloud-prod.msad.ubs.net",
  "owner": "svc_agv thorprod",
  "permission": "gx,mx",
  "timezone": "Europe/Zurich",
  "group": "DP01",
  "application": "1861_APP_AT5134",
  "description": "THOR job to run DAO REPORTS feed in PROD. Reach out to DL.TS-ASR-IB-FX-Derivs for any issues."
}


start_mins = ",".join(f"{m:02}" for m in parse_cron_field(minute, 0, 59))
run_window = convert_to_run_window(hour)

def parse_cron_field(field, min_val, max_val):
    """Parses cron fields like '0-3,5,7-8' to a sorted set of integers."""
    result = set()
    for part in field.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            result.update(range(start, end + 1))
        else:
            result.add(int(part))
    return sorted(v for v in result if min_val <= v <= max_val)


def convert_to_run_window(hour_field):
    ranges = []
    for part in hour_field.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            ranges.append(f"{start:02}:00-{end:02}:59")
        else:
            hour = int(part)
            ranges.append(f"{hour:02}:00-{hour:02}:59")
    return ",".join(ranges)



