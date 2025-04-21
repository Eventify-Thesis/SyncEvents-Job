# Upload Events Job

This repository contains a script to upload events to the cloud. It is designed to be run as a scheduled job (e.g., via cron or Task Scheduler).

## Setup
1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Run the job:
   ```sh
   python run_upload_events.py
   ```

## Automation
- Use cron (Linux) or Task Scheduler (Windows) to run `python run_upload_events.py` daily.

## Project Structure
- `upload_events/`: Main logic
- `run_upload_events.py`: Entry point
- `tests/`: Unit tests
