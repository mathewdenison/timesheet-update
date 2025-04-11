import os
import json
import logging
import base64
import copy

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import shared code from your published package.
from timesheet_common_timesheet_mfdenison_hopkinsep.utils.timelog_update_manager import TimeLogUpdateManager
from timesheet_common_timesheet_mfdenison_hopkinsep.utils.dashboard import send_dashboard_update
from timesheet_common_timesheet_mfdenison_hopkinsep.serializers import TimeLogSerializer

# Initialize Cloud Logging (logs will go to Google Cloud Logging)
client = cloud_logging.Client()
client.setup_logging()

# Standard logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a Pub/Sub publisher client.
publisher = pubsub_v1.PublisherClient()
project_id = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
pto_deduction_queue = f"projects/{project_id}/topics/pto_deduction_queue"

def timelog_update_handler(event, context):
    """
    Cloud Function triggered by a Pub/Sub message to update a TimeLog record.

    Expects a JSON payload (possibly double-encoded) with a structure similar to:
       {
         "timelog_id": "<ID>",
         "data": {
             "employee": <...>,    // or "employee_id", which will be ensured
             "pto_hours": <...>,
             ... (other fields)
         }
       }

    The function:
      - Decodes and parses the message.
      - Patches the data (renames "employee" to "employee_id" if necessary).
      - Instantiates TimeLogUpdateManager to update the record.
      - Sends a PTO deduction message if applicable.
      - Calls send_dashboard_update for a dashboard refresh.

    If the function completes without error, the Pub/Sub message is acknowledged.
    """
    update_data = None
    timelog_id = None
    try:
        # Decode the incoming Pub/Sub message. (The data is base64-encoded.)
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        first_pass = json.loads(raw_data)
        update_data = json.loads(first_pass) if isinstance(first_pass, str) else first_pass

        timelog_id = update_data.get("timelog_id")
        logger.info(f"Received update for TimeLog ID: {timelog_id}")

        # Fix the field name if necessary.
        if "data" in update_data and "employee" in update_data["data"]:
            update_data["data"]["employee_id"] = update_data["data"].pop("employee")
            logger.info("Patched 'employee' to 'employee_id' in update data.")

        # Instantiate and run the update manager.
        updater = TimeLogUpdateManager(timelog_id, update_data["data"])
        result = updater.update_timelog()

        if result["result"] == "success":
            logger.info(f"TimeLog {timelog_id} successfully updated.")
        else:
            logger.warning(f"Failed to update TimeLog {timelog_id}. Reason: {result['message']}")

    except Exception as e:
        logger.exception(f"Exception updating TimeLog {timelog_id if timelog_id else 'unknown'}: {str(e)}")
        raise

    finally:
        try:
            # Retrieve employee_id from the update data.
            employee_id = (update_data.get("data", {}).get("employee_id")
                           if isinstance(update_data, dict) else None)

            # If there are PTO hours to deduct, and the value is greater than 0, publish a PTO deduction message.
            const_pto = update_data.get("data", {}).get("pto_hours")
            if const_pto is not None and float(const_pto) > 0:
                pto_deduction_payload = {
                    "employee_id": employee_id,
                    "pto_deduction": const_pto
                }
                publisher.publish(
                    pto_deduction_queue,
                    data=json.dumps(pto_deduction_payload).encode("utf-8")
                )
                logger.info("Published PTO deduction payload: %s", pto_deduction_payload)

            # Send a dashboard refresh message regardless of PTO deduction.
            if employee_id:
                send_dashboard_update(
                    employee_id,
                    "refresh_data",
                    "Time log updated, please refresh dashboard data."
                )
                logger.info("Dashboard refresh sent for employee_id=%s", employee_id)
            else:
                logger.warning("Missing employee_id in update data: %s", update_data)
        except Exception as dashboard_e:
            logger.warning("Dashboard update failed: %s", str(dashboard_e))
