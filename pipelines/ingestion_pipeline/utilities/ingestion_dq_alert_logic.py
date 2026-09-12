FAILURE_RATE_ALERT_THRESHOLD = 25

def send_alert(message: str) -> None:
    """
    Stub — replace with a real Slack/PagerDuty/webhook call. Databricks' own
    documented event hook example sends to Slack via a Databricks secret-backed
    token: dbutils.secrets.get(scope=..., key=...) then a requests.post() to
    the Slack webhook URL. Kept as a stub here rather than a fake credential.
    """
    print(f"[DQ ALERT] {message}")


def check_expectations_and_alert(event:dict,pipeline_label:str):
    if event.get("event_type")!="flow_progress":
        return
    
    details=event.get("details",{})
    flow_progress=details.get("flow_progress",{})
    data_quality=flow_progress.get("data_quality",{})
    expectations=data_quality.get("expectations",[])

    for expectation in expectations:
        passed=expectation.get("passed_record",0)
        failed=expectation.get("failed_record",0)
        total=passed+failed
        if total==0: continue
        failure_rate=round(failed/total*100,2)
        
        if failure_rate>FAILURE_RATE_ALERT_THRESHOLD:
            send_alert(
                 f"[{pipeline_label}] Expectation '{exp.get('name')}' on '{exp.get('dataset')}' is "
                f"failing {failure_rate:.1f}% of rows ({failed} of {total}) — "
                f"above the {FAILURE_RATE_ALERT_THRESHOLD}% threshold."
            )










