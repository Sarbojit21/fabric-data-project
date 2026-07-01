# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# =============================================================================
# 
# Calls UltiPro RaaS SOAP API day by day and saves raw XML to Bronze Lakehouse.
#
# Key design decisions based on SSIS C# script analysis:
#   1. ReportKey CAN be polled multiple times — key stays valid while Working
#   2. Use Status field (Working/Success/Failed) not byte count to detect readiness
#   3. Use ReportRetrievalUri from ExecuteReport response not hardcoded URL
#   4. Re-authenticate per date — tokens expire quickly
#   5. No fixed warmup — poll every 20s from the start, just like SSIS tight loop
# =============================================================================


# ── CELL 1 ─ Parameters  (toggle as Parameter cell in Fabric) ─────────────────
#EXTRACT_TYPE      = "Incremental"   # "Full" | "Incremental" | "Test"
#START_DATE_PARAM  = "2026-05-01"              # yyyy-MM-dd  blank = auto-calculated
#END_DATE_PARAM    = "2026-05-03"              # yyyy-MM-dd  blank = auto-calculated
BEFORE_MONTH      = 1               # months back from today (incremental only)
#FULL_LOAD_START   = "2024-01-01"
#FULL_LOAD_END     = "2026-05-25"


# ── CELL 2 ─ Configuration ────────────────────────────────────────────────────
# TODO: move credentials to Azure Key Vault once confirmed working
API_BASE_URL     = "https://service5.ultipro.com"   
API_USERNAME     = "svc_sgs_ssbi_raas"
API_PASSWORD     = "jL(6kJEoek2p*@T_$zFl%beD_?Fx2!YzE8mB6D-K1aG-+N%5rn"
API_CUSTOMER_KEY = "T4DJB"
API_USER_KEY     = "DOAUSC000010"

REPORT_PATH = (
    "/content/folder[@name='zzzCompany Folders']"
    "/folder[@name='SGS International, LLC']"
    "/folder[@name='UltiPro']"
    "/folder[@name='Reports As a Service_VCM']"
    "/report[@name='VCM: Census Report_Point in Time_v2 (with currency fix)']"
)

BRONZE_FOLDER = "Files/Ultipro/RAAS API"
BRONZE_LAKEHOUSE_NAME = "BRONZE"

# ── Polling config ────────────────────────────────────────────────────────────
# Mirrors SSIS do-while loop — poll every N seconds until Status == Success.
# No warmup needed — the key stays valid across multiple RetrieveReport calls
# (confirmed from SSIS C# which reuses the same key until Working → Success).
POLL_INTERVAL_SECS = 20     # poll every 20s — matches SSIS tight loop
POLL_TIMEOUT_SECS  = 1200   # 20 min — covers worst case days
MAX_DATE_RETRIES   = 3      # retry whole date up to 3 times on any error
RETRY_WAIT_SECS    = 120    # wait 2 min between date-level retries



# ── CELL 3 ─ Imports ──────────────────────────────────────────────────────────
import requests, time, logging
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from xml.etree import ElementTree as ET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("CensusExtract")


# ── CELL 4 ─ Date range ───────────────────────────────────────────────────────
def calc_date_range(extract_type, start_param, end_param,
                    before_month, full_start, full_end):
    today = date.today()
    if extract_type.strip().lower() == "full":
        return (
            date.fromisoformat(full_start),
            date.fromisoformat(full_end),
        )
    start = (
        date.fromisoformat(start_param)
        if start_param
        else today - timedelta(days=30)   
    )
    end = (
        date.fromisoformat(end_param)
        if end_param
        else today + timedelta(days=1)
    )
    return start, end


start_date, end_date = calc_date_range(
    EXTRACT_TYPE, START_DATE_PARAM, END_DATE_PARAM,
    BEFORE_MONTH, FULL_LOAD_START, FULL_LOAD_END,
)
log.info(f"Extract type : {EXTRACT_TYPE}")
log.info(f"Date range   : {start_date} → {end_date}  ({(end_date - start_date).days} days)")


# ── CELL 5 ─ SOAP helpers ─────────────────────────────────────────────────────
NS_SOAP   = "http://www.w3.org/2003/05/soap-envelope"
NS_BI     = "http://www.ultipro.com/dataservices/bidata/2"
NS_STREAM = "http://www.ultipro.com/dataservices/bistream/2"
NS_ADDR   = "http://www.w3.org/2005/08/addressing"

LOGON_ACTION    = f"{NS_BI}/IBIDataService/LogOn"
EXECUTE_ACTION  = f"{NS_BI}/IBIDataService/ExecuteReport"
RETRIEVE_ACTION = f"{NS_STREAM}/IBIStreamService/RetrieveReport"
BIDATA_URL      = f"{API_BASE_URL}/services/BIDataService"


def soap_post(url, action, body_xml, extra_headers=None, timeout=300):
    headers = {
        "Content-Type": f'application/soap+xml;charset=UTF-8;action="{action}"',
        **(extra_headers or {}),
    }
    resp = requests.post(
        url, data=body_xml.encode("utf-8"),
        headers=headers, timeout=timeout
    )
    resp.raise_for_status()
    return resp.text


def extract_tag(xml_text, tag):
    """Pull a single tag value out of an XML string."""
    open_tag, close_tag = f"<{tag}>", f"</{tag}>"
    s = xml_text.find(open_tag)
    if s == -1:
        return None
    s += len(open_tag)
    e = xml_text.find(close_tag, s)
    return xml_text[s:e] if e != -1 else None


def logon():
    body = f"""<soap:Envelope xmlns:soap="{NS_SOAP}" xmlns:ns="{NS_BI}">
  <soap:Header xmlns:wsa="{NS_ADDR}">
    <wsa:Action>{LOGON_ACTION}</wsa:Action>
  </soap:Header>
  <soap:Body>
    <ns:LogOn>
      <ns:logOnRequest>
        <ns:UserName>{API_USERNAME}</ns:UserName>
        <ns:Password>{API_PASSWORD}</ns:Password>
        <ns:ClientAccessKey>{API_CUSTOMER_KEY}</ns:ClientAccessKey>
        <ns:UserAccessKey>{API_USER_KEY}</ns:UserAccessKey>
      </ns:logOnRequest>
    </ns:LogOn>
  </soap:Body>
</soap:Envelope>"""
    resp = soap_post(BIDATA_URL, LOGON_ACTION, body)
    session = {
        "Token":       extract_tag(resp, "Token"),
        "InstanceKey": extract_tag(resp, "InstanceKey"),
        "ServiceId":   extract_tag(resp, "ServiceId"),
    }
    if not all(session.values()):
        raise RuntimeError(f"Logon failed.\nResponse:\n{resp[:600]}")
    return session


def execute_report(token, instance_key, service_id, pit_date: str):
    """
    Submit report request. Returns (ReportKey, ReportRetrievalUri).
    ReportRetrievalUri is the dynamic endpoint to poll — from SSIS:
        streamClient = CreateBIStreamServiceClientInstance(response.ReportRetrievalUri)
    """
    body = f"""<soap:Envelope xmlns:soap="{NS_SOAP}" xmlns:ns="{NS_BI}">
  <soap:Header xmlns:wsa="{NS_ADDR}">
    <wsa:Action>{EXECUTE_ACTION}</wsa:Action>
  </soap:Header>
  <soap:Body>
    <ns:ExecuteReport>
      <ns:request>
        <ns:ReportPath>{REPORT_PATH}</ns:ReportPath>
        <ns:ReportParameters>
          <ns:ReportParameter>
            <ns:Name>Point In Time Date</ns:Name>
            <ns:Value>{pit_date}T00:00:00</ns:Value>
            <ns:DataType>xsdDateTime</ns:DataType>
          </ns:ReportParameter>
        </ns:ReportParameters>
      </ns:request>
      <ns:context>
        <ns:ServiceId>{service_id}</ns:ServiceId>
        <ns:ClientAccessKey>{API_CUSTOMER_KEY}</ns:ClientAccessKey>
        <ns:Token>{token}</ns:Token>
        <ns:StatusMessage xsi:nil="true"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
        <ns:InstanceKey>{instance_key}</ns:InstanceKey>
      </ns:context>
    </ns:ExecuteReport>
  </soap:Body>
</soap:Envelope>"""
    resp = soap_post(
        BIDATA_URL, EXECUTE_ACTION, body,
        extra_headers={"US-DELIMITER": ","}
    )

    report_key     = extract_tag(resp, "ReportKey")
    retrieval_uri  = extract_tag(resp, "ReportRetrievalUri")
    status         = extract_tag(resp, "Status")

    if not report_key:
        raise RuntimeError(
            f"No ReportKey returned for {pit_date}.\n"
            f"Status: {status}\nResponse:\n{resp[:500]}"
        )

    log.info(f"  ReportKey     : {report_key}")
    log.info(f"  RetrievalUri  : {retrieval_uri}")
    log.info(f"  Status        : {status}")

    return report_key, retrieval_uri


def retrieve_report(report_key: str, retrieval_uri: str) -> str:
    """
    Call RetrieveReport using the dynamic URI from ExecuteReport.
    This mirrors SSIS: streamClient = new BIStreamServiceClient(response.ReportRetrievalUri)
    The same ReportKey can be called multiple times while status is Working.
    """
    body = f"""<soap:Envelope xmlns:soap="{NS_SOAP}" xmlns:ns="{NS_STREAM}">
  <soap:Header xmlns:wsa="{NS_ADDR}">
    <ns:ReportKey>{report_key}</ns:ReportKey>
    <wsa:Action>{RETRIEVE_ACTION}</wsa:Action>
  </soap:Header>
  <soap:Body>
    <ns:RetrieveReportRequest/>
  </soap:Body>
</soap:Envelope>"""
    return soap_post(retrieval_uri, RETRIEVE_ACTION, body, timeout=300)




def get_report_status(xml_response: str) -> str:
    """
    Extract Status handling namespace prefixes like h:Status, ns:Status etc.
    UltiPro returns <h:Status xmlns:h="...">Working</h:Status> not plain <Status>.
    Also handles Completed as a success value (confirmed from actual response).
    """
    # Search for any tag ending in Status — handles h:Status, ns:Status, Status
    import re
    match = re.search(r'<[^>]*:?Status[^>]*>([^<]+)</[^>]*:?Status>', xml_response)
    if match:
        status = match.group(1).strip()
        # Normalise — API returns Completed, SSIS SDK returns Success
        if status in ("Completed", "Success"):
            return "Completed"
        return status   # Working, Failed, etc.

    # Fallback — if ReportStream has content it's ready
    try:
        root = ET.fromstring(xml_response)
        el   = root.find(
            f".//{{{NS_STREAM}}}StreamReportResponse"
            f"/{{{NS_STREAM}}}ReportStream"
        )
        if el is not None and (el.text or "").strip():
            return "Completed"
    except Exception:
        pass

    return "Unknown"

 


# ── CELL 6 ─ Polling loop ─────────────────────────────────────────────────────
def wait_for_report(report_key: str, retrieval_uri: str, pit_str: str) -> str:
    """
    Mirrors SSIS do-while loop exactly:
        do {
            status = streamClient.RetrieveReport(response.ReportKey, ...)
        } while (status == ReportResponseStatus.Working)

    Key insight from SSIS code:
        - Same ReportKey polled multiple times — key valid while Working
        - No warmup sleep needed — poll immediately and keep going
        - Stop only when Success or Failed

    Each poll call has a 5 min HTTP timeout so on slow networks the call
    itself can block for a while before returning Working — that's fine.
    """
    deadline = time.monotonic() + POLL_TIMEOUT_SECS
    attempt  = 0

    while True:
        attempt += 1
        elapsed = POLL_TIMEOUT_SECS - (deadline - time.monotonic())

        log.info(
            f"  [{pit_str}] poll #{attempt}  {elapsed:.0f}s elapsed  "
            f"→ calling RetrieveReport …"
        )

        try:
            xml_content = retrieve_report(report_key, retrieval_uri)
        except requests.exceptions.Timeout:
            # Timeout on the call itself — log and retry
            log.warning(f"  [{pit_str}] RetrieveReport timed out, retrying …")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"[{pit_str}] overall timeout exceeded")
            time.sleep(POLL_INTERVAL_SECS)
            continue

        status = get_report_status(xml_content)
        size   = len(xml_content.encode("utf-8"))

        log.info(
            f"  [{pit_str}] status={status}  {size:,} bytes"
        )

        if status == "Completed":
            log.info(f"  [{pit_str}] ✓ ready at {elapsed:.0f}s elapsed")
            return xml_content

        if status == "Failed":
            msg = extract_tag(xml_content, "StatusMessage") or "no message"
            raise RuntimeError(f"[{pit_str}] API returned Failed: {msg}")

        # Working or Unknown — log a snippet to help diagnose if needed
        if status not in ("Working",):
            log.warning(
                f"  [{pit_str}] unexpected status '{status}' — "
                f"snippet: {xml_content[200:400]}"
            )

        # Check timeout before sleeping
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"[{pit_str}] not ready after {POLL_TIMEOUT_SECS}s "
                f"({attempt} polls). Last status: {status}"
            )

        time.sleep(POLL_INTERVAL_SECS)


# ── CELL 7 ─ Lakehouse path + Bronze write helper ─────────────────────────────
def get_bronze_files_root():
    """
    Resolve full abfss:// path for Bronze lakehouse.
    Avoids mssparkutils.fs.put failures caused by spaces in folder names
    when using relative paths — RAAS API space gets URL-encoded to RAAS%20API.
    """
    lh   = mssparkutils.lakehouse.get(BRONZE_LAKEHOUSE_NAME)
    return (
        f"abfss://{lh.workspaceId}"
        f"@onelake.dfs.fabric.microsoft.com"
        f"/{lh.id}/Files/Ultipro/RAAS API"
    )

BRONZE_ROOT = get_bronze_files_root()
log.info(f"Bronze root: {BRONZE_ROOT}")


def save_to_bronze(xml_content: str, filename: str):
    path = f"{BRONZE_ROOT}/{filename}"
    mssparkutils.fs.put(path, xml_content, overwrite=True)
    log.info(f"  Bronze ← {filename}  ({len(xml_content):,} chars)")
# ── CELL 8 ─ Extract loop ─────────────────────────────────────────────────────
def extract_one_date(pit_str: str, filename: str) -> bool:
    """
    Attempt to extract a single date. Returns True on success, False on failure.
    Retries the full logon → execute → retrieve cycle up to MAX_DATE_RETRIES times.
    Handles transient errors: Cognos runtime errors, 502s, timeouts.
    """
    for attempt in range(1, MAX_DATE_RETRIES + 1):
        try:
            if attempt > 1:
                log.info(f"  [{pit_str}] retry attempt {attempt}/{MAX_DATE_RETRIES} "
                         f"— waiting {RETRY_WAIT_SECS}s first …")
                time.sleep(RETRY_WAIT_SECS)

            # Fresh auth every attempt — Cognos errors often clear after a pause
            session = logon()
            log.info(f"  [{pit_str}] logged on  ServiceId={session['ServiceId']}")

            t_start    = time.monotonic()
            report_key, retrieval_uri = execute_report(
                session["Token"],
                session["InstanceKey"],
                session["ServiceId"],
                pit_str,
            )

            xml_content = wait_for_report(report_key, retrieval_uri, pit_str)
            log.info(f"  [{pit_str}] total time: {time.monotonic() - t_start:.0f}s")

            save_to_bronze(xml_content, filename)
            return True   # success

        except TimeoutError as te:
            log.error(f"  [{pit_str}] attempt {attempt} timed out: {te}")
        except Exception as exc:
            log.error(f"  [{pit_str}] attempt {attempt} failed: {exc}")

    log.error(f"  [{pit_str}] all {MAX_DATE_RETRIES} attempts failed — skipping")
    return False


# ── Main loop ─────────────────────────────────────────────────────────────────
failed_dates = []
current      = start_date

# Process end_date - 1 day last so it gets the freshest attempt
# Business requirement: latest date must always succeed
all_dates = []
while current < end_date:
    all_dates.append(current)
    current += timedelta(days=1)

# Put the last date at the end of the list so if anything is skipped
# it's historical dates, not the most recent one
# (list is already chronological so this is just making the priority explicit)
log.info(f"Processing {len(all_dates)} dates. "
         f"Most recent: {all_dates[-1] if all_dates else 'none'}")

for d in all_dates:
    pit_str  = d.strftime("%Y-%m-%d")
    filename = f"CensusDaily_{d.strftime('%Y%m%d')}.xml"
    log.info(f"════ {pit_str} ════")

    success = extract_one_date(pit_str, filename)
    if not success:
        failed_dates.append(pit_str)

# ── Final retry pass for failed dates, end date first ────────────────────────
if failed_dates:
    log.info(f"Retrying {len(failed_dates)} failed dates …")

    # Sort so end_date - 1 (most recent) is retried first
    retry_order = sorted(failed_dates, reverse=True)
    still_failed = []

    for pit_str in retry_order:
        d        = datetime.strptime(pit_str, "%Y-%m-%d").date()
        filename = f"CensusDaily_{d.strftime('%Y%m%d')}.xml"
        log.info(f"════ RETRY {pit_str} ════")

        success = extract_one_date(pit_str, filename)
        if not success:
            still_failed.append(pit_str)

    failed_dates = still_failed

# ── Summary — never raise, just log ──────────────────────────────────────────
most_recent = all_dates[-1].strftime("%Y-%m-%d") if all_dates else "none"
end_date_failed = most_recent in failed_dates

log.info(f"Extract complete.")
log.info(f"Total dates    : {len(all_dates)}")
log.info(f"Succeeded      : {len(all_dates) - len(failed_dates)}")
log.info(f"Failed         : {len(failed_dates)}")
log.info(f"Most recent ({most_recent}): {'✓ SUCCESS' if not end_date_failed else '✗ FAILED'}")

if failed_dates:
    log.warning(f"Failed dates: {failed_dates}")
    if end_date_failed:
        log.error(f"CRITICAL: Most recent date {most_recent} failed to extract")

# Do NOT raise — let the notebook exit cleanly so Bronze→Silver still runs
# The failed dates will simply be missing from Bronze
# Pipeline can check the exit value or logs for failures
mssparkutils.notebook.exit(
    "SUCCESS" if not end_date_failed else f"PARTIAL_FAIL:{','.join(failed_dates)}"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
