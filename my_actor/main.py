import asyncio
import logging
import re
import time

from datetime import date, datetime, timezone
from urllib.parse import urlparse

from apify import Actor


log = logging.getLogger(__name__)


# =====================================
# EXISTING APIFY ACTORS
# =====================================

SCRAPER_ACTOR = "apify/facebook-posts-scraper"
SCREENSHOT_ACTOR = "hpbt/website-screenshot-pdf"
PDF_ACTOR = "hironewf/facebook-png-to-pdf"


# =====================================
# SCRAPER SAFETY SETTINGS
# =====================================

# Stop after this many consecutive block
# warnings without additional posts.
MAX_BLOCK_WARNINGS = 3

# Maximum Facebook scraper runtime.
SCRAPER_TIMEOUT_SECONDS = 300

# How often to check scraper progress.
POLL_INTERVAL_SECONDS = 5


# =====================================
# FACEBOOK SCREENSHOT CSS
# =====================================

CUSTOM_CSS = """
/* Hide Facebook login popup */
[role="dialog"]:has(input[type="password"]) {
    display: none !important;
}

/* Hide translucent backdrop */
div:has(> div:first-child:empty):has(#login_popup_cta_form) > div:first-child:empty {
    display: none !important;
}

/* Remove additional overlay tint */
:root {
    --always-light-overlay: transparent !important;
    --always-dark-overlay: transparent !important;
}
"""


# =====================================
# HELPER FUNCTIONS
# =====================================

def validate_date(value):
    """Validate an absolute YYYY-MM-DD date."""

    date_string = str(value).strip()

    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}",
        date_string,
    ):
        raise ValueError(
            "Only Posts Newer Than must use "
            "YYYY-MM-DD format."
        )

    try:
        parsed = date.fromisoformat(date_string)
    except ValueError as exc:
        raise ValueError(
            "Invalid date. Example: 2026-11-01."
        ) from exc

    return parsed.isoformat()


def get_post_url(item):
    """Extract a valid Facebook post URL."""

    if not isinstance(item, dict):
        return None

    url = item.get("url")

    if not isinstance(url, str):
        return None

    url = url.strip()

    try:
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()

        if parsed.scheme not in ("http", "https"):
            return None

        if hostname not in (
            "facebook.com",
            "www.facebook.com",
            "m.facebook.com",
            "web.facebook.com",
        ):
            return None

        if (
            "/posts/" in parsed.path
            or "/permalink/" in parsed.path
            or parsed.path.endswith("/story.php")
            or parsed.path.endswith("/permalink.php")
        ):
            return url

    except ValueError:
        return None

    return None


async def read_all_items(dataset_id):
    """Retrieve all records from an Apify dataset."""

    if not dataset_id:
        return []

    dataset = Actor.apify_client.dataset(
        dataset_id
    )

    records = []
    offset = 0

    while True:
        result = await dataset.list_items(
            offset=offset,
            limit=1000,
            clean=False,
        )

        batch = result.items or []

        records.extend(batch)
        offset += len(batch)

        if len(batch) < 1000:
            break

    return records


def extract_post_urls(records):
    """Deduplicate valid Facebook post URLs."""

    urls = []
    seen = set()

    for item in records:
        url = get_post_url(item)

        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


async def call_actor(actor_id, run_input):
    """Call a downstream Actor normally."""

    log.info("Starting Actor: %s", actor_id)

    run = await Actor.call(
        actor_id=actor_id,
        run_input=run_input,
    )

    if run is None:
        raise RuntimeError(
            f"{actor_id} returned no run."
        )

    if run.status != "SUCCEEDED":
        raise RuntimeError(
            f"{actor_id} ended with status "
            f"{run.status}. Run ID: {run.id}"
        )

    log.info(
        "%s succeeded. Run ID: %s",
        actor_id,
        run.id,
    )

    return run


# =====================================
# SCRAPER LOG MONITORING
# =====================================

def parse_scraper_events(log_text):
    """
    Extract block and progress events from
    the Facebook scraper logs.

    BLOCKED and SOFT-BLOCKED are counted
    only when they appear in scraper
    warning messages.
    """

    events = []

    for line in log_text.splitlines():

        if "CheerioCrawler:" in line and (
            "SOFT-BLOCKED" in line
            or "BLOCKED" in line
        ):
            events.append(("block", None))
            continue

        if "[PROGRESS]" in line:
            match = re.search(
                r"Found\s+(\d+)\s+new page posts",
                line,
            )

            if match:
                new_posts = int(match.group(1))

                if new_posts > 0:
                    events.append(
                        ("progress", new_posts)
                    )

    return events


async def run_monitored_scraper(scraper_input):
    """
    Start the Facebook scraper and monitor
    its logs and dataset.

    Stop when:
      - The scraper completes normally
      - 3 consecutive block warnings occur
      - 5 minutes elapse

    Preserve available dataset records.
    """

    log.info(
        "STEP 1: Starting monitored "
        "Facebook scraper"
    )

    actor_client = Actor.apify_client.actor(
        SCRAPER_ACTOR
    )

    started_run = await actor_client.start(
        run_input=scraper_input
    )

    if not started_run:
        raise RuntimeError(
            "Could not start Facebook scraper."
        )

    run_id = started_run["id"]

    run_client = Actor.apify_client.run(
        run_id
    )

    dataset_id = started_run.get(
        "defaultDatasetId"
    )

    log.info(
        "Facebook scraper started: %s",
        run_id,
    )

    start_time = time.monotonic()

    # Number of consecutive block warnings
    # since the last reported progress.
    consecutive_blocks = 0

    # Number of parsed log events already seen.
    processed_event_count = 0

    stop_reason = None
    final_status = None

    terminal_statuses = {
        "SUCCEEDED",
        "FAILED",
        "ABORTED",
        "TIMED-OUT",
    }

    while True:

        # ---------------------------------
        # CHECK CURRENT RUN STATUS
        # ---------------------------------

        run_info = await run_client.get()

        if not run_info:
            raise RuntimeError(
                "Could not retrieve scraper status."
            )

        status = run_info.get("status")

        dataset_id = (
            run_info.get("defaultDatasetId")
            or dataset_id
        )

        if status in terminal_statuses:
            final_status = status

            if status == "SUCCEEDED":
                stop_reason = "completed_normally"
            else:
                stop_reason = (
                    f"scraper_{status.lower()}"
                )

            log.info(
                "Scraper finished with status %s",
                status,
            )

            break

        # ---------------------------------
        # CHECK ELAPSED TIME
        # ---------------------------------

        elapsed = (
            time.monotonic() - start_time
        )

        if elapsed >= SCRAPER_TIMEOUT_SECONDS:
            stop_reason = "timeout"

            log.warning(
                "Scraper exceeded %s seconds. "
                "Stopping and preserving results.",
                SCRAPER_TIMEOUT_SECONDS,
            )

            break

        # ---------------------------------
        # READ SCRAPER LOGS
        # ---------------------------------

        try:
            log_text = await run_client.log().get()

            if isinstance(log_text, bytes):
                log_text = log_text.decode(
                    "utf-8",
                    errors="replace",
                )

            events = parse_scraper_events(
                log_text or ""
            )

            # Only process events we haven't
            # already counted.
            new_events = events[
                processed_event_count:
            ]

            processed_event_count = len(events)

            for event_type, value in new_events:

                if event_type == "progress":
                    consecutive_blocks = 0

                    log.info(
                        "Scraper progress: "
                        "%s additional posts reported.",
                        value,
                    )

                elif event_type == "block":
                    consecutive_blocks += 1

                    log.warning(
                        "Facebook block warning "
                        "%s/%s",
                        consecutive_blocks,
                        MAX_BLOCK_WARNINGS,
                    )

        except Exception:
            log.exception(
                "Could not inspect scraper logs. "
                "Will retry monitoring."
            )

        # ---------------------------------
        # STOP AFTER REPEATED BLOCKS
        # ---------------------------------

        if (
            consecutive_blocks
            >= MAX_BLOCK_WARNINGS
        ):
            stop_reason = "block_limit"

            log.warning(
                "Reached %s consecutive block "
                "warnings. Stopping scraper.",
                MAX_BLOCK_WARNINGS,
            )

            break

        await asyncio.sleep(
            POLL_INTERVAL_SECONDS
        )

    # =====================================
    # STOP SCRAPER IF STILL RUNNING
    # =====================================

    if stop_reason in (
        "timeout",
        "block_limit",
    ):
        try:
            current_run = await run_client.get()

            if (
                current_run
                and current_run.get("status")
                not in terminal_statuses
            ):
                log.info(
                    "Aborting scraper run %s",
                    run_id,
                )

                await run_client.abort(
                    gracefully=False
                )

        except Exception:
            log.exception(
                "Could not abort scraper run."
            )

    # =====================================
    # RETRIEVE PARTIAL RESULTS
    # =====================================

    # Allow a short interval for the final
    # run state to become available.
    try:
        current_run = await run_client.get()

        if current_run:
            final_status = current_run.get(
                "status"
            )

            dataset_id = (
                current_run.get(
                    "defaultDatasetId"
                )
                or dataset_id
            )

    except Exception:
        log.warning(
            "Could not refresh final scraper status."
        )

    if not dataset_id:
        raise RuntimeError(
            "Facebook scraper returned "
            "no dataset ID."
        )

    records = await read_all_items(
        dataset_id
    )

    post_urls = extract_post_urls(
        records
    )

    log.info(
        "Scraper stopped: reason=%s, "
        "status=%s, records=%s, URLs=%s",
        stop_reason,
        final_status,
        len(records),
        len(post_urls),
    )

    if not post_urls:
        raise RuntimeError(
            "Facebook scraper stopped without "
            "collecting any usable post URLs. "
            f"Reason: {stop_reason}. "
            f"Run ID: {run_id}"
        )

    if stop_reason != "completed_normally":
        log.warning(
            "Using %s collected posts despite "
            "scraper not completing normally.",
            len(post_urls),
        )

    return {
        "runId": run_id,
        "datasetId": dataset_id,
        "records": records,
        "postUrls": post_urls,
        "stopReason": stop_reason,
        "finalStatus": final_status,
    }


# =====================================
# MAIN WORKFLOW
# =====================================

async def main():

    print(
        "FACEBOOK WORKFLOW: Started",
        flush=True,
    )

    async with Actor:

        config = await Actor.get_input() or {}

        # =================================
        # INPUT CONFIGURATION
        # =================================

        profile_url = (
            config.get("facebookProfileUrl")
            or ""
        ).strip()

        if not profile_url:
            raise ValueError(
                "facebookProfileUrl is required."
            )

        parsed = urlparse(profile_url)

        if (
            parsed.scheme not in (
                "http",
                "https",
            )
            or (parsed.hostname or "").lower()
            not in (
                "facebook.com",
                "www.facebook.com",
                "m.facebook.com",
                "web.facebook.com",
            )
        ):
            raise ValueError(
                "Enter a valid Facebook profile URL."
            )

        results_limit = int(
            config.get("resultsLimit", 20)
        )

        only_newer_than = validate_date(
            config.get("onlyPostsNewerThan")
            or "2025-12-31"
        )

        viewport_width = int(
            config.get("viewportWidth", 800)
        )

        viewport_height = int(
            config.get("viewportHeight", 1280)
        )

        max_image_height = int(
            config.get("maxImageHeight", 1400)
        )

        page_size = (
            config.get("pageSize")
            or "LETTER"
        ).upper()

        if results_limit < 1:
            raise ValueError(
                "resultsLimit must be positive."
            )

        if (
            viewport_width < 1
            or viewport_height < 1
        ):
            raise ValueError(
                "Viewport dimensions must be positive."
            )

        if max_image_height < 0:
            raise ValueError(
                "maxImageHeight cannot be negative."
            )

        if page_size not in (
            "LETTER",
            "A4",
        ):
            raise ValueError(
                "pageSize must be LETTER or A4."
            )

        log.info(
            "Profile: %s",
            profile_url,
        )

        log.info(
            "Requested maximum posts: %s",
            results_limit,
        )

        log.info(
            "Posts newer than: %s",
            only_newer_than,
        )

        # =================================
        # STEP 1: FACEBOOK SCRAPER
        # =================================

        scraper_input = {
            "captionText": False,
            "onlyPostsNewerThan": only_newer_than,
            "resultsLimit": results_limit,
            "startUrls": [
                {
                    "url": profile_url
                }
            ],
        }

        scraper_result = (
            await run_monitored_scraper(
                scraper_input
            )
        )

        records = scraper_result["records"]

        post_urls = scraper_result[
            "postUrls"
        ]

        scraper_run_id = scraper_result[
            "runId"
        ]

        scraper_stop_reason = scraper_result[
            "stopReason"
        ]

        log.info(
            "STEP 1 COMPLETE: %s posts available.",
            len(post_urls),
        )

        await Actor.set_value(
            "POST_URLS",
            {
                "profileUrl": profile_url,
                "onlyPostsNewerThan": only_newer_than,
                "requestedMaximum": results_limit,
                "count": len(post_urls),
                "urls": post_urls,
                "scraperStopReason": (
                    scraper_stop_reason
                ),
            },
        )

        # =================================
        # STEP 2: SCREENSHOT ACTOR
        # =================================

        log.info(
            "STEP 2: Capturing %s screenshots",
            len(post_urls),
        )

        screenshot_input = {
            "customCss": CUSTOM_CSS,
            "devices": ["custom"],
            "format": "png",
            "fullPage": False,
            "hideChatWidgets": False,
            "hideCookieBanners": False,
            "hideSelectors": [],
            "pdfLandscape": False,
            "pdfPrintBackground": True,
            "proxyConfiguration": {
                "useApifyProxy": False
            },
            "scrollPage": True,
            "urls": post_urls,
            "viewportHeight": viewport_height,
            "viewportWidth": viewport_width,
        }

        screenshot_run = await call_actor(
            SCREENSHOT_ACTOR,
            screenshot_input,
        )

        screenshot_dataset_id = (
            screenshot_run.default_dataset_id
        )

        if not screenshot_dataset_id:
            raise RuntimeError(
                "Screenshot Actor returned "
                "no dataset ID."
            )

        screenshot_records = (
            await read_all_items(
                screenshot_dataset_id
            )
        )

        if not screenshot_records:
            raise RuntimeError(
                "Screenshot Actor returned "
                "an empty dataset."
            )

        log.info(
            "STEP 2 COMPLETE: "
            "%s screenshot records.",
            len(screenshot_records),
        )

        # =================================
        # STEP 3: PDF MERGER
        # =================================

        log.info(
            "STEP 3: Generating PDF"
        )

        merger_input = {
            "datasetId": screenshot_dataset_id,
            "pageSize": page_size,
            "marginPoints": 30,
            "trimWhiteEdges": False,
            "maxImageHeight": max_image_height,
        }

        merger_run = await call_actor(
            PDF_ACTOR,
            merger_input,
        )

        merger_store_id = (
            merger_run.default_key_value_store_id
        )

        if not merger_store_id:
            raise RuntimeError(
                "PDF merger returned no "
                "key-value store ID."
            )

        merger_store = (
            Actor.apify_client.key_value_store(
                merger_store_id
            )
        )

        pdf_record = (
            await merger_store.get_record(
                "facebook-posts.pdf"
            )
        )

        if not pdf_record:
            raise RuntimeError(
                "PDF merger completed but "
                "facebook-posts.pdf was not found."
            )

        pdf_bytes = pdf_record.get(
            "value"
        )

        if isinstance(
            pdf_bytes,
            bytearray,
        ):
            pdf_bytes = bytes(
                pdf_bytes
            )

        if not isinstance(
            pdf_bytes,
            bytes,
        ):
            raise RuntimeError(
                "PDF record did not contain "
                "binary PDF data."
            )

        await Actor.set_value(
            "facebook-posts.pdf",
            pdf_bytes,
            content_type="application/pdf",
        )

        merger_summary_record = (
            await merger_store.get_record(
                "SUMMARY"
            )
        )

        merger_summary = (
            merger_summary_record.get("value")
            if merger_summary_record
            else None
        )

        log.info(
            "STEP 3 COMPLETE: PDF saved."
        )

        # =================================
        # STEP 4: FINAL SUMMARY
        # =================================

        summary = {
            "status": "success",
            "facebookProfileUrl": profile_url,
            "onlyPostsNewerThan": only_newer_than,

            "requestedMaximumPosts": (
                results_limit
            ),

            "postsScraped": len(records),

            "uniquePostUrls": len(
                post_urls
            ),

            "scraperStopReason": (
                scraper_stop_reason
            ),

            "scraperFinalStatus": (
                scraper_result["finalStatus"]
            ),

            "scraperCompletedNormally": (
                scraper_stop_reason
                == "completed_normally"
            ),

            "screenshotDatasetRecords": len(
                screenshot_records
            ),

            "scraperRunId": (
                scraper_run_id
            ),

            "screenshotRunId": (
                screenshot_run.id
            ),

            "mergerRunId": (
                merger_run.id
            ),

            "pdfKey": "facebook-posts.pdf",

            "pdfSummary": merger_summary,

            "generatedAt": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        await Actor.set_value(
            "SUMMARY",
            summary,
        )

        await Actor.push_data(
            summary
        )

        log.info(
            "WORKFLOW COMPLETE: "
            "%s/%s requested posts collected. "
            "PDF generated successfully.",
            len(post_urls),
            results_limit,
        )

        print(
            "FACEBOOK WORKFLOW: Completed",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())