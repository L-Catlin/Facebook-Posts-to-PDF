"""Facebook posts -> photo discovery -> screenshots -> ordered PDF.

This version keeps the explicit monitoring, validation, intermediate artifacts,
logging, and partial-results recovery of the original orchestrator. It adds
photo discovery as a separate, non-fatal stage.
"""

import asyncio
import io
import httpx
from PIL import Image
import logging
import re
import time
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from apify import Actor

log = logging.getLogger(__name__)

SCRAPER_ACTOR = "apify/facebook-posts-scraper"
PHOTOS_ACTOR = "apify/facebook-photos-scraper"
SCREENSHOT_ACTOR = "hpbt/website-screenshot-pdf"
PDF_ACTOR = "hironewf/facebook-png-to-pdf"

MAX_BLOCK_WARNINGS = 3
SCRAPER_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 5

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


# ------------------------------------------------------------
# SDK / input helpers
# ------------------------------------------------------------

def field(obj, name, default=None):
    """Apify SDK run objects may be dicts or attribute-based models."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def run_field(obj, snake_name, camel_name):
    return field(obj, snake_name) or field(obj, camel_name)


def validate_date(value):
    value = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("onlyPostsNewerThan must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError("Invalid onlyPostsNewerThan date") from exc


def valid_facebook_host(host):
    return (host or "").lower() in {
        "facebook.com", "www.facebook.com", "m.facebook.com", "web.facebook.com"
    }


def validate_profile_url(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("facebookProfileUrl is required")
    url = value.strip()
    try:
        parsed = urlparse(url)
    except ValueError as exc:
        raise ValueError("Invalid Facebook profile URL") from exc
    if parsed.scheme not in ("http", "https") or not valid_facebook_host(parsed.hostname):
        raise ValueError("Enter a valid Facebook profile URL")
    return url


def canonical_post_url(url):
    """Normalize equivalent post URLs for matching Photos Scraper records.

    Preserve story.php query identifiers; strip tracking queries on /posts/ URLs.
    Do not use this function for photo-viewer URLs.
    """
    if not isinstance(url, str) or not url.strip():
        return None
    try:
        parsed = urlparse(url.strip())
        if parsed.scheme not in ("http", "https") or not valid_facebook_host(parsed.hostname):
            return None
        path = parsed.path.rstrip("/") or "/"
        is_path_post = "/posts/" in path or "/permalink/" in path
        is_query_post = path in ("/story.php", "/permalink.php")
        if not (is_path_post or is_query_post):
            return None
        query = ""
        if is_query_post:
            params = parse_qs(parsed.query)
            # Keep only identity parameters to avoid tracking-query mismatches.
            pairs = []
            for key in ("story_fbid", "fbid", "id"):
                if params.get(key):
                    pairs.append((key, params[key][0]))
            if not pairs:
                return None
            query = urlencode(pairs)
        return urlunparse(("https", "www.facebook.com", path, "", query, ""))
    except ValueError:
        return None


def post_url_from_record(item):
    if not isinstance(item, dict):
        return None
    return canonical_post_url(item.get("url"))


def photo_id(item):
    if not isinstance(item, dict):
        return None
    value = item.get("id")
    if value is None:
        return None
    value = str(value).strip()
    return value if value.isdigit() else None


def mediaset_token(item):
    if not isinstance(item, dict):
        return None
    for media in item.get("media") or []:
        if isinstance(media, dict):
            token = media.get("mediaset_token")
            if isinstance(token, str) and token.startswith("pcb."):
                return token
    return None


def photo_viewer_url(pid, token=None):
    params = {"fbid": pid}
    if token:
        params["set"] = token
    return "https://www.facebook.com/photo/?" + urlencode(params)


# ------------------------------------------------------------
# Dataset and downstream Actor helpers
# ------------------------------------------------------------

async def read_all_items(dataset_id):
    if not dataset_id:
        return []
    dataset = Actor.apify_client.dataset(dataset_id)
    records = []
    offset = 0
    while True:
        result = await dataset.list_items(offset=offset, limit=1000, clean=False)
        batch = result.items or []
        records.extend(batch)
        offset += len(batch)
        if len(batch) < 1000:
            break
    return records


async def call_actor(actor_id, run_input):
    log.info("Starting Actor %s", actor_id)
    run = await Actor.call(actor_id=actor_id, run_input=run_input)
    if run is None:
        raise RuntimeError(f"{actor_id} did not return a run")
    status = field(run, "status")
    if status != "SUCCEEDED":
        raise RuntimeError(
            f"{actor_id} ended with status {status}; Run ID: {field(run, 'id')}"
        )
    log.info("Actor %s succeeded; run=%s", actor_id, field(run, "id"))
    return run


# ------------------------------------------------------------
# Posts Scraper monitoring (preserved safeguard)
# ------------------------------------------------------------

def new_log_lines(previous_text, current_text):
    """Count only newly appended *complete* lines.

    Log retrieval normally returns cumulative text. When it is replaced or
    truncated, skip the uncertain section rather than double-count blocks.
    """
    if not current_text:
        return [], current_text
    if not current_text.startswith(previous_text):
        if previous_text:
            log.warning("Scraper log changed/truncated; resetting monitoring cursor")
            return [], current_text
    new_text = current_text[len(previous_text):]
    if not new_text:
        return [], current_text
    # Defer the final incomplete line until the next poll.
    if not new_text.endswith("\n"):
        complete, sep, _ = new_text.rpartition("\n")
        if not sep:
            return [], previous_text
        consumed = len(previous_text) + len(complete) + 1
        return complete.splitlines(), current_text[:consumed]
    return new_text.splitlines(), current_text


def inspect_scraper_log_lines(lines, block_count):
    """Reset block streak on scraper progress; otherwise count block warnings."""
    for line in lines:
        if "[PROGRESS]" in line:
            match = re.search(r"Found\s+(\d+)\s+new page posts", line)
            if match and int(match.group(1)) > 0:
                block_count = 0
                log.info("Scraper reports %s newly found posts", match.group(1))
                continue
        if "CheerioCrawler:" in line and (
            "SOFT-BLOCKED" in line or "BLOCKED" in line
        ):
            block_count += 1
            log.warning("Scraper block warning %s/%s", block_count, MAX_BLOCK_WARNINGS)
    return block_count


async def monitored_posts_scraper(scraper_input):
    """Monitor, abort stalled scraper, and recover dataset records if available."""
    log.info("STEP 1: Starting Facebook Posts Scraper")
    started = await Actor.apify_client.actor(SCRAPER_ACTOR).start(
        run_input=scraper_input
    )
    run_id = field(started, "id")
    if not run_id:
        raise RuntimeError("Posts Scraper did not return a run ID")
    run_client = Actor.apify_client.run(run_id)
    dataset_id = run_field(started, "default_dataset_id", "defaultDatasetId")
    terminal = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}
    started_at = time.monotonic()
    previous_log = ""
    consecutive_blocks = 0
    stop_reason = None
    final_status = None

    while True:
        run_info = await run_client.get()
        if not run_info:
            raise RuntimeError(f"Cannot retrieve scraper run {run_id}")
        final_status = field(run_info, "status")
        dataset_id = run_field(run_info, "default_dataset_id", "defaultDatasetId") or dataset_id

        if final_status in terminal:
            stop_reason = (
                "completed_normally" if final_status == "SUCCEEDED"
                else f"scraper_{str(final_status).lower()}"
            )
            break

        elapsed = time.monotonic() - started_at
        if elapsed >= SCRAPER_TIMEOUT_SECONDS:
            stop_reason = "timeout"
            log.warning("Posts Scraper exceeded %s seconds", SCRAPER_TIMEOUT_SECONDS)
            break

        try:
            raw = await run_client.log().get()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            lines, previous_log = new_log_lines(previous_log, raw or "")
            consecutive_blocks = inspect_scraper_log_lines(lines, consecutive_blocks)
        except Exception as exc:
            log.warning("Scraper log monitoring failed; will retry: %s", exc)

        if consecutive_blocks >= MAX_BLOCK_WARNINGS:
            stop_reason = "block_limit"
            log.warning("Stopping Posts Scraper after repeated block warnings")
            break

        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    if stop_reason in ("timeout", "block_limit"):
        try:
            latest = await run_client.get()
            if latest and field(latest, "status") not in terminal:
                await run_client.abort(gracefully=False)
                log.info("Aborted Posts Scraper run %s", run_id)
        except Exception as exc:
            log.warning("Could not abort Posts Scraper run %s: %s", run_id, exc)

    try:
        latest = await run_client.get()
        if latest:
            final_status = field(latest, "status")
            dataset_id = run_field(latest, "default_dataset_id", "defaultDatasetId") or dataset_id
    except Exception as exc:
        log.warning("Could not refresh final Posts Scraper status: %s", exc)

    if not dataset_id:
        raise RuntimeError(f"Posts Scraper returned no dataset ID; run={run_id}")
    records = await read_all_items(dataset_id)
    posts = []
    seen = set()
    for record in records:
        url = post_url_from_record(record)
        if url and url not in seen:
            seen.add(url)
            posts.append({"url": url, "raw": record, "photos": []})

    log.info(
        "STEP 1 COMPLETE: status=%s reason=%s records=%s unique posts=%s",
        final_status, stop_reason, len(records), len(posts)
    )
    if not posts:
        raise RuntimeError(
            "No usable Facebook posts were collected. "
            f"Scraper status={final_status}, reason={stop_reason}, run={run_id}"
        )
    if stop_reason != "completed_normally":
        log.warning("Continuing with %s partial posts", len(posts))
    return posts, {
        "runId": run_id,
        "datasetId": dataset_id,
        "stopReason": stop_reason,
        "finalStatus": final_status,
        "postsScraped": len(records),
        "uniquePostUrls": len(posts),
    }


# ------------------------------------------------------------
# Photos Scraper: one global-limit run, graceful empty/failure
# ------------------------------------------------------------

def direct_image_url(row):
    """Use the image URL returned by the Photos Scraper, not a Facebook viewer URL."""
    image = row.get("image")
    if isinstance(image, str) and image.startswith(("https://", "http://")):
        return image
    if isinstance(image, dict):
        for key in ("url", "uri", "src", "href"):
            value = image.get(key)
            if isinstance(value, str) and value.startswith(("https://", "http://")):
                return value
    return None


async def collect_photos(posts, photos_results_limit):
    """Discover original image URLs, retaining only photos beyond the first five.

    The first five photos are already represented in the original post screenshot.
    The Photos Scraper's resultsLimit applies to the entire run.
    """
    log.info("STEP 2: Discovering direct photos (global limit=%s)", photos_results_limit)
    by_url = {post["url"]: post for post in posts}
    discovered = {post["url"]: [] for post in posts}
    seen = {post["url"]: set() for post in posts}
    photos_run_id = None
    photos_status = "NOT_STARTED"
    photo_rows = []
    photo_error = None
    photos_input = {
        "resultsLimit": photos_results_limit,
        "startUrls": [{"url": post["url"]} for post in posts],
    }
    try:
        run = await Actor.call(actor_id=PHOTOS_ACTOR, run_input=photos_input)
        if run is None:
            raise RuntimeError("Photos Scraper returned no run")
        photos_run_id = field(run, "id")
        photos_status = field(run, "status")
        dataset_id = run_field(run, "default_dataset_id", "defaultDatasetId")
        if dataset_id:
            photo_rows = await read_all_items(dataset_id)
        if photos_status != "SUCCEEDED":
            log.warning("Photos Scraper ended with %s; using any returned photos", photos_status)
    except Exception as exc:
        photo_error = str(exc)
        photos_status = "ERROR"
        log.warning("Photos Scraper failed: %s; posts will still be included", exc)

    unmatched = 0
    missing_images = 0
    for row in photo_rows:
        if not isinstance(row, dict):
            continue
        parent = canonical_post_url(row.get("facebookUrl"))
        pid = photo_id(row)
        if parent not in by_url or not pid:
            unmatched += 1
            continue
        if row.get("dataType") == "video":
            continue
        image_url = direct_image_url(row)
        if not image_url:
            missing_images += 1
        if pid not in seen[parent]:
            seen[parent].add(pid)
            discovered[parent].append({"id": pid, "imageUrl": image_url, "photoUrl": (row.get("creation_story") or {}).get("url") or f"https://www.facebook.com/photo/?fbid={pid}"})

    skipped_previews = 0
    for post in posts:
        parent = post["url"]
        preview_ids = []
        for media in post["raw"].get("media") or []:
            if not isinstance(media, dict):
                continue
            if media.get("__typename") == "Photo" or media.get("__isMedia") == "Photo":
                pid = photo_id(media)
                if pid and pid not in preview_ids:
                    preview_ids.append(pid)
        # Prefer matching the five preview IDs exactly. If they aren't available,
        # assume the Photos Scraper returns the photos in original post order.
        if preview_ids:
            preview_set = set(preview_ids[:5])
            retained = [photo for photo in discovered[parent] if photo["id"] not in preview_set]
        else:
            retained = discovered[parent][5:]
            if discovered[parent]:
                log.warning("No preview IDs for %s; skipping first five returned photos by order", parent)
        skipped_previews += len(discovered[parent]) - len(retained)
        post["photos"] = retained
        log.info("Post %s: %s additional direct images", parent, len(retained))

    total = sum(len(post["photos"]) for post in posts)
    log.info("STEP 2 COMPLETE: rows=%s, extra images=%s, skipped previews=%s, missing image URLs=%s",
             len(photo_rows), total, skipped_previews, missing_images)
    return {
        "runId": photos_run_id,
        "status": photos_status,
        "error": photo_error,
        "resultsLimit": photos_results_limit,
        "rowsReturned": len(photo_rows),
        "unmatchedRows": unmatched,
        "missingImageUrls": missing_images,
        "previewPhotosSkipped": skipped_previews,
        "additionalPhotoPages": total,
    }


# ------------------------------------------------------------
# Manifest and screenshot stages
# ------------------------------------------------------------

async def cache_photos(posts):
    """Fetch original bytes before screenshot runs; retain failures for reporting."""
    semaphore = asyncio.Semaphore(10)
    store = await Actor.open_key_value_store()
    async with httpx.AsyncClient(timeout=httpx.Timeout(25, connect=10), follow_redirects=True) as client:
        async def fetch(post, photo):
            url = photo.get("imageUrl")
            if not url:
                photo["downloadError"] = "Photos Scraper returned no image URL"
                return
            async with semaphore:
                for attempt in range(1, 4):
                    try:
                        response = await client.get(url)
                        response.raise_for_status()
                        data = response.content
                        with Image.open(io.BytesIO(data)) as image:
                            image.load()
                        key = f"PHOTO-{photo['id']}.bin"
                        await store.set_value(key, data, content_type="application/octet-stream")
                        photo["storageKey"] = key
                        photo["storageId"] = store.id
                        return
                    except Exception as exc:
                        reason = f"{type(exc).__name__}: {str(exc).split('?')[0]}"
                        log.warning("Photo %s host=%s attempt %s/3: %s", photo['id'], urlparse(url).hostname, attempt, reason)
                        if attempt < 3:
                            await asyncio.sleep(attempt)
                photo["downloadError"] = f"After 3 attempts: {reason}"
        await asyncio.gather(*(fetch(post, photo) for post in posts for photo in post["photos"]))


def capture_log(summary, manifest):
    merger = summary.get("pdfSummary") or {}
    failures = merger.get("skipped") or []
    photos = [x for x in failures if x.get("kind") == "photo"]
    groups = {}
    for item in photos:
        groups.setdefault(item["url"], []).append(item)
    included = merger.get("included") or []
    discovery = summary.get("photosScraper") or {}
    uncertain = discovery.get("status") != "SUCCEEDED" or discovery.get("unmatchedRows", 0) or discovery.get("rowsReturned", 0) >= discovery.get("resultsLimit", float("inf"))
    lines = ["FACEBOOK REPORT — CAPTURE LOG", "", f"{len(groups)} posts with a total of {len(photos)} failed photos.", f"Other missing report entries: {len(failures)-len(photos)}", "", "Status: " + ("COMPLETED WITH KNOWN OMISSIONS OR DISCOVERY WARNINGS" if failures or uncertain else "COMPLETED — NO KNOWN CAPTURE FAILURES"), "", "Counts cover discovered report entries only; undiscovered photos cannot be counted."]
    if uncertain:
        lines += ["", f"Discovery warning: status={discovery.get('status')}; rows={discovery.get('rowsReturned')}; limit={discovery.get('resultsLimit')}; unmatched={discovery.get('unmatchedRows')}. Review the source posts manually."]
    for url, items in groups.items():
        parent = next((x for x in included if x.get("kind") == "post" and x.get("url") == url), None)
        related = [x["page"] for x in included if x.get("url") == url]
        lines += ["", "="*60, f"Post URL: {url}", f"Parent report page: {parent['page'] if parent else 'not included'}", f"Insert after report page: {max(related) if related else 'locate by post order'} (original page numbering)", f"Missing photos: {len(items)}"]
        for item in items:
            lines += ["", f"Photo ID: {item.get('photoId', 'unknown')}", f"Photo URL: {item.get('photoUrl') or 'unavailable'}", f"Failure: {item.get('error')}"]
    for item in failures:
        if item.get("kind") != "photo":
            lines += ["", f"Missing {item.get('kind')}: {item.get('url')}", f"Failure: {item.get('error')}"]
    return "\n".join(lines) + "\n"


def build_manifest(posts, profile_url):
    """Order: post screenshot, its extra original images, next post, ..."""
    manifest = [{"kind": "profile", "url": profile_url}]
    for number, post in enumerate(posts, start=1):
        manifest.append({
            "kind": "post", "postNumber": number, "url": post["url"]
        })
        for photo in post["photos"]:
            manifest.append({
                "kind": "photo", "postNumber": number,
                "photoId": photo["id"], "url": post["url"],
                **{k: photo[k] for k in ("imageUrl", "photoUrl", "storageKey", "storageId", "downloadError") if photo.get(k)}
            })
    return manifest


async def run_screenshots(manifest, width, height, kind="post"):
    """Screenshot Actor output order is not trusted; merger matches by URL."""
    urls = list(dict.fromkeys(entry["url"] for entry in manifest if entry["kind"] == kind))
    log.info("STEP 3: Requesting %s screenshots", len(urls))
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
        "proxyConfiguration": {"useApifyProxy": False},
        "scrollPage": kind != "profile",
        "urls": urls,
        "viewportHeight": height,
        "viewportWidth": width,
    }
    run = await call_actor(SCREENSHOT_ACTOR, screenshot_input)
    dataset_id = run_field(run, "default_dataset_id", "defaultDatasetId")
    if not dataset_id:
        raise RuntimeError("Screenshot Actor returned no dataset ID")
    rows = await read_all_items(dataset_id)
    if not rows:
        raise RuntimeError("Screenshot Actor returned an empty dataset")
    successes = sum(
        1 for row in rows if isinstance(row, dict)
        and str(row.get("status", "")).lower() == "success"
    )
    log.info("STEP 3 COMPLETE: screenshot rows=%s successes=%s", len(rows), successes)
    return run, dataset_id, rows


async def run_pdf_merger(dataset_id, manifest, page_size, max_image_height, profile_dataset_id):
    log.info("STEP 4: Building ordered PDF from %s manifest entries", len(manifest))
    merger_input = {
        "datasetId": dataset_id,
        "profileDatasetId": profile_dataset_id,
        "manifest": manifest,
        "pageSize": page_size,
        "marginPoints": 30,
        "trimWhiteEdges": False,
        "maxImageHeight": max_image_height,
    }
    run = await call_actor(PDF_ACTOR, merger_input)
    store_id = run_field(run, "default_key_value_store_id", "defaultKeyValueStoreId")
    if not store_id:
        raise RuntimeError("PDF merger returned no key-value store ID")
    store = Actor.apify_client.key_value_store(store_id)
    for key, content_type in (
        ("facebook-posts.pdf", "application/pdf"),
        ("facebook-posts.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ):
        record = await store.get_record(key)
        value = field(record, "value") if record else None
        if isinstance(value, bytearray):
            value = bytes(value)
        if not isinstance(value, bytes) or not value:
            raise RuntimeError(f"Report merger returned no binary {key}")
        await Actor.set_value(key, value, content_type=content_type)
        log.info("Saved %s (%s bytes)", key, len(value))
    summary_record = await store.get_record("SUMMARY")
    merger_summary = field(summary_record, "value") if summary_record else None
    log.info("STEP 4 COMPLETE: PDF and DOCX saved")
    return run, merger_summary


# ------------------------------------------------------------
# Main orchestration
# ------------------------------------------------------------

async def main():
    print("FACEBOOK WORKFLOW: Started", flush=True)
    async with Actor:
        config = await Actor.get_input() or {}
        profile_url = validate_profile_url(config.get("facebookProfileUrl"))
        results_limit = int(config.get("resultsLimit", 20))
        photos_results_limit = int(config.get("photosResultsLimit", 100))
        only_newer_than = validate_date(config.get("onlyPostsNewerThan") or "2025-12-31")
        viewport_width = int(config.get("viewportWidth", 800))
        viewport_height = int(config.get("viewportHeight", 1280))
        max_image_height = int(config.get("maxImageHeight", 1400))
        page_size = str(config.get("pageSize") or "LETTER").upper()

        if results_limit < 1:
            raise ValueError("resultsLimit must be positive")
        if photos_results_limit < 1:
            raise ValueError("photosResultsLimit must be positive")
        if viewport_width < 1 or viewport_height < 1:
            raise ValueError("Viewport dimensions must be positive")
        if max_image_height < 0:
            raise ValueError("maxImageHeight must not be negative")
        if page_size not in ("LETTER", "A4"):
            raise ValueError("pageSize must be LETTER or A4")

        log.info("Profile URL: %s", profile_url)
        log.info("Requested maximum posts: %s", results_limit)
        log.info("Requested maximum photos (global): %s", photos_results_limit)
        log.info("Only posts newer than: %s", only_newer_than)
        log.info("Viewport: %sx%s; PDF page size: %s", viewport_width, viewport_height, page_size)

        posts_input = {
            "captionText": False,
            "onlyPostsNewerThan": only_newer_than,
            "resultsLimit": results_limit,
            "startUrls": [{"url": profile_url}],
        }
        posts, posts_info = await monitored_posts_scraper(posts_input)
        await Actor.set_value("POST_URLS", {
            "profileUrl": profile_url,
            "onlyPostsNewerThan": only_newer_than,
            "requestedMaximum": results_limit,
            "count": len(posts),
            "urls": [post["url"] for post in posts],
            "scraperStopReason": posts_info["stopReason"],
        })

        photos_info = await collect_photos(posts, photos_results_limit)
        await cache_photos(posts)
        manifest = build_manifest(posts, profile_url)
        await Actor.set_value("REPORT_MANIFEST", {
            "profileUrl": profile_url,
            "entries": manifest,
            "posts": len(posts),
            "photoPages": sum(len(post["photos"]) for post in posts),
        })
        log.info("Manifest prepared with %s ordered pages", len(manifest))

        profile_run, profile_dataset_id, profile_rows = await run_screenshots(
            manifest, viewport_width, viewport_height, kind="profile"
        )
        screenshot_run, screenshot_dataset_id, screenshot_rows = await run_screenshots(
            manifest, viewport_width, viewport_height
        )
        merger_run, merger_summary = await run_pdf_merger(
            screenshot_dataset_id, manifest, page_size, max_image_height, profile_dataset_id
        )

        summary = {
            "status": "success",
            "facebookProfileUrl": profile_url,
            "onlyPostsNewerThan": only_newer_than,
            "requestedMaximumPosts": results_limit,
            "postsScraped": posts_info["postsScraped"],
            "uniquePostUrls": len(posts),
            "scraperRunId": posts_info["runId"],
            "scraperStopReason": posts_info["stopReason"],
            "scraperFinalStatus": posts_info["finalStatus"],
            "scraperCompletedNormally": posts_info["stopReason"] == "completed_normally",
            "photosResultsLimit": photos_results_limit,
            "photosScraper": photos_info,
            "additionalPhotosIncluded": sum(len(post["photos"]) for post in posts),
            "screenshotsRequested": len(set(entry["url"] for entry in manifest if entry["kind"] in ("profile", "post"))),
            "profileScreenshotRunId": field(profile_run, "id"),
            "profileScreenshotDatasetId": profile_dataset_id,
            "profileScreenshotDatasetRecords": len(profile_rows),
            "screenshotDatasetRecords": len(screenshot_rows),
            "screenshotRunId": field(screenshot_run, "id"),
            "mergerRunId": field(merger_run, "id"),
            "pdfKey": "facebook-posts.pdf",
            "docxKey": "facebook-posts.docx",
            "pdfSummary": merger_summary,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }
        summary["additionalPhotosIncluded"] = sum(x.get("kind") == "photo" for x in (merger_summary or {}).get("included", []))
        summary["logsKey"] = "REPORT_LOGS.txt"
        await Actor.set_value("REPORT_LOGS.txt", capture_log(summary, manifest), content_type="text/plain; charset=utf-8")
        await Actor.set_value("SUMMARY", summary)
        await Actor.push_data(summary)
        log.info(
            "WORKFLOW COMPLETE: %s posts, %s photos, %s PDF pages",
            len(posts), summary["additionalPhotosIncluded"],
            field(merger_summary, "pages", "unknown")
        )
        print("FACEBOOK WORKFLOW: Completed", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
