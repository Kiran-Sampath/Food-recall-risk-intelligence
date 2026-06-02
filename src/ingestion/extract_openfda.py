import requests
import json
from datetime import datetime
import argparse
from pathlib import Path
from io import BytesIO
from zipfile import ZipFile


def _build_search(field: str, start_date: str | None, end_date: str | None) -> str | None:
    if not start_date and not end_date:
        return None
    start = (start_date or "19000101").replace("-", "")
    end = (end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")
    return f"{field}:[{start}+TO+{end}]"


def fetch_and_save(
    limit: int = 100,
    dest_dir: str = "data/bronze",
    page_size: int = 100,
    start_date: str | None = None,
    end_date: str | None = None,
    date_field: str = "report_date",
):
    url = "https://api.fda.gov/food/enforcement.json"
    search = _build_search(date_field, start_date, end_date)
    fallback_warning = None

    def fetch_batches(active_search: str | None):
        fetched = []
        for skip in range(0, limit, page_size):
            batch_limit = min(page_size, limit - skip)
            params = {"limit": batch_limit, "skip": skip}
            if active_search:
                params["search"] = active_search

            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("results", [])
            fetched.extend(batch)
            if len(batch) < batch_limit:
                break
        return fetched

    try:
        results = fetch_batches(search)
    except requests.HTTPError as err:
        status_code = err.response.status_code if err.response is not None else None
        if search and status_code and status_code >= 500:
            fallback_warning = f"openFDA returned HTTP {status_code} for search '{search}'. Retried without date filter."
            results = fetch_batches(None)
            search = None
        else:
            raise

    data = {
        "meta": {
            "source": url,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "requested_limit": limit,
            "records_fetched": len(results),
            "search": search,
            "warning": fallback_warning,
        },
        "results": results,
    }

    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(dest_dir) / f"food_recalls_{datetime.now().date()}.json"
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)

    print(f"Saved bronze file: {file_path}")
    return str(file_path)


def fetch_complete_download(dest_dir: str = "data/bronze"):
    manifest_url = "https://api.fda.gov/download.json"
    manifest_resp = requests.get(manifest_url, timeout=30)
    manifest_resp.raise_for_status()
    manifest = manifest_resp.json()
    enforcement = manifest["results"]["food"]["enforcement"]
    partitions = enforcement.get("partitions", [])
    if not partitions:
        raise RuntimeError("No food enforcement download partitions found in openFDA manifest.")

    all_results = []
    downloaded_files = []
    for partition in partitions:
        file_url = partition["file"]
        zip_resp = requests.get(file_url, timeout=120)
        zip_resp.raise_for_status()
        with ZipFile(BytesIO(zip_resp.content)) as archive:
            json_names = [name for name in archive.namelist() if name.endswith(".json")]
            if not json_names:
                raise RuntimeError(f"No JSON file found inside {file_url}")
            with archive.open(json_names[0]) as fh:
                payload = json.load(fh)
        all_results.extend(payload.get("results", []))
        downloaded_files.append(file_url)

    data = {
        "meta": {
            "source": manifest_url,
            "downloaded_files": downloaded_files,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "records_fetched": len(all_results),
            "manifest_total_records": enforcement.get("total_records"),
            "export_date": enforcement.get("export_date"),
            "warning": None,
        },
        "results": all_results,
    }

    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(dest_dir) / f"food_recalls_complete_{datetime.now().date()}.json"
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)

    print(f"Saved complete bronze file: {file_path}")
    return str(file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dest", type=str, default="data/bronze")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--date-field", type=str, default="report_date")
    parser.add_argument("--complete-download", action="store_true", help="Download the complete food enforcement dataset from openFDA's zipped download file.")
    args = parser.parse_args()
    if args.complete_download:
        fetch_complete_download(dest_dir=args.dest)
    else:
        fetch_and_save(
            limit=args.limit,
            dest_dir=args.dest,
            page_size=args.page_size,
            start_date=args.start_date,
            end_date=args.end_date,
            date_field=args.date_field,
        )


if __name__ == "__main__":
    main()
