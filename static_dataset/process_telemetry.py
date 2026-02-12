"""Process raw CSV telemetry data inside Docker container.

Supports two modes:
  --mode init   : Load initial time window of data (synchronous).
  --mode stream : Continuously add new data as time passes (background).

Reads raw CSVs from /data/raw/ using chunked reading for efficiency.
Only rows within the target time window are kept in memory.
If data is sorted by timestamp, reading stops early once past the window.

The host passes configuration via /app/config.json.
"""

import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd


RAW_DATA_PATH = Path("/root/raw")
OUTPUT_PATH = Path("/agent/telemetry")
CONFIG_PATH = Path("/root/config.json")
STATE_PATH = Path("/agent/telemetry/.stream_state.json")

CHUNK_SIZE = 50_000


def load_config():
    if not CONFIG_PATH.exists():
        print("Error: /app/config.json not found")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        return json.load(f)


def resolve_files(config):
    """Resolve raw CSV file paths grouped by telemetry type.

    Returns:
        list of (tel_type, filepath) tuples.
    """
    data_mapping = config.get("data_mapping", {})
    telemetry = config.get("telemetry", {})

    type_configs = [
        ("logs", "log_files", "enable_log"),
        ("metrics", "metric_files", "enable_metric"),
        ("traces", "trace_files", "enable_trace"),
    ]

    files = []
    for tel_type, files_key, enable_key in type_configs:
        if not telemetry.get(enable_key, False):
            continue

        for filename in data_mapping.get(files_key, []):
            matches = list(RAW_DATA_PATH.rglob(filename))
            if not matches:
                matches = list(RAW_DATA_PATH.rglob(f"*{filename}*"))
            for filepath in matches:
                files.append((tel_type, filepath))

    return files


def read_csv_filtered(filepath, start_ts=None, end_ts=None):
    """Read CSV in chunks, returning only rows within [start_ts, end_ts).

    If the data is sorted by timestamp and we pass end_ts, stops early.
    If no time window is given, reads everything (still chunked for memory).

    Returns:
        (DataFrame of matching rows, bool whether any rows existed)
    """
    chunks = []
    has_timestamp = None
    total_read = 0

    for chunk in pd.read_csv(filepath, chunksize=CHUNK_SIZE):
        total_read += len(chunk)

        # Check on first chunk if timestamp column exists
        if has_timestamp is None:
            has_timestamp = "timestamp" in chunk.columns

        if not has_timestamp or start_ts is None or end_ts is None:
            # No filtering possible, collect all
            chunks.append(chunk)
            continue

        # Filter to time window
        mask = (chunk["timestamp"] >= start_ts) & (chunk["timestamp"] < end_ts)
        filtered = chunk[mask]

        if not filtered.empty:
            chunks.append(filtered)

        # Early termination: if the minimum timestamp in this chunk is already
        # past the end of our window, all subsequent data is too late.
        if chunk["timestamp"].min() >= end_ts:
            break

    if not chunks:
        return pd.DataFrame(), total_read > 0

    return pd.concat(chunks, ignore_index=True), True


def write_rows(namespace, tel_type, filename, df, time_offset):
    """Apply time offset and append DataFrame to a single CSV per source file."""
    if df.empty:
        return 0

    out_dir = OUTPUT_PATH / namespace / tel_type
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    if time_offset and "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"] + time_offset

    output_file = out_dir / f"{filename}.csv"
    write_header = not output_file.exists()
    df.to_csv(output_file, mode="a", header=write_header, index=False)
    return len(df)


def mode_init(config):
    """Initial load: read only rows within [init_start, init_end] using chunked I/O."""
    namespace = config["namespace"]
    time_offset = config.get("time_offset", 0)
    init_start = config.get("init_start_original")
    init_end = config.get("init_end_original")

    print(f"=== Init mode: {namespace} ===")
    print(f"  Time offset: {time_offset}s")

    if init_start is not None and init_end is not None:
        print(f"  Original window: {datetime.fromtimestamp(init_start)} ~ {datetime.fromtimestamp(init_end)}")
    else:
        print("  No time window specified, loading all data")

    files = resolve_files(config)
    total_rows = 0

    for tel_type, filepath in files:
        try:
            df, _ = read_csv_filtered(filepath, init_start, init_end)
        except Exception as e:
            print(f"  Warning: Failed to read {filepath}: {e}")
            continue

        if df.empty:
            continue

        count = write_rows(namespace, tel_type, filepath.stem, df, time_offset)
        total_rows += count
        print(f"  {tel_type}/{filepath.stem}: {count} rows")

    # Save stream state: the boundary of what we've loaded
    stream_cursor = init_end if init_end is not None else 0
    state = {"stream_cursor": stream_cursor}
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)

    print(f"=== Init complete: {total_rows} rows ===")


def _get_max_timestamp(files):
    """Scan raw CSV files to find the maximum timestamp (lightweight)."""
    max_ts = 0
    for _, filepath in files:
        try:
            for chunk in pd.read_csv(filepath, usecols=["timestamp"],
                                     chunksize=CHUNK_SIZE):
                max_ts = max(max_ts, chunk["timestamp"].max())
        except (ValueError, KeyError):
            pass
    return max_ts


def mode_stream(config):
    """Streaming mode: periodically read new rows from disk and write them.

    Each tick reads [cursor, new_cursor) from raw CSV using chunked I/O,
    writes matching rows to /data/telemetry/, then discards them.
    No data is held in memory between ticks.
    """
    namespace = config["namespace"]
    time_offset = config.get("time_offset", 0)
    replay = config.get("replay", {})
    interval = replay.get("streaming_interval_seconds", 30)
    speed_factor = replay.get("speed_factor", 1.0)

    print(f"=== Stream mode: {namespace} ===")
    print(f"  Interval: {interval}s, Speed: {speed_factor}x")

    if not STATE_PATH.exists():
        print("Error: No stream state found. Run --mode init first.")
        sys.exit(1)

    with open(STATE_PATH) as f:
        state = json.load(f)

    cursor = state["stream_cursor"]
    if cursor == 0:
        print("No time window was set during init. Nothing to stream.")
        return

    files = resolve_files(config)
    max_ts = _get_max_timestamp(files)
    if max_ts <= cursor:
        print("  No remaining data to stream.")
        return

    print(f"  Starting cursor: {datetime.fromtimestamp(cursor)}")
    print(f"  Data ends at: {datetime.fromtimestamp(max_ts)}")

    last_tick = time.time()

    while cursor < max_ts:
        time.sleep(interval)

        now = time.time()
        elapsed_real = now - last_tick
        last_tick = now

        new_cursor = cursor + elapsed_real * speed_factor

        # Read only [cursor, new_cursor) from disk, write, then discard
        total_new = 0
        for tel_type, filepath in files:
            try:
                df, _ = read_csv_filtered(filepath, cursor, new_cursor)
            except Exception:
                continue
            if df.empty:
                continue
            count = write_rows(namespace, tel_type, filepath.stem, df, time_offset)
            total_new += count

        cursor = new_cursor
        with open(STATE_PATH, "w") as f:
            json.dump({"stream_cursor": cursor}, f)

        if total_new > 0:
            print(f"  +{total_new} rows (cursor: {datetime.fromtimestamp(cursor)})")

    print("=== Stream complete: all data consumed ===")


def main():
    parser = argparse.ArgumentParser(description="Process telemetry data")
    parser.add_argument("--mode", choices=["init", "stream", "all"],
                        default="all",
                        help="init=initial load, stream=background, all=legacy bulk")
    args = parser.parse_args()

    config = load_config()

    if args.mode == "init":
        mode_init(config)
    elif args.mode == "stream":
        mode_stream(config)
    elif args.mode == "all":
        mode_init(config)


if __name__ == "__main__":
    main()
