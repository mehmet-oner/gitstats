#!/usr/bin/env python3
"""Generate day-by-day git history stats with minimal clone/download cost.

For remote repositories, this script creates/updates a local *bare partial clone*
(`--filter=blob:none`) and then scans history with one `git log --shortstat` pass.

Use `--commits-only` to skip diff stats and count only commits per day. In that
mode, the non-commit CSV columns are emitted as zeros for compatibility.

Output columns:
- date: YYYY-MM-DD (commit date)
- commits: number of commits on that day
- added_lines: total lines added
- removed_lines: total lines removed
- modified_lines: added_lines + removed_lines
- moved_lines_approx: per-commit `min(added, removed)` summed for the day

Note: Git does not expose exact "moved lines" directly in fast history stats; this is
an approximation that treats replaced/relocated lines within diffs as moved.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict

DATE_MARKER = "__DATE__"
INSERTIONS_RE = re.compile(r"(\d+)\s+insertions?\(\+\)")
DELETIONS_RE = re.compile(r"(\d+)\s+deletions?\(-\)")


def git_env() -> dict[str, str]:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    env["LANG"] = "C"
    return env


def run_git(
    args: list[str],
    cwd: Path | None = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=str(cwd) if cwd else None,
            text=True,
            env=git_env(),
            capture_output=capture_output,
            check=check,
        )
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or str(e)).strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {msg}") from e


def parse_since_parts(value: str) -> tuple[int, str]:
    m = re.fullmatch(r"\s*(\d+)\s*([dmyDMy])\s*", value)
    if not m:
        raise RuntimeError("Invalid --since value. Use formats like 9d, 10m, 1y")
    qty = int(m.group(1))
    if qty <= 0:
        raise RuntimeError("--since quantity must be > 0")
    return qty, m.group(2).lower()


def parse_since_window(value: str) -> str:
    qty, unit = parse_since_parts(value)
    word = {"d": "day", "m": "month", "y": "year"}[unit]
    if qty != 1:
        word += "s"
    return f"{qty} {word} ago"


def _shift_back_months(base: date, months: int) -> date:
    month_index = (base.year * 12 + (base.month - 1)) - months
    year = month_index // 12
    month = (month_index % 12) + 1
    day = min(base.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _shift_back_years(base: date, years: int) -> date:
    year = base.year - years
    day = min(base.day, calendar.monthrange(year, base.month)[1])
    return date(year, base.month, day)


def history_window_start_date(value: str) -> date:
    qty, unit = parse_since_parts(value)
    cutoff = date.today()

    if unit == "d":
        cutoff -= timedelta(days=qty)
    elif unit == "m":
        cutoff = _shift_back_months(cutoff, qty)
    else:
        cutoff = _shift_back_years(cutoff, qty)

    return cutoff


def shallow_fetch_since(value: str, pad_days: int = 30) -> str:
    return (history_window_start_date(value) - timedelta(days=pad_days)).isoformat()


def split_date_window(start: date, end_exclusive: date, slices: int) -> list[tuple[date, date]]:
    total_days = (end_exclusive - start).days
    if total_days <= 0:
        return []
    if slices <= 1:
        return [(start, end_exclusive)]

    actual_slices = min(slices, total_days)
    base, remainder = divmod(total_days, actual_slices)
    windows: list[tuple[date, date]] = []
    cursor = start

    for i in range(actual_slices):
        span = base + (1 if i < remainder else 0)
        next_cursor = cursor + timedelta(days=span)
        windows.append((cursor, next_cursor))
        cursor = next_cursor

    return windows


def repo_cache_path(repo: str, cache_dir: Path) -> Path:
    digest = hashlib.sha1(repo.encode("utf-8")).hexdigest()[:12]
    safe = "".join(ch if ch.isalnum() else "_" for ch in repo).strip("_")
    safe = safe[-40:] if len(safe) > 40 else safe
    return cache_dir / f"{safe}_{digest}.git"


def is_remote_repo(repo: str) -> bool:
    return repo.startswith(("http://", "https://", "ssh://", "git@"))


def normalize_branch_name(branch: str) -> str:
    cleaned = branch.strip()
    prefix = "refs/heads/"
    if cleaned.startswith(prefix):
        cleaned = cleaned[len(prefix):]
    if not cleaned:
        raise RuntimeError("Branch name cannot be empty")
    return cleaned


def detect_remote_default_branch(repo: str) -> str:
    output = run_git(["ls-remote", "--symref", repo, "HEAD"]).stdout.splitlines()
    for line in output:
        if not line.startswith("ref: ") or not line.endswith("\tHEAD"):
            continue
        ref_name = line[5:].split("\t", 1)[0]
        prefix = "refs/heads/"
        if ref_name.startswith(prefix):
            return ref_name[len(prefix):]
    raise RuntimeError(f"Could not determine default branch for remote: {repo}")


def detect_local_default_ref(repo_path: Path) -> str:
    result = run_git(
        ["-C", str(repo_path), "symbolic-ref", "--quiet", "--short", "HEAD"],
        check=False,
    )
    ref_name = result.stdout.strip()
    return ref_name or "HEAD"


def is_shallow_repo(repo_path: Path) -> bool:
    result = run_git(
        ["-C", str(repo_path), "rev-parse", "--is-shallow-repository"],
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def build_remote_fetch_args(
    repo_path: Path,
    branch: str | None,
    since: str | None,
    include_shallow: bool = True,
    refetch: bool = False,
) -> list[str]:
    args = [
        "-C",
        str(repo_path),
        "fetch",
    ]
    if refetch:
        args.append("--refetch")
    args.extend([
        "--progress",
        "--filter=blob:none",
    ])
    if branch is None:
        args.append("--prune")
    args.extend([
        "--prune-tags",
        "--no-tags",
    ])
    if since and include_shallow:
        args.append(f"--shallow-since={shallow_fetch_since(since)}")
    args.append("origin")
    if branch is None:
        args.append("+refs/heads/*:refs/heads/*")
    else:
        args.append(f"+refs/heads/{branch}:refs/heads/{branch}")
    return args


def refresh_commit_graph(repo_path: Path) -> None:
    run_git(
        [
            "-C",
            str(repo_path),
            "commit-graph",
            "write",
            "--reachable",
            "--split",
            "--changed-paths",
        ]
    )


def prepare_repo(
    repo: str,
    cache_dir: Path,
    since: str | None = None,
    branch: str | None = None,
    all_branches: bool = False,
) -> tuple[Path, str | None]:
    if not is_remote_repo(repo):
        p = Path(repo).expanduser().resolve()
        if not (p / ".git").exists() and not (p / "objects").exists():
            raise RuntimeError(f"Not a git repository: {p}")
        if all_branches:
            return p, None
        if branch:
            return p, normalize_branch_name(branch)
        return p, detect_local_default_ref(p)

    cache_dir.mkdir(parents=True, exist_ok=True)
    target = repo_cache_path(repo, cache_dir)
    selected_branch = None if all_branches else normalize_branch_name(branch) if branch else detect_remote_default_branch(repo)
    target_existed = target.exists()

    if not target_existed:
        t0 = time.time()
        print(f"[progress] initializing cache for {repo} -> {target}", file=sys.stderr, flush=True)
        run_git(["init", "--bare", str(target)], capture_output=False)
        run_git(["-C", str(target), "remote", "add", "origin", repo])
        run_git(["-C", str(target), "config", "remote.origin.promisor", "true"])
        run_git(["-C", str(target), "config", "remote.origin.partialclonefilter", "blob:none"])
        run_git(["-C", str(target), "config", "extensions.partialClone", "origin"])
        print(f"[progress] cache initialized in {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
    else:
        origin = run_git(["-C", str(target), "remote", "get-url", "origin"]).stdout.strip()
        if origin != repo:
            raise RuntimeError(f"Cache path collision: expected {repo}, found {origin}")

    t0 = time.time()
    branch_label = selected_branch or "all branches"
    print(f"[progress] fetching {branch_label} for {repo}", file=sys.stderr, flush=True)
    run_git(
        build_remote_fetch_args(
            target,
            selected_branch,
            since,
            include_shallow=(not target_existed) or is_shallow_repo(target),
        ),
        capture_output=False,
    )
    refresh_commit_graph(target)
    print(f"[progress] fetch finished in {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
    if selected_branch is None:
        return target, None
    return target, f"refs/heads/{selected_branch}"


def _collect_daily_stats_with_cmd(
    cmd: list[str],
    progress_label: str | None = None,
    progress_every_commits: int = 5000,
) -> Dict[str, dict]:

    proc = subprocess.Popen(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=git_env(),
    )
    if not proc.stdout:
        raise RuntimeError("Failed to capture git log output")

    daily = defaultdict(lambda: {
        "commits": 0,
        "added_lines": 0,
        "removed_lines": 0,
        "modified_lines": 0,
        "moved_lines_approx": 0,
        "net_change_lines": 0,
    })

    current_date = None
    commits_seen = 0
    t0 = time.time()
    last_report = t0

    for raw in proc.stdout:
        line = raw.rstrip("\n")

        if line.startswith(DATE_MARKER):
            current_date = line[len(DATE_MARKER):]
            daily[current_date]["commits"] += 1
            commits_seen += 1
            if progress_label and commits_seen % progress_every_commits == 0:
                elapsed = time.time() - t0
                print(
                    f"[progress] {progress_label}: parsed {commits_seen} commits in {elapsed:.1f}s",
                    file=sys.stderr,
                    flush=True,
                )
                last_report = time.time()
            continue

        if not line or current_date is None:
            continue

        add_match = INSERTIONS_RE.search(line)
        del_match = DELETIONS_RE.search(line)
        if not add_match and not del_match:
            continue

        added = int(add_match.group(1)) if add_match else 0
        removed = int(del_match.group(1)) if del_match else 0

        day = daily[current_date]
        day["added_lines"] += added
        day["removed_lines"] += removed
        day["modified_lines"] += added + removed
        day["moved_lines_approx"] += min(added, removed)
        day["net_change_lines"] += added - removed

        if progress_label and (time.time() - last_report) >= 15:
            elapsed = time.time() - t0
            print(
                f"[progress] {progress_label}: still working, {commits_seen} commits seen in {elapsed:.1f}s",
                file=sys.stderr,
                flush=True,
            )
            last_report = time.time()

    stderr = proc.stderr.read() if proc.stderr else ""
    returncode = proc.wait()

    if returncode != 0:
        raise RuntimeError(f"git log failed (exit {returncode}): {stderr.strip()}")

    if progress_label:
        elapsed = time.time() - t0
        print(
            f"[progress] {progress_label}: done, parsed {commits_seen} commits in {elapsed:.1f}s",
            file=sys.stderr,
            flush=True,
        )
    return dict(sorted(daily.items(), key=lambda kv: kv[0]))


def is_promisor_corruption_error(message: str) -> bool:
    m = message.lower()
    return (
        "git log failed" in m
        and "commit graph file but not in the object database" in m
        and "promisor remote" in m
    )


def repair_cached_repo(repo_path: Path, branch: str | None, since: str | None) -> None:
    print(f"[progress] attempting cache repair for {repo_path}", file=sys.stderr, flush=True)
    run_git(
        build_remote_fetch_args(
            repo_path,
            branch,
            since,
            include_shallow=is_shallow_repo(repo_path),
            refetch=True,
        ),
        capture_output=False,
    )
    refresh_commit_graph(repo_path)


def rebuild_cached_repo(
    repo: str,
    cache_dir: Path,
    since: str | None,
    branch: str | None,
    all_branches: bool,
) -> tuple[Path, str | None]:
    target = repo_cache_path(repo, cache_dir)
    if target.exists():
        print(f"[progress] removing corrupted cache {target}", file=sys.stderr, flush=True)
        shutil.rmtree(target, ignore_errors=True)
    return prepare_repo(
        repo,
        cache_dir,
        since=since,
        branch=branch,
        all_branches=all_branches,
    )


def build_git_log_cmd(
    repo_path: Path,
    target_ref: str | None,
    exts: list[str] | None = None,
    since: str | None = None,
    range_start: date | None = None,
    range_end_exclusive: date | None = None,
    commits_only: bool = False,
) -> list[str]:
    cmd = [
        "git",
        "-c",
        "commitGraph.readChangedPaths=true",
    ]
    if not commits_only:
        cmd.extend([
            "-c",
            "diff.algorithm=myers",
            "-c",
            "diff.indentHeuristic=false",
        ])
    cmd.extend([
        "-C",
        str(repo_path),
        "log",
        "--date=short",
        "--pretty=format:" + DATE_MARKER + "%cd",
    ])
    if not commits_only:
        cmd.extend([
            "--shortstat",
            "--no-color",
            "--no-notes",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
        ])

    if target_ref is None:
        cmd.append("--all")
    else:
        cmd.append(target_ref)

    if range_start is not None and range_end_exclusive is not None:
        cmd.append(f"--since={range_start.isoformat()} 00:00:00")
        cmd.append(f"--before={range_end_exclusive.isoformat()} 00:00:00")
    elif since:
        cmd.append(f"--since={parse_since_window(since)}")

    norm_exts: list[str] = []
    if exts:
        for ext in exts:
            e = ext.strip().lower().lstrip(".")
            if e:
                norm_exts.append(e)

    if norm_exts:
        cmd.append("--")
        cmd.extend([f":(glob)**/*.{ext}" for ext in sorted(set(norm_exts))])

    return cmd


def merge_daily_stats(parts: list[Dict[str, dict]]) -> Dict[str, dict]:
    merged = defaultdict(lambda: {
        "commits": 0,
        "added_lines": 0,
        "removed_lines": 0,
        "modified_lines": 0,
        "moved_lines_approx": 0,
        "net_change_lines": 0,
    })

    for part in parts:
        for day, row in part.items():
            merged_day = merged[day]
            merged_day["commits"] += row["commits"]
            merged_day["added_lines"] += row["added_lines"]
            merged_day["removed_lines"] += row["removed_lines"]
            merged_day["modified_lines"] += row["modified_lines"]
            merged_day["moved_lines_approx"] += row["moved_lines_approx"]
            merged_day["net_change_lines"] += row["net_change_lines"]

    return dict(sorted(merged.items(), key=lambda kv: kv[0]))


def collect_daily_stats_time_sliced(
    repo_path: Path,
    target_ref: str | None,
    exts: list[str] | None,
    since: str,
    intra_repo_jobs: int,
    commits_only: bool = False,
    progress_label: str | None = None,
) -> Dict[str, dict]:
    windows = split_date_window(
        history_window_start_date(since),
        date.today() + timedelta(days=1),
        intra_repo_jobs,
    )
    if len(windows) <= 1:
        return _collect_daily_stats_with_cmd(
            build_git_log_cmd(
                repo_path,
                target_ref,
                exts=exts,
                since=since,
                commits_only=commits_only,
            ),
            progress_label=progress_label,
        )

    results: list[Dict[str, dict]] = []
    with ThreadPoolExecutor(max_workers=len(windows)) as executor:
        future_map = {}
        for index, (range_start, range_end) in enumerate(windows, start=1):
            chunk_label = progress_label
            if chunk_label:
                chunk_label = f"{chunk_label} [slice {index}/{len(windows)}]"
            future = executor.submit(
                _collect_daily_stats_with_cmd,
                build_git_log_cmd(
                    repo_path,
                    target_ref,
                    exts=exts,
                    range_start=range_start,
                    range_end_exclusive=range_end,
                    commits_only=commits_only,
                ),
                chunk_label,
            )
            future_map[future] = index

        for future in as_completed(future_map):
            results.append(future.result())

    return merge_daily_stats(results)


def collect_daily_stats_filtered(
    repo: str,
    repo_path: Path,
    cache_dir: Path,
    target_ref: str | None,
    exts: list[str] | None = None,
    since: str | None = None,
    branch: str | None = None,
    all_branches: bool = False,
    intra_repo_jobs: int = 1,
    commits_only: bool = False,
    progress_label: str | None = None,
) -> Dict[str, dict]:
    def run_collection(current_repo_path: Path, current_target_ref: str | None) -> Dict[str, dict]:
        if since and intra_repo_jobs > 1:
            return collect_daily_stats_time_sliced(
                current_repo_path,
                current_target_ref,
                exts,
                since,
                intra_repo_jobs,
                commits_only=commits_only,
                progress_label=progress_label,
            )
        return _collect_daily_stats_with_cmd(
            build_git_log_cmd(
                current_repo_path,
                current_target_ref,
                exts=exts,
                since=since,
                commits_only=commits_only,
            ),
            progress_label=progress_label,
        )

    try:
        return run_collection(repo_path, target_ref)
    except RuntimeError as e:
        if not is_promisor_corruption_error(str(e)):
            raise
        repair_branch = normalize_branch_name(target_ref) if target_ref else None
        repair_cached_repo(repo_path, repair_branch, since)
        print(f"[progress] retrying git log after repair for {repo_path}", file=sys.stderr, flush=True)
        try:
            return run_collection(repo_path, target_ref)
        except RuntimeError as e2:
            if not is_promisor_corruption_error(str(e2)) or not is_remote_repo(repo):
                raise
            rebuilt_repo_path, rebuilt_target_ref = rebuild_cached_repo(
                repo,
                cache_dir,
                since,
                branch,
                all_branches,
            )
            print(
                f"[progress] retrying git log after full cache rebuild for {rebuilt_repo_path}",
                file=sys.stderr,
                flush=True,
            )
            return run_collection(rebuilt_repo_path, rebuilt_target_ref)


def write_csv(daily_stats: Dict[str, dict], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date",
            "commits",
            "added_lines",
            "removed_lines",
            "modified_lines",
            "moved_lines_approx",
            "net_change_lines",
        ])
        for date, row in daily_stats.items():
            writer.writerow([
                date,
                row["commits"],
                row["added_lines"],
                row["removed_lines"],
                row["modified_lines"],
                row["moved_lines_approx"],
                row["net_change_lines"],
            ])


def write_combined_csv(rows: list[dict], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "repo",
            "date",
            "commits",
            "added_lines",
            "removed_lines",
            "modified_lines",
            "moved_lines_approx",
            "net_change_lines",
        ])
        for row in rows:
            writer.writerow([
                row["repo"],
                row["date"],
                row["commits"],
                row["added_lines"],
                row["removed_lines"],
                row["modified_lines"],
                row["moved_lines_approx"],
                row["net_change_lines"],
            ])


def append_combined_rows(rows: list[dict], output_csv: Path, write_header: bool = False) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "repo",
                "date",
                "commits",
                "added_lines",
                "removed_lines",
                "modified_lines",
                "moved_lines_approx",
                "net_change_lines",
            ])
        for row in rows:
            writer.writerow([
                row["repo"],
                row["date"],
                row["commits"],
                row["added_lines"],
                row["removed_lines"],
                row["modified_lines"],
                row["moved_lines_approx"],
                row["net_change_lines"],
            ])
        f.flush()


def load_repos_from_json(json_path: Path) -> list[str]:
    with json_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    repos: list[str]
    if isinstance(payload, list):
        repos = payload
    elif isinstance(payload, dict) and isinstance(payload.get("repos"), list):
        repos = payload["repos"]
    else:
        raise RuntimeError("JSON input must be either an array of repo strings or {\"repos\": [...]} format")

    cleaned = [r.strip() for r in repos if isinstance(r, str) and r.strip()]
    if not cleaned:
        raise RuntimeError("JSON input did not contain any valid repository entries")
    return cleaned


def build_rows_for_repo(
    index: int,
    total_repos: int,
    repo: str,
    cache_dir: Path,
    exts: list[str],
    since: str,
    branch: str | None,
    all_branches: bool,
    intra_repo_jobs: int,
    commits_only: bool,
) -> tuple[int, list[dict], int]:
    print(f"[progress] [{index}/{total_repos}] preparing {repo}", file=sys.stderr, flush=True)
    repo_path, target_ref = prepare_repo(
        repo,
        cache_dir,
        since=since,
        branch=branch,
        all_branches=all_branches,
    )
    daily_stats = collect_daily_stats_filtered(
        repo,
        repo_path,
        cache_dir,
        target_ref,
        exts,
        since,
        branch=branch,
        all_branches=all_branches,
        intra_repo_jobs=intra_repo_jobs,
        commits_only=commits_only,
        progress_label=f"[{index}/{total_repos}] {repo}",
    )

    rows_for_repo: list[dict] = []
    for day, row in daily_stats.items():
        rows_for_repo.append({
            "repo": repo,
            "date": day,
            "commits": row["commits"],
            "added_lines": row["added_lines"],
            "removed_lines": row["removed_lines"],
            "modified_lines": row["modified_lines"],
            "moved_lines_approx": row["moved_lines_approx"],
            "net_change_lines": row["net_change_lines"],
        })
    rows_for_repo.sort(key=lambda r: r["date"])
    return index, rows_for_repo, len(daily_stats)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute day-by-day line-change stats for one or many git repos without full checkout."
    )
    parser.add_argument("repo", nargs="?", help="Git repo URL or local repo path")
    parser.add_argument("--output", default="daily_stats.csv", help="CSV output path")
    parser.add_argument("--repos-json", help="Path to JSON file containing repositories")
    parser.add_argument(
        "--cache-dir",
        default=".cache",
        help="Directory for cached bare partial clones (default: .cache)",
    )
    parser.add_argument(
        "--ext",
        action="append",
        default=[],
        help="Optional file extension filter (repeatable), e.g. --ext ts --ext java",
    )
    parser.add_argument(
        "--since",
        default="7d",
        help="History window, e.g. 9d (days), 10m (months), 1y (years). Default: 7d",
    )
    parser.add_argument(
        "--branch",
        help="Branch to scan for faster stats (default: remote HEAD or current local branch)",
    )
    parser.add_argument(
        "--all-branches",
        action="store_true",
        help="Scan all branches instead of a single branch (slower)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=max(1, min(4, os.cpu_count() or 1)),
        help="Number of repos to process in parallel when using --repos-json (default: up to 4)",
    )
    parser.add_argument(
        "--intra-repo-jobs",
        type=int,
        default=1,
        help="Number of time-sliced workers to use within a single repo scan (default: 1)",
    )
    parser.add_argument(
        "--commits-only",
        action="store_true",
        help="Count commits per day only and skip diff/line stats for much faster scans",
    )
    args = parser.parse_args()

    if not args.repo and not args.repos_json:
        print("Error: provide either a single <repo> argument or --repos-json", file=sys.stderr)
        return 1
    if args.repo and args.repos_json:
        print("Error: use either <repo> or --repos-json, not both", file=sys.stderr)
        return 1
    if args.branch and args.all_branches:
        print("Error: use either --branch or --all-branches, not both", file=sys.stderr)
        return 1
    if args.jobs <= 0:
        print("Error: --jobs must be greater than 0", file=sys.stderr)
        return 1
    if args.intra_repo_jobs <= 0:
        print("Error: --intra-repo-jobs must be greater than 0", file=sys.stderr)
        return 1

    try:
        cache_dir = Path(args.cache_dir).expanduser()

        if args.repos_json:
            repos = load_repos_from_json(Path(args.repos_json).expanduser())
            combined_output = Path(args.output).expanduser()
            workers = min(args.jobs, len(repos))
            completed_rows: list[tuple[int, list[dict]]] = []
            total_rows = 0

            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(
                        build_rows_for_repo,
                        i,
                        len(repos),
                        repo,
                        cache_dir,
                        args.ext,
                        args.since,
                        args.branch,
                        args.all_branches,
                        args.intra_repo_jobs,
                        args.commits_only,
                    ): (i, repo)
                    for i, repo in enumerate(repos, start=1)
                }

                for future in as_completed(future_map):
                    i, repo = future_map[future]
                    index, rows_for_repo, day_count = future.result()
                    print(f"[{i}/{len(repos)}] {repo} processed ({day_count} day rows)")
                    completed_rows.append((index, rows_for_repo))
                    total_rows += len(rows_for_repo)

            combined_rows: list[dict] = []
            for _, rows_for_repo in sorted(completed_rows, key=lambda item: item[0]):
                combined_rows.extend(rows_for_repo)
            write_combined_csv(combined_rows, combined_output)
            print(f"Wrote combined CSV with {total_rows} rows to {combined_output}")
            return 0

        repo_path, target_ref = prepare_repo(
            args.repo,
            cache_dir,
            since=args.since,
            branch=args.branch,
            all_branches=args.all_branches,
        )
        daily_stats = collect_daily_stats_filtered(
            args.repo,
            repo_path,
            cache_dir,
            target_ref,
            args.ext,
            args.since,
            branch=args.branch,
            all_branches=args.all_branches,
            intra_repo_jobs=args.intra_repo_jobs,
            commits_only=args.commits_only,
            progress_label=args.repo,
        )
        write_csv(daily_stats, Path(args.output).expanduser())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Wrote {len(daily_stats)} day rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
