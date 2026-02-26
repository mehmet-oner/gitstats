#!/usr/bin/env python3
"""Generate day-by-day git line-change stats with minimal clone/download cost.

For remote repositories, this script creates/updates a local *bare partial clone*
(`--filter=blob:none`) and then scans history with one `git log --shortstat` pass.

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


def parse_since_window(value: str) -> str:
    m = re.fullmatch(r"\s*(\d+)\s*([dmyDMy])\s*", value)
    if not m:
        raise RuntimeError("Invalid --since value. Use formats like 9d, 10m, 1y")
    qty = int(m.group(1))
    if qty <= 0:
        raise RuntimeError("--since quantity must be > 0")
    unit = m.group(2).lower()
    word = {"d": "day", "m": "month", "y": "year"}[unit]
    if qty != 1:
        word += "s"
    return f"{qty} {word} ago"


def repo_cache_path(repo: str, cache_dir: Path) -> Path:
    digest = hashlib.sha1(repo.encode("utf-8")).hexdigest()[:12]
    safe = "".join(ch if ch.isalnum() else "_" for ch in repo).strip("_")
    safe = safe[-40:] if len(safe) > 40 else safe
    return cache_dir / f"{safe}_{digest}.git"


def is_remote_repo(repo: str) -> bool:
    return repo.startswith(("http://", "https://", "ssh://", "git@"))


def prepare_repo(repo: str, cache_dir: Path) -> Path:
    if not is_remote_repo(repo):
        p = Path(repo).expanduser().resolve()
        if not (p / ".git").exists() and not (p / "objects").exists():
            raise RuntimeError(f"Not a git repository: {p}")
        return p

    cache_dir.mkdir(parents=True, exist_ok=True)
    target = repo_cache_path(repo, cache_dir)

    if not target.exists():
        # Bare + blobless partial clone keeps history metadata without checkout.
        t0 = time.time()
        print(f"[progress] cloning {repo} -> {target}", file=sys.stderr, flush=True)
        run_git(
            ["clone", "--progress", "--bare", "--filter=blob:none", "--no-tags", repo, str(target)],
            capture_output=False,
        )
        print(f"[progress] clone finished in {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
    else:
        origin = run_git(["-C", str(target), "remote", "get-url", "origin"]).stdout.strip()
        if origin != repo:
            raise RuntimeError(f"Cache path collision: expected {repo}, found {origin}")

    # Keep network and local updates minimal.
    t0 = time.time()
    print(f"[progress] fetching updates for {repo}", file=sys.stderr, flush=True)
    run_git(
        [
            "-C",
            str(target),
            "fetch",
            "--progress",
            "--filter=blob:none",
            "--prune",
            "--prune-tags",
            "--no-tags",
            "origin",
            "+refs/heads/*:refs/heads/*",
        ],
        capture_output=False,
    )
    print(f"[progress] fetch finished in {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
    return target


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


def repair_cached_repo(repo_path: Path) -> None:
    print(f"[progress] attempting cache repair for {repo_path}", file=sys.stderr, flush=True)
    run_git(
        [
            "-C",
            str(repo_path),
            "fetch",
            "--refetch",
            "--progress",
            "--filter=blob:none",
            "--prune",
            "--prune-tags",
            "--no-tags",
            "origin",
            "+refs/heads/*:refs/heads/*",
        ],
        capture_output=False,
    )
    run_git(["-C", str(repo_path), "commit-graph", "write", "--reachable"])


def rebuild_cached_repo(repo: str, cache_dir: Path) -> Path:
    target = repo_cache_path(repo, cache_dir)
    if target.exists():
        print(f"[progress] removing corrupted cache {target}", file=sys.stderr, flush=True)
        shutil.rmtree(target, ignore_errors=True)
    return prepare_repo(repo, cache_dir)


def collect_daily_stats_filtered(
    repo: str,
    repo_path: Path,
    cache_dir: Path,
    exts: list[str] | None = None,
    since: str | None = None,
    progress_label: str | None = None,
) -> Dict[str, dict]:
    cmd = [
        "git",
        "-c",
        "core.commitGraph=false",
        "-c",
        "diff.algorithm=myers",
        "-c",
        "diff.indentHeuristic=false",
        "-C",
        str(repo_path),
        "log",
        "--all",
        "--date=short",
        "--pretty=format:" + DATE_MARKER + "%cd",
        "--shortstat",
        "--no-color",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
    ]

    if since:
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

    try:
        return _collect_daily_stats_with_cmd(
            cmd,
            progress_label=progress_label,
        )
    except RuntimeError as e:
        if not is_promisor_corruption_error(str(e)):
            raise
        repair_cached_repo(repo_path)
        print(f"[progress] retrying git log after repair for {repo_path}", file=sys.stderr, flush=True)
        try:
            return _collect_daily_stats_with_cmd(
                cmd,
                progress_label=progress_label,
            )
        except RuntimeError as e2:
            if not is_promisor_corruption_error(str(e2)) or not is_remote_repo(repo):
                raise
            rebuilt_repo_path = rebuild_cached_repo(repo, cache_dir)
            c_index = cmd.index("-C")
            cmd[c_index + 1] = str(rebuilt_repo_path)
            print(
                f"[progress] retrying git log after full cache rebuild for {rebuilt_repo_path}",
                file=sys.stderr,
                flush=True,
            )
            return _collect_daily_stats_with_cmd(
                cmd,
                progress_label=progress_label,
            )


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
    args = parser.parse_args()

    if not args.repo and not args.repos_json:
        print("Error: provide either a single <repo> argument or --repos-json", file=sys.stderr)
        return 1
    if args.repo and args.repos_json:
        print("Error: use either <repo> or --repos-json, not both", file=sys.stderr)
        return 1

    try:
        cache_dir = Path(args.cache_dir).expanduser()

        if args.repos_json:
            repos = load_repos_from_json(Path(args.repos_json).expanduser())
            combined_output = Path(args.output).expanduser()
            combined_output.parent.mkdir(parents=True, exist_ok=True)
            if combined_output.exists():
                combined_output.unlink()
            append_combined_rows([], combined_output, write_header=True)
            total_rows = 0

            for i, repo in enumerate(repos, start=1):
                print(f"[progress] [{i}/{len(repos)}] preparing {repo}", file=sys.stderr, flush=True)
                repo_path = prepare_repo(repo, cache_dir)
                daily_stats = collect_daily_stats_filtered(
                    repo,
                    repo_path,
                    cache_dir,
                    args.ext,
                    args.since,
                    progress_label=f"[{i}/{len(repos)}] {repo}",
                )
                print(f"[{i}/{len(repos)}] {repo} processed ({len(daily_stats)} day rows)")

                rows_for_repo: list[dict] = []
                for date, row in daily_stats.items():
                    rows_for_repo.append({
                        "repo": repo,
                        "date": date,
                        "commits": row["commits"],
                        "added_lines": row["added_lines"],
                        "removed_lines": row["removed_lines"],
                        "modified_lines": row["modified_lines"],
                        "moved_lines_approx": row["moved_lines_approx"],
                        "net_change_lines": row["net_change_lines"],
                    })
                rows_for_repo.sort(key=lambda r: r["date"])
                append_combined_rows(rows_for_repo, combined_output, write_header=False)
                total_rows += len(rows_for_repo)

            print(f"Wrote combined CSV with {total_rows} rows to {combined_output}")
            return 0

        repo_path = prepare_repo(args.repo, cache_dir)
        daily_stats = collect_daily_stats_filtered(
            args.repo,
            repo_path,
            cache_dir,
            args.ext,
            args.since,
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
