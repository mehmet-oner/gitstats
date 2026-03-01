# Git History Stats

Generate daily Git line-change stats without full checkout.

By default the script now scans one branch (remote default branch or the current local branch),
fetches only the requested history window for remote repos, and processes multi-repo batches in parallel.
Use `--commits-only` to skip diff stats and count only commits per day.

## Usage

Single repo:
```bash
python3 git_history_daily_stats.py <repo_url_or_path> --since 5y --commits-only --output daily_stats.csv
```

Multiple repos from JSON:
```bash
python3 git_history_daily_stats.py --repos-json repos_test.json --since 5y --jobs 4 --intra-repo-jobs 4 --output all_repos_daily_stats.csv
```

Optional filters:
```bash
--ext ts --ext java   # only these file extensions
--since 30d           # history window: d=days, m=months, y=years (default is 7d)
--branch main         # scan a specific branch
--all-branches        # scan every branch (slower, old behavior)
--intra-repo-jobs 4   # split one repo's time window into parallel slices
--commits-only        # count commits only; non-commit columns are zero
```

## Dashboard

`dashboard.html` is a static viewer for the CSV output.
