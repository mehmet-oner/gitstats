# Git History Stats

Generate daily Git line-change stats without full checkout.

## Usage

Single repo:
```bash
python3 git_history_daily_stats.py <repo_url_or_path> --output daily_stats.csv
```

Multiple repos from JSON:
```bash
python3 git_history_daily_stats.py --repos-json repos_test.json --output all_repos_daily_stats.csv
```

Optional filters:
```bash
--ext ts --ext java   # only these file extensions
--since 30d           # history window: d=days, m=months, y=years (default is 7d)
```

## Dashboard

`dashboard.html` is a static viewer for the CSV output.
