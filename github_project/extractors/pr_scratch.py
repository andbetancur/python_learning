import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# agregamos el directorio raíz del proyecto al path de Python
# esto es necesario porque estamos corriendo el script desde extractors/
# y los imports como "from connectors.github_connector import..." buscan desde la raíz

import config
from utils.secrets import get_secret
from connectors.github_connector import GitHubConnector

gh = GitHubConnector(config.GCP_PROJECT_ID, config.SECRET_GITHUB)

commits = gh.get_paginated("/repos/andbetancur/python_learning/commits")

print(f"Total commits: {len(commits)}")
print(f"Primer commit: {commits[0]['sha']}")

rows = []
for commit in commits:
    # por cada commit imprimimos solo los campos que vamos a guardar en BigQuery
    
    git_author    = commit["commit"].get("author") or {}
    git_committer = commit["commit"].get("committer") or {}
    gh_author     = commit.get("author") or {}
    gh_committer  = commit.get("committer") or {}
    verification  = commit["commit"].get("verification") or {}
    
    rows.append({
        "sha":             commit["sha"],
        "message":         commit["commit"]["message"],
        "author_name":     git_author.get("name"),
        "author_email":    git_author.get("email"),
        "author_date":     git_author.get("date"),
        "committer_name":  git_committer.get("name"),
        "committer_login": gh_committer.get("login"),
        "author_login":    gh_author.get("login"),
        "comment_count":   commit["commit"].get("comment_count"),
        "verified":        verification.get("verified"),
        "parent_count":    len(commit.get("parents", [])),
        "html_url":        commit["html_url"],
    })

df = pd.DataFrame(rows)

df["author_date"] = pd.to_datetime(df["author_date"], utc=True)

print(df)
print('---')
print(df.dtypes)
print(df["author_date"])