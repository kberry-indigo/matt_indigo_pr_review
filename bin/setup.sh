#!/usr/bin/env sh

set -e

gh_username=$1
repo_name=indigo-ag/api-interview-$gh_username
branch_name=order-null-hedge

echo $gh_username

command -v hub >/dev/null 2>&1 || { echo >&2 "'hub' is required; aborting."; exit 1; }

git fetch
git branch -f $branch_name origin/$branch_name
hub create $repo_name -p
git remote add $gh_username git@github.com:$repo_name.git 2>&1 || true
git push $gh_username master
git push $gh_username $branch_name:refs/heads/$branch_name

open_browser() {
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$1"
  elif command -v open >/dev/null 2>&1; then
    open "$1"
  else
    echo "$1"
  fi
}

echo "\nNext, you'll want to:"

echo "\n  1. Add $gh_username to the list of collaborators"
open_browser https://github.com/$repo_name/settings/collaboration

echo "\n  2. Create a new pull request"
open_browser https://github.com/$repo_name/compare/$branch_name
