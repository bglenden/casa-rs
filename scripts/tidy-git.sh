#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/tidy-git.sh [--apply]

Finds local git state left behind after pull requests merge into origin/main:
linked worktrees and local branches whose commits reached main through a
merge. Without --apply it only reports; with --apply it removes them.

Never touched: the main worktree, the worktree this runs from, worktrees with
uncommitted or untracked changes, and branches or worktrees whose tip is on
main's first-parent line (for example a fresh branch with no commits yet).
EOF
}

apply=0
case "${1:-}" in
  "") ;;
  --apply) apply=1 ;;
  -h | --help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

start_worktree="$(git rev-parse --show-toplevel)"
main_worktree="$(git worktree list --porcelain | awk '/^worktree /{print substr($0, 10); exit}')"
cd "$main_worktree"

git fetch --prune --quiet origin
base="origin/main"
mainline=$'\n'"$(git rev-list --first-parent "$base")"$'\n'

# A tip counts as merged when main contains it but it is not on main's own
# first-parent line, i.e. it arrived through a merge.
merged_into_main() {
  git merge-base --is-ancestor "$1" "$base" && [[ "$mainline" != *$'\n'"$1"$'\n'* ]]
}

run() {
  if [[ $apply -eq 1 ]]; then
    "$@"
  else
    echo "  would run: $*"
  fi
}

echo "== Worktrees"
git worktree prune
worktree_branches=$'\n'
path="" head="" branch=""
while IFS= read -r line || [[ -n "$path" ]]; do
  case "$line" in
    "worktree "*) path="${line#worktree }" ;;
    "HEAD "*) head="${line#HEAD }" ;;
    "branch "*) branch="${line#branch refs/heads/}" ;;
    "")
      if [[ -n "$path" && "$path" != "$main_worktree" ]]; then
        label="$path${branch:+ [$branch]}"
        if [[ "$path" == "$start_worktree" ]]; then
          echo "keep   $label (current worktree)"
          worktree_branches+="$branch"$'\n'
        elif [[ -n "$(git -C "$path" status --porcelain 2>/dev/null)" ]]; then
          echo "keep   $label (uncommitted or untracked changes)"
          worktree_branches+="$branch"$'\n'
        elif merged_into_main "$head"; then
          echo "remove $label (merged into $base)"
          run git worktree remove "$path"
        else
          echo "keep   $label (not merged into $base)"
          worktree_branches+="$branch"$'\n'
        fi
      fi
      path="" head="" branch=""
      ;;
  esac
done < <(git worktree list --porcelain; echo)

echo "== Local branches"
current_branch="$(git branch --show-current)"
git for-each-ref --format='%(refname:short) %(objectname) %(upstream:track)' refs/heads |
  while read -r name sha track; do
    [[ "$name" == "main" || "$name" == "$current_branch" ]] && continue
    [[ "$worktree_branches" == *$'\n'"$name"$'\n'* ]] && continue
    if merged_into_main "$sha"; then
      echo "delete $name (merged into $base)"
      run git branch -D "$name"
    elif [[ "$track" == "[gone]" ]]; then
      echo "keep   $name (GitHub branch deleted but not merged into $base; check it)"
    fi
  done

if [[ $apply -eq 0 ]]; then
  echo "Dry run. Re-run with --apply (just tidy --apply) to make these changes."
fi
