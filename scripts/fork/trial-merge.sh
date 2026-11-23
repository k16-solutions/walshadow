#!/usr/bin/env bash
# k16 fork: report what merging <theirs> into <ours> would conflict on,
# without touching the working tree (git merge-tree). See docs/upstream-sync.md.
#
#   scripts/fork/trial-merge.sh [ours] [theirs]     # default origin/main upstream/main
#
# Prints Markdown: commits behind, conflicted files with conflicted-line counts.
# Exit 0 = merges cleanly, 1 = conflicts, 2 = usage or git error.
set -euo pipefail

ours=${1:-origin/main}
theirs=${2:-upstream/main}

for ref in "$ours" "$theirs"; do
  git rev-parse --verify --quiet "$ref^{commit}" >/dev/null || {
    echo "unknown ref: $ref" >&2
    exit 2
  }
done

behind=$(git rev-list --count --no-merges "$ours..$theirs")
base=$(git merge-base "$ours" "$theirs")

set +e
out=$(git merge-tree --write-tree --name-only "$ours" "$theirs")
status=$?
set -e
if [ "$status" -gt 1 ]; then
  echo "git merge-tree failed ($status)" >&2
  exit 2
fi
tree=$(printf '%s\n' "$out" | head -n 1)

echo "### Trial merge of \`$theirs\` into \`$ours\`"
echo
echo "- Upstream commits not yet merged: **$behind**"
echo "- Merge base: \`$(git log -1 --format='%h %ad %s' --date=short "$base")\`"

if [ "$status" -eq 0 ]; then
  echo "- Result: merges cleanly"
  exit 0
fi

echo
echo "| Conflicted lines | File |"
echo "|---:|---|"
total=0
files=0
while IFS= read -r line; do
  case "$line" in
    "CONFLICT (content): Merge conflict in "*)
      f=${line#CONFLICT (content): Merge conflict in }
      n=$(git show "$tree:$f" | awk '/^<<<<<<< /{i=1;next} /^>>>>>>> /{i=0;next} i{n++} END{print n+0}')
      total=$((total + n))
      files=$((files + 1))
      echo "| $n | \`$f\` |"
      ;;
    CONFLICT*)
      files=$((files + 1))
      echo "| – | ${line#CONFLICT } |"
      ;;
  esac
done < <(printf '%s\n' "$out" | grep '^CONFLICT' | sort)
echo
echo "- Result: **$files** conflicted files, **$total** conflicted lines"
exit 1
