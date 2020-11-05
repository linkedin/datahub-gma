#!/bin/bash

tag="v$1"
echo "Applying and pushing tag $tag"

# For debugging
echo "current branch" "$(git branch --show-current)"
echo "=== recent history"
git log --oneline -n 10
echo "==="

EXISTING_TAGS=$(git tag --points-at HEAD)

if [[ "$EXISTING_TAGS" ]]; then
  echo "Tags already exist on HEAD: $EXISTING_TAGS"
  exit 1
fi

echo "Tagging commit"

git tag "$tag"

commit=$(git rev-parse HEAD)
full_name=$GITHUB_REPOSITORY
git_refs_url=$(jq .repository.git_refs_url $GITHUB_EVENT_PATH | tr -d '"' | sed 's/{\/sha}//g')
echo "pushing tag $tag to repo $full_name"

git_refs_response=$(
curl -s -X POST $git_refs_url \
-H "Authorization: token $GITHUB_TOKEN" \
-d @- << EOF
{
  "ref": "refs/tags/$tag",
  "sha": "$commit"
}
EOF
)

git_ref_posted=$( echo "${git_refs_response}" | jq .ref | tr -d '"' )

echo "::debug::${git_refs_response}"
if [ "${git_ref_posted}" = "refs/tags/${tag}" ]; then
  exit 0
else
  echo "::error::Tag was not created properly."
  exit 1
fi