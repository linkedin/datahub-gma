#!/usr/bin/env bash
# The purpose of this script is to publish artifacts to ~/local-repo, which is what LinkedIn uses internally as a local
# repository, rather than ~/.m2. So this is useful for LinkedIn devs, not other open source devs.

if [ ! -d "$HOME" ]; then
  echo 'Cannot perform local release, $HOME is not set to a valid directory.'
  exit 1
fi

# Create ~/local-repo if it doesn't already exist
LOCAL_REPO="${HOME}/local-repo"
if [ ! -d $LOCAL_REPO ]; then
  mkdir $LOCAL_REPO
fi

VERSION="$(./gradlew -q getVersion)-SNAPSHOT"

echo "Publishing GMA $VERSION to ${LOCAL_REPO}..."

# Publish artifacts to Maven local, but override the repo path and add a -SNAPSHOT suffix to avoid confusion with
# versions published to bintray.
./gradlew -Dmaven.repo.local=$LOCAL_REPO publishToMavenLocal -Pversion="${VERSION}-SNAPSHOT"

if [ $? = 0 ]; then
  echo "Published GMA $VERSION to $LOCAL_REPO"
else
  exit 1
fi