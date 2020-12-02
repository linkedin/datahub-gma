#!/usr/bin/env bash
# Publishes artifacts to the local maven repository, also setting the version to a SNAPSHOT version to avoid confusion
# with the artifacts published to bintray.

VERSION="$(./gradlew -q getVersion)-SNAPSHOT"

echo "Publishing GMA $VERSION to ${LOCAL_REPO}..."

./gradlew publishToMavenLocal -Pversion="${VERSION}"

if [ $? = 0 ]; then
  echo "Published GMA $VERSION to local maven"
else
  exit 1
fi