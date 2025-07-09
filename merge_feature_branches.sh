#!/bin/bash

# Script to merge all ome/ feature branches and perform a build.
# Merged build will be pushed to ome/merge_build_YYYYMMDD branch
# and includes the generated JAR file

set -e  # Exit immediately if a command exits with a non-zero status

# Configuration
REPO_DIR=$(pwd)
TEMP_BRANCH="merge_build_$(date +%Y%m%d)"
BASE_BRANCH="master"

# Function to display messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to clean up on error
cleanup() {
    log "Error occurred. Cleaning up..."
    git checkout $BASE_BRANCH
    git branch -D $TEMP_BRANCH 2>/dev/null || true
    exit 1
}

# Set up error handling
trap cleanup ERR

# Start process
log "Starting merge of ome/ branches (excluding master)"
log "Working directory: $REPO_DIR"

# Check if ome remote exists, add it if not
if ! git remote | grep -q "^ome$"; then
    log "Adding ome remote: git@github.com:ome/omero-zarr-pixel-buffer.git"
    git remote add ome git@github.com:ome/omero-zarr-pixel-buffer.git
fi

# Make sure we're on master branch and up to date
log "Checking out and updating $BASE_BRANCH branch"
git fetch --all
git checkout $BASE_BRANCH
git pull ome $BASE_BRANCH

# Create a new temporary branch for the merge
log "Creating temporary branch: $TEMP_BRANCH"
git checkout -b $TEMP_BRANCH

# Get all remote ome/ branches except master
log "Finding ome/ branches to merge"
OME_BRANCHES=$(git branch -r | grep "ome/" | grep -v "ome/master" | sed 's/^[ \t]*//')

if [ -z "$OME_BRANCHES" ]; then
    log "No ome/ branches found to merge (excluding master)"
    cleanup
fi

# Display branches that will be merged
log "The following branches will be merged:"
echo "$OME_BRANCHES" | sed 's/^/  - /'

# Merge each branch
for branch in $OME_BRANCHES; do
    log "Merging $branch"
    git merge --no-edit $branch || {
        log "Merge conflict detected in $branch"
        log "Please resolve conflicts manually and then run the build"
        log "You can continue with: git merge --continue"
        log "Or abort with: git merge --abort"
        exit 1
    }
done

# Run the build
log "All branches merged successfully. Running build..."
./gradlew build

# Copy the generated JAR file to a more accessible location
JAR_FILE=$(find ./build/libs -name "omero-zarr-pixel-buffer-*-SNAPSHOT.jar" | sort -V | tail -n 1)
JAR_FILENAME=$(basename "$JAR_FILE")
cp "$JAR_FILE" ./
log "Generated JAR file: $JAR_FILENAME"

# Add JAR file to git
git add "$JAR_FILENAME"
git commit -m "Add generated JAR file: $JAR_FILENAME"
log "Committed JAR file to git"

# Push to GitHub
git push ome $TEMP_BRANCH

# Show success message
log "Build completed and branch pushed to ome/$TEMP_BRANCH."
log "The merged branches are now in the $TEMP_BRANCH branch."
log "JAR file is available at: https://raw.githubusercontent.com/ome/omero-zarr-pixel-buffer/refs/heads/$TEMP_BRANCH/$JAR_FILENAME"
