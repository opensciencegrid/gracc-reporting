#!/bin/bash
# Wrapper to set the correct env variables and build the docker container for gracc-reporting

START=${PWD}
SETUP_DOT_PY=${PWD}/setup.py

VERSION=`grep -E "version=\'.+\'\,$" ${SETUP_DOT_PY}`  # VERSION will be something like "version='2.0'," without surrounding "
VERSION=${VERSION%\'*}	# Remove trailing quotation, comma
VERSION=${VERSION#*\'}	# Remove everything up until the first quotation

if [[ "x$VERSION" == "x" ]] ; then 
	echo "Version is not properly set in the setup.py file.  Exiting"
	exit 1
else 
	echo "VERSION $VERSION"
fi

DOCKER=`which docker`
echo "Docker executable found at $DOCKER"

if [[ "x$DOCKER" == "x" ]] ; then 
	echo "Could not find docker executable.  Exiting"
	exit 1
fi

DOCKERIMAGEPREFIX="opensciencegrid/gracc-reporting"
DOCKERIMAGE="${DOCKERIMAGEPREFIX}:${VERSION}"
echo "Will build image $DOCKERIMAGE"

echo "Building docker image"

# Get the script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

$DOCKER build --build-arg version=$VERSION . -t $DOCKERIMAGE -f $DIR/Dockerfile

STATUS=$?

if [[ $STATUS != 0 ]] ; then 
	echo "Error building Docker image"
	exit $STATUS
fi

echo "Built docker image $DOCKERIMAGE successfully"

$DOCKER push $DOCKERIMAGE

echo "Pushed docker image $DOCKERIMAGE successfully"

exit 0
