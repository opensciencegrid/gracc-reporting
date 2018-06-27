#!/bin/bash
# Wrapper to set the correct env variables and build the docker container for gracc-reporting

# Build the RPM
START=${PWD}
SRCSPEC=${START}/gracc-reporting.spec
VERSION=`grep -m 1 version ${SRCSPEC} | awk '{print $3}'`

if [[ "x$VERSION" == "x" ]] ; then 
	echo "Version is not properly set in spec file.  Exiting"
	exit 1
else 
	echo "VERSION $VERSION"
fi

SRC=${START}/../dist/gracc-reporting-${VERSION}.tar.gz
TARFILENAME=${SRC##*/}

DOCKER=`which docker`
echo "Docker executable found at $DOCKER"

if [[ "x$DOCKER" == "x" ]] ; then 
	echo "Could not find docker executable.  Exiting"
	exit 1
fi

DOCKERIMAGEPREFIX="shreyb/gracc-reporting"
DOCKERIMAGE="${DOCKERIMAGEPREFIX}:${VERSION}"
echo "Will build image $DOCKERIMAGE"

cp ${SRC} ${START}/${TARFILENAME}
echo "Copied source to build dir"

function cleanup {
	test ! -f ${START}/${TARFILENAME} || rm ${START}/${TARFILENAME}
}
trap cleanup EXIT SIGHUP SIGINT SIGTERM


echo "Building docker image"
$DOCKER build --build-arg version=$VERSION . -t $DOCKERIMAGE

STATUS=$?

if [[ $STATUS != 0 ]] ; then 
	echo "Error building Docker image"
	exit $STATUS
fi

echo "Built docker image $DOCKERIMAGE successfully"

$DOCKER push $DOCKERIMAGE

echo "Pushed docker image $DOCKERIMAGE successfully"

exit 0
