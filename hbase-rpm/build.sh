#!/bin/bash
set -e
set -x

ROOT_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# If not specified, extract the version.
if [[ "X$HBASE_VERSION" = "X" ]]; then
    echo "Must specifiy \$HBASE_VERSION"
    exit 1
fi

# Setup build dir
BUILD_DIR="${ROOT_DIR}/build"
rm -rf $BUILD_DIR
mkdir -p ${BUILD_DIR}/{SOURCES,SPECS,RPMS}
cp -a $ROOT_DIR/sources/* ${BUILD_DIR}/SOURCES/
cp $ROOT_DIR/hbase.spec ${BUILD_DIR}/SPECS/

# Download bin tar built by hbase-assembly
SOURCES_DIR=$BUILD_DIR/SOURCES
mvn dependency:copy \
    -Dartifact=org.apache.hbase:hbase-assembly:${MAVEN_VERSION}:tar.gz:bin \
    -DoutputDirectory=$SOURCES_DIR \
    -DlocalRepositoryDirectory=$SOURCES_DIR \
    -Dtransitive=false
INPUT_TAR=`ls -d $SOURCES_DIR/hbase-assembly-*.tar.gz`

rpmbuild \
    --define "_topdir $BUILD_DIR" \
    --define "input_tar $INPUT_TAR" \
    --define "hbase_version ${HBASE_VERSION}" \
    --define "release ${PKG_RELEASE}%{?dist}" \
    $BUILD_DIR/SPECS/hbase.spec

if [[ -d $RPMS_OUTPUT_DIR ]]; then
    mkdir -p $RPMS_OUTPUT_DIR

    # Move rpms to output dir for upload

    find ${BUILD_DIR}/RPMS -name "*.rpm" -exec mv {} $RPMS_OUTPUT_DIR/ \;
fi
