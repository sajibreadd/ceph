#!/bin/bash -ex
# vim: ts=4 sw=4 expandtab

# Modified by croit to use the locally built packages.
# No arguments are accepted.

D=$(dirname $0)
CFILE=${1:-$D/Containerfile}
shift || true

usage() {
    cat << EOF
$0 [containerfile] (defaults to 'Containerfile')

The current directory must be the ceph.git repository where the package build
has just happened.

No other arguments or environment variables are accepted.
EOF
}

CI_CONTAINER=false
NO_PUSH=true
FLAVOR=${FLAVOR:-default}
# default: current checked-out branch
BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}
# default: current checked-out branch
CEPH_SHA1=${CEPH_SHA1:-$(git rev-parse HEAD)}
# default: build host arch
ARCH=${ARCH:-$(uname -m)}
if [[ "${ARCH}" == "aarch64" ]] ; then ARCH=arm64; fi
REPO_ARCH=amd64
if [[ "${ARCH}" = arm64 ]] ; then
    REPO_ARCH=arm64
fi

VERSION=${VERSION:-$(git describe)}

# check for existence of all required variables
: "${CI_CONTAINER:?}"
: "${FLAVOR:?}"
: "${BRANCH:?}"
: "${CEPH_SHA1:?}"
: "${ARCH:?}"
: "${VERSION:?}"

CEPH_GIT_REPO=/ceph

# BRANCH will be, say, origin/main.  remove <remote>/
BRANCH=${BRANCH##*/}

podman build --pull=newer --squash -f $CFILE -t build.sh.output \
    --build-arg FROM_IMAGE=${FROM_IMAGE:-quay.io/centos/centos:stream9} \
    --build-arg CEPH_SHA1=${CEPH_SHA1} \
    --build-arg CEPH_GIT_REPO=${CEPH_GIT_REPO} \
    --build-arg CEPH_REF=${BRANCH:-main} \
    --build-arg OSD_FLAVOR=${FLAVOR:-default} \
    --build-arg CI_CONTAINER=${CI_CONTAINER:-default} \
    rpmbuild/RPMS \
    2>&1 

image_id=$(podman image ls localhost/build.sh.output --format '{{.ID}}')

# grab useful image attributes for building the tag
#
# the variable settings are prefixed with "export CEPH_CONTAINER_" so that
# an eval or . can be used to put them into the environment
#
# PATH is removed from the output as it would cause problems for this
# parent script and its children
#
# notes:
#
# we want .Architecture and everything in .Config.Env
#
# printf will not accept "\n" (is this a podman bug?)
# so construct vars with two calls to podman inspect, joined by a newline,
# so that vars will get the output of the first command, newline, output
# of the second command
#
vars="$(podman inspect -f '{{printf "export CEPH_CONTAINER_ARCH=%v" .Architecture}}' ${image_id})
$(podman inspect -f '{{range $index, $value := .Config.Env}}export CEPH_CONTAINER_{{$value}}{{println}}{{end}}' ${image_id})"
vars="$(echo "${vars}" | grep -v PATH)"
eval ${vars}

# remove everything up to and including the last slash
fromtag=${CEPH_CONTAINER_FROM_IMAGE##*/}
# translate : to -
fromtag=${fromtag/:/-}
builddate=$(date +%Y%m%d)
local_tag=${fromtag}-${CEPH_CONTAINER_CEPH_REF}-${CEPH_CONTAINER_ARCH}-${builddate}

repopath=localhost/ceph

version_tag=${repopath}:${VERSION}-${builddate}
podman tag ${image_id} ${version_tag}
podman save ${version_tag} --format oci-archive -o rpmbuild/ceph-container-${VERSION}-${builddate}.oci.tar
