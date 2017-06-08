#!/bin/sh
#
# promote the package named in argv[1] to the repo level in argv[2]
#

if [ $# -lt 2 ]; then
  echo "Usage: $0 package-name [qe|staging|prod]" 1>&2
  echo " e.g.: $0 volga-2.0.58 prod" 1>&2
  exit 1
fi

PKG=$1
LVL=$2

case $LVL in
  prod)
    PLVL='staging'
    ;;
  staging)
    PLVL='qe'
    ;;
  qe)
    PLVL='dev'
    ;;
  *)
    echo "$0: Unknown level $LVL - Choices are qe, staging, prod" 1>&2
    exit 2
    ;;
esac

ROOT=/var/repo
FILENAME="$PKG-1.noarch.rpm"
BASERPM="$ROOT/$PLVL/$FILENAME"
LINKRPM="$ROOT/$LVL/$FILENAME"

if [ ! -e $BASERPM ]; then
  echo "rpmfile not found: $BASERPM" 1>&2
  exit 2
fi

if [ -e $LINKRPM ]; then
  echo "rpmfile already exists: $BASERPM" 1>&2
  exit 3
fi

ln -s $BASERPM $LINKRPM
createrepo --update "$ROOT/$LVL"
