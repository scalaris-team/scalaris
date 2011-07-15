#!/bin/bash

test -f VERSION || exit 1

version=$(cat VERSION)

OUTPUTFILE="$(pwd)/../scalaris-$version.tar.bz2"
if test -f $OUTPUTFILE
then
   echo Exiting: $OUTPUTFILE already exist
   exit 1
fi
tmpdir=$(mktemp -d)
workdir="$tmpdir/scalaris-$version"
mkdir -p "$workdir"
cp -a * "$workdir/"
cd $workdir
make clean
#rm -Rf lib logs
cd ..
echo "Creating scalaris-$version.tar.bz2"
tar cjf "$OUTPUTFILE" "scalaris-$version" --exclude=.svn
cd
rm -Rf "$tmpdir"
