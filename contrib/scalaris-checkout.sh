#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris" # folder base name (without version)
url="http://scalaris.googlecode.com/svn/trunk/"

#####

echo "checkout ${url} -> ${name}-${date} ..."
svn checkout ${url} ./${name}-${date}

echo -n "get svn revision ..."
revision=`svn info ./${name}-${date} --xml | grep revision | cut -d '"' -f 2 | head -n 1`
echo " ${revision}"
# not safe in other languages than English:
# revision=`svn info ${name} | grep "Revision:" | cut -d ' ' -f 4`

echo "rename ${name}-${date} -> ${name}-svn${revision} ..."
mv ./${name}-${date} ./${name}-svn${revision}

echo "making ${name}-svn${revision}.tar.bz2 ..."
tar -cjf ./${name}-svn${revision}.tar.bz2 ./${name}-svn${revision} --exclude-vcs

echo "extracting .spec file ..."
#cp ${name}-${revision}/contrib/scalaris.spec ./scalaris.spec.svn
sed "s/%define pkg_version 0.0.1/%define pkg_version svn${revision}/g" < ./${name}-svn${revision}/contrib/scalaris.spec > ./scalaris.spec

echo "removing ${name}-svn${revision} ..."
rm -rf ./${name}-svn${revision}
