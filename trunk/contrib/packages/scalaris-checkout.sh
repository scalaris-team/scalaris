#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris" # folder base name (without version)
tagversion=${1:-"0.2.3"}
url="http://scalaris.googlecode.com/svn/tags/${tagversion}"
deletefolder=0 # set to 1 to delete the folder the repository is checked out to

#####

result=1
folder="./${name}-${tagversion}"

if [ ! -d ${folder} ]; then
  echo "checkout ${url} -> ${folder} ..."
  svn checkout ${url} ${folder}
  result=$?
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}.tar.gz"
  echo "making ${tarfile} ..."
  tar -czf ${tarfile} ${folder} --exclude-vcs
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages
  #cp ${name}-${revision}/contrib/scalaris.spec ./scalaris.spec.svn
  sed -e "s/%define pkg_version .*/%define pkg_version ${tagversion}/g" < ${sourcefolder}/scalaris.spec > ./scalaris.spec
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
