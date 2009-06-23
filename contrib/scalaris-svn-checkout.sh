#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-svn" # folder base name (without version)
url="http://scalaris.googlecode.com/svn/trunk/"
deletefolder=0 # set to 1 to delete the folder the repository is checked out to

#####

folder="./${name}"

if [ ! -d ${folder} ]; then
  echo "checkout ${url} -> ${folder} ..."
  svn checkout ${url} ${folder}
  result=$?
else
  echo "update ${url} -> ${folder} ..."
  svn update ${folder}
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo -n "get svn revision ..."
  revision=`svn info ${folder} --xml | grep revision | cut -d '"' -f 2 | head -n 1`
  result=$?
  echo " ${revision}"
  # not safe in other languages than English:
  # revision=`svn info ${name} | grep "Revision:" | cut -d ' ' -f 4`
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}-${revision}.tar.bz2"
  newfoldername="${folder}-${revision}"
  echo "making ${tarfile} ..."
  mv "${folder}" "${newfoldername}" && tar -cjf ${tarfile} ${newfoldername} --exclude-vcs && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  #cp ${name}-${revision}/contrib/scalaris.spec ./scalaris.spec.svn
  sed -e "s/%define pkg_version .*/%define pkg_version ${revision}/g" -e "s/Name:           scalaris/Name:           scalaris-svn\nObsoletes:      scalaris\nProvides:       scalaris = svn%{pkg_version}/g" -e "s/%package java/%package java\nObsoletes:  scalaris-java\nProvides:   scalaris-java = svn%{pkg_version}/g" -e "s/%package doc/%package doc\nObsoletes:  scalaris-doc\nProvides:   scalaris-doc = svn%{pkg_version}/g" -e "s/%package client/%package client\nObsoletes:  scalaris-client\nProvides:   scalaris-client = svn%{pkg_version}/g" < ${folder}/contrib/scalaris.spec > ./scalaris-svn.spec
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
