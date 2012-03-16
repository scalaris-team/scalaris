#!/bin/bash

SCALARIS_VERSION="0.4.1+svn"
date=`date +"%Y%m%d"`
name="scalaris" # folder base name (without version)
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
  pkg_version="${SCALARIS_VERSION}${revision}"
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}-${pkg_version}.tar.gz"
  newfoldername="${folder}-${pkg_version}"
  echo "making ${tarfile} ..."
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/main
  sed -e "s/%define pkg_version .*/%define pkg_version ${pkg_version}/g" \
      < ${sourcefolder}/scalaris.spec              > ./scalaris.spec && \
  cp  ${sourcefolder}/scalaris-rpmlintrc             ./scalaris-rpmlintrc
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages/main
  sed -e "s/Version: 1-1/Version: ${pkg_version}-1/g" \
      -e "s/scalaris\\.orig\\.tar\\.gz/scalaris-${pkg_version}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris\\.diff\\.tar\\.gz/scalaris-${pkg_version}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris.dsc               > ./scalaris.dsc && \
  sed -e "s/(1-1)/(${pkg_version}-1)/g" \
      < ${sourcefolder}/debian.changelog           > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                 ./debian.control && \
  cp  ${sourcefolder}/debian.rules                   ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris.install        ./debian.scalaris.install && \
  cp  ${sourcefolder}/debian.scalaris.conffiles      ./debian.scalaris.conffiles && \
  cp  ${sourcefolder}/debian.scalaris.postrm         ./debian.scalaris.postrm && \
  cp  ${sourcefolder}/debian.scalaris.postinst       ./debian.scalaris.postinst && \
  cp  ${sourcefolder}/debian.scalaris-doc.install    ./debian.scalaris-doc.install
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
