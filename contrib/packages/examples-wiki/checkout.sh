#!/bin/bash

SCALARIS_VERSION="0.4.0"
date=`date +"%Y%m%d"`
name="scalaris-examples-wiki" # folder base name (without version)
pkg_version=${1:-"$SCALARIS_VERSION"}
branchversion=${2:-"0.4"}
url="http://scalaris.googlecode.com/svn/branches/${branchversion}/contrib/wikipedia"
package_url="http://scalaris.googlecode.com/svn/branches/${branchversion}/contrib/packages/examples-wiki"
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

package_folder="./packaging"

if [ ! -d ${package_folder} ]; then
  echo "checkout ${package_url} -> ${package_folder} ..."
  svn checkout ${package_url} ${package_folder}
  result=$?
else
  echo "update ${package_url} -> ${package_folder} ..."
  svn update ${package_folder}
  result=$?
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}-${pkg_version}.tar.gz"
  newfoldername="${folder}-${pkg_version}"
  echo "making ${tarfile} ..."
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs --exclude=${newfoldername}/contrib/jetty-libs/jsp/*.jar --exclude=${newfoldername}/contrib/jetty-libs/*.jar && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${package_folder}
  sed -e "s/%define pkg_version .*/%define pkg_version ${pkg_version}/g" \
      < ${sourcefolder}/scalaris-examples-wiki.spec > ./scalaris-examples-wiki.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${package_folder}
  sed -e "s/Version: 1-1/Version: ${pkg_version}-1/g" \
      -e "s/scalaris-examples-wiki\\.orig\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-examples-wiki\\.diff\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-examples-wiki.dsc  > ./scalaris-examples-wiki.dsc && \
  sed -e "s/(1-1)/(${pkg_version}-1)/g" \
      < ${sourcefolder}/debian.changelog            > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                  ./debian.control && \
  cp  ${sourcefolder}/debian.rules                    ./debian.rules
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
