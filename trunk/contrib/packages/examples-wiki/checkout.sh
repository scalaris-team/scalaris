#!/bin/bash

SCALARIS_VERSION="0.5.0+svn"
date=`date +"%Y%m%d"`
name="scalaris-examples-wiki" # folder base name (without version)
url="http://scalaris.googlecode.com/svn/trunk/contrib/wikipedia"
package_url="http://scalaris.googlecode.com/svn/trunk/contrib/packages/examples-wiki"
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
  sourcefolder=${package_folder}
  sed -e "s/%define pkg_version .*/%define pkg_version ${pkg_version}/g" \
      < ${sourcefolder}/scalaris-examples-wiki.spec  > ./scalaris-examples-wiki.spec && \
  cp  ${sourcefolder}/scalaris-examples-wiki.changes   ./scalaris-examples-wiki.changes
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${package_folder}
  sed -e "s/Version: .*-.*/Version: ${pkg_version}-1/g" \
      -e "s/scalaris-examples-wiki\\.orig\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-examples-wiki\\.diff\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-examples-wiki.dsc  > ./scalaris-examples-wiki.dsc && \
  sed -e "0,/(.*-.*)/s//(${pkg_version}-1)/" \
      -e "0,/ -- Nico Kruber <kruber@zib.de>  .*/s// -- Nico Kruber <kruber@zib.de>  `LANG=C date -R`/" \
      < ${sourcefolder}/debian.changelog           > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                  ./debian.control && \
  cp  ${sourcefolder}/debian.rules                    ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris-examples-wiki-tomcat5.conffiles \
                                                     ./debian.scalaris-examples-wiki-tomcat5.conffiles && \
  cp  ${sourcefolder}/debian.scalaris-examples-wiki-tomcat6.conffiles \
                                                     ./debian.scalaris-examples-wiki-tomcat6.conffiles

  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
