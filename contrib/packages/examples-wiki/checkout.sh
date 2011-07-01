#!/bin/bash

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
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}-${revision}.tar.gz"
  newfoldername="${folder}-${revision}"
  echo "making ${tarfile} ..."
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs --exclude=${newfoldername}/contrib/jetty-libs/jsp/*.jar --exclude=${newfoldername}/contrib/jetty-libs/*.jar && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${package_folder}
  sed -e "s/%define pkg_version .*/%define pkg_version ${revision}/g" \
      < ${sourcefolder}/scalaris-svn-examples-wiki.spec > ./scalaris-svn-examples-wiki.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${package_folder}
  sed -e "s/Version: 1-1/Version: ${revision}-1/g" \
      -e "s/scalaris-svn-examples-wiki\\.orig\\.tar\\.gz/scalaris-svn-examples-wiki-${revision}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-svn-examples-wiki\\.diff\\.tar\\.gz/scalaris-svn-examples-wiki-${revision}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-svn-examples-wiki.dsc  > ./scalaris-svn-examples-wiki.dsc && \
  sed -e "s/(1-1)/(${revision}-1)/g" \
      < ${sourcefolder}/debian.changelog                > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                      ./debian.control && \
  cp  ${sourcefolder}/debian.rules                        ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris-svn-examples-wiki-tomcat6.files \
                                                          ./debian.scalaris-svn-examples-wiki-tomcat6.files
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
