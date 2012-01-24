#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-examples-wiki" # folder base name (without version)
rpmversion=${1:-"0.4.0"}
debversion="${rpmversion}-1"
branchversion=${2:-"0.4"}
url="http://scalaris.googlecode.com/svn/branches/${branchversion}/contrib/wikipedia"
package_url="http://scalaris.googlecode.com/svn/branches/${branchversion}/contrib/packages/examples-wiki"
deletefolder=0 # set to 1 to delete the folder the repository is checked out to

#####

result=0
folder="./${name}-${rpmversion}"

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
  tarfile="${folder}.tar.gz"
  echo "making ${tarfile} ..."
  tar -czf ${tarfile} ${folder} --exclude-vcs --exclude=${folder}/contrib/jetty-libs/jsp/*.jar --exclude=${folder}/contrib/jetty-libs/*.jar
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${package_folder}
  sed -e "s/%define pkg_version .*/%define pkg_version ${rpmversion}/g" \
      -e "s/scalaris-java >= 0.4.0/scalaris-java >= ${rpmversion}/g" \
      < ${sourcefolder}/scalaris-examples-wiki.spec > ./scalaris-examples-wiki.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${package_folder}
  sed -e "s/Version: 0.4.0-1/Version: ${debversion}/g" \
      -e "s/scalaris-java (>= 0.4.0-1)/scalaris-java (>= ${debversion})/g" \
      -e "s/scalaris-examples-wiki\\.orig\\.tar\\.gz/scalaris-examples-wiki-${rpmversion}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-examples-wiki\\.diff\\.tar\\.gz/scalaris-examples-wiki-${rpmversion}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-examples-wiki.dsc  > ./scalaris-examples-wiki.dsc && \
  sed -e "s/(0.4.0-1)/(${debversion})/g" \
      < ${sourcefolder}/debian.changelog            > ./debian.changelog && \
  sed -e "s/scalaris-java (= 0.4.0-1)/scalaris-java (= ${debversion})/g" \
      < ${sourcefolder}/debian.control              > ./debian.control && \
  cp  ${sourcefolder}/debian.rules                    ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris-examples-wiki-tomcat5.files \
                                                      ./debian.scalaris-examples-wiki-tomcat5.files && \
  cp  ${sourcefolder}/debian.scalaris-examples-wiki-tomcat6.files \
                                                      ./debian.scalaris-examples-wiki-tomcat6.files
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
