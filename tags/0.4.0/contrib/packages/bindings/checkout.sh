#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-bindings" # folder base name (without version)
rpmversion=${1:-"0.4.0"}
debversion="${rpmversion}-1"
branchversion=${2:-"0.4.0"}
url="http://scalaris.googlecode.com/svn/tags/${branchversion}"
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

if [ ${result} -eq 0 ]; then
  tarfile="${folder}.tar.gz"
  echo "making ${tarfile} ..."
  tar -czf ${tarfile} ${folder} --exclude-vcs --exclude=${folder}/src --exclude=${folder}/test --exclude=${folder}/include --exclude=${folder}/contrib --exclude=${folder}/user-dev-guide --exclude=${folder}/doc --exclude=${folder}/docroot
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/bindings
  sed -e "s/%define pkg_version .*/%define pkg_version ${rpmversion}/g" \
      < ${sourcefolder}/scalaris-bindings.spec         > ./scalaris-bindings.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages/bindings
  sed -e "s/Version: 0.4.0-1/Version: ${debversion}/g" \
      -e "s/scalaris-bindings\\.orig\\.tar\\.gz/scalaris-bindings-${rpmversion}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-bindings\\.diff\\.tar\\.gz/scalaris-bindings-${rpmversion}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-bindings.dsc          > ./scalaris-bindings.dsc && \
  sed -e "s/(0.4.0-1)/(${debversion})/g" \
      < ${sourcefolder}/debian.changelog               > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                     ./debian.control && \
  cp  ${sourcefolder}/debian.rules                       ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris-java.files         ./debian.scalaris-java.files && \
  cp  ${sourcefolder}/debian.scalaris-java.conffiles     ./debian.scalaris-java.conffiles && \
  cp  ${sourcefolder}/debian.scalaris-java.postrm        ./debian.scalaris-java.postrm && \
  cp  ${sourcefolder}/debian.scalaris-java.postinst      ./debian.scalaris-java.postinst && \
  cp  ${sourcefolder}/debian.python-scalaris.files       ./debian.python-scalaris.files && \
  cp  ${sourcefolder}/debian.python3-scalaris.files      ./debian.python3-scalaris.files && \
  cp  ${sourcefolder}/debian.scalaris-ruby1.8.files      ./debian.scalaris-ruby1.8.files
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
