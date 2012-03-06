#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris" # folder base name (without version)
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
  tar -czf ${tarfile} ${folder} --exclude-vcs
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/main
  sed -e "s/%define pkg_version .*/%define pkg_version ${rpmversion}/g" \
      < ${sourcefolder}/scalaris.spec              > ./scalaris.spec && \
  cp  ${sourcefolder}/scalaris-rpmlintrc             ./scalaris-rpmlintrc
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages/main
  sed -e "s/Version: 0.4.0-1/Version: ${debversion}/g" \
      -e "s/scalaris\\.orig\\.tar\\.gz/scalaris-${rpmversion}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris\\.diff\\.tar\\.gz/scalaris-${rpmversion}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris.dsc               > ./scalaris.dsc && \
  sed -e "s/(0.4.0-1)/(${debversion})/g" \
      < ${sourcefolder}/debian.changelog           > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                 ./debian.control && \
  cp  ${sourcefolder}/debian.rules                   ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris.files          ./debian.scalaris.files && \
  cp  ${sourcefolder}/debian.scalaris.conffiles      ./debian.scalaris.conffiles && \
  cp  ${sourcefolder}/debian.scalaris.postrm         ./debian.scalaris.postrm && \
  cp  ${sourcefolder}/debian.scalaris.postinst       ./debian.scalaris.postinst && \
  cp  ${sourcefolder}/debian.scalaris-doc.files      ./debian.scalaris-doc.files
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
