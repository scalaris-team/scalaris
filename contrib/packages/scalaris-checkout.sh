#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris" # folder base name (without version)
tagversion=${1:-"0.3"}
url="http://scalaris.googlecode.com/svn/branches/${tagversion}"
deletefolder=0 # set to 1 to delete the folder the repository is checked out to

#####

result=0
folder="./${name}-${tagversion}"

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
  sourcefolder=${folder}/contrib/packages
  sed -e "s/%define pkg_version .*/%define pkg_version ${tagversion}/g" \
      < ${sourcefolder}/scalaris.spec > ./scalaris.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages
  sed -e "s/0.3.0/${tagversion}/g" \
      < ${sourcefolder}/scalaris.dsc          > ./scalaris.dsc && \
  sed -e "s/(0.3.0-/(${tagversion}-/g" \
      < ${sourcefolder}/debian.changelog      > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control            ./debian.control && \
  cp  ${sourcefolder}/debian.rules              ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris-doc.files ./debian.scalaris-doc.files && \
  cp  ${sourcefolder}/debian.scalaris.conffiles ./debian.scalaris.conffiles && \
  cp  ${sourcefolder}/debian.scalaris.files     ./debian.scalaris.files && \
  cp  ${sourcefolder}/debian.scalaris.postinst  ./debian.scalaris.postinst && \
  cp  ${sourcefolder}/debian.scalaris.postrm    ./debian.scalaris.postrm

  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
