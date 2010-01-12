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
  tarfile="${folder}-${revision}.tar.gz"
  newfoldername="${folder}-${revision}"
  echo "making ${tarfile} ..."
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages
  sed -e "s/%define pkg_version .*/%define pkg_version ${revision}/g" \
      -e "s/Name:           scalaris/Name:           scalaris-svn\nConflicts:      scalaris/g" \
      -e "s/%package java/%package java\nConflicts:  scalaris-java/g" \
      -e "s/%package doc/%package doc\nConflicts:  scalaris-doc/g" \
      -e "s/%package client/%package client\nConflicts:  scalaris-client/g" \
      < ${sourcefolder}/scalaris.spec > ./scalaris-svn.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages
  sed -e "s/scalaris/scalaris-svn/g" \
      -e "s/0.2.3/${revision}/g" \
      < ${sourcefolder}/scalaris.dsc > ./scalaris-svn.dsc && \
  sed -e "s/scalaris/scalaris-svn/g" \
      -e "s/stable/unstable/g" \
      < ${sourcefolder}/debian.changelog > ./debian.changelog && \
  sed -e "s/Source: scalaris/Source: scalaris-svn/g" \
      -e "s/Package: scalaris\$/Package: scalaris-svn\nConflicts: scalaris/g" \
      -e "s/Package: scalaris-client\$/Package: scalaris-svn-client\nConflicts: scalaris-client/g" \
      -e "s/Package: scalaris-java\$/Package: scalaris-svn-java\nConflicts: scalaris-java/g" \
      -e "s/Package: scalaris-doc\$/Package: scalaris-svn-doc\nConflicts: scalaris-doc/g" \
      -e 's/scalaris (= ${binary:Version})/scalaris-svn (= ${binary:Version})/g' \
      -e 's/scalaris-java (= ${binary:Version})/scalaris-svn-java (= ${binary:Version})/g' \
      < ${sourcefolder}/debian.control > ./debian.control && \
  sed -e 's/BUILD_DIR:=$(CURDIR)\/debian\/scalaris/BUILD_DIR:=$(CURDIR)\/debian\/scalaris-svn/g' \
      < ${sourcefolder}/debian.rules > ./debian.rules && \
  cp  ${sourcefolder}/debian.scalaris.files          ./debian.scalaris-svn.files && \
  cp  ${sourcefolder}/debian.scalaris.conffiles      ./debian.scalaris-svn.conffiles && \
  cp  ${sourcefolder}/debian.scalaris.postrm         ./debian.scalaris-svn.postrm && \
  cp  ${sourcefolder}/debian.scalaris.postinst       ./debian.scalaris-svn.postinst && \
  cp  ${sourcefolder}/debian.scalaris-client.files   ./debian.scalaris-svn-client.files && \
  cp  ${sourcefolder}/debian.scalaris-doc.files      ./debian.scalaris-svn-doc.files && \
  cp  ${sourcefolder}/debian.scalaris-java.files     ./debian.scalaris-svn-java.files && \
  cp  ${sourcefolder}/debian.scalaris-java.conffiles ./debian.scalaris-svn-java.conffiles && \
  cp  ${sourcefolder}/debian.scalaris-java.postrm    ./debian.scalaris-svn-java.postrm && \
  cp  ${sourcefolder}/debian.scalaris-java.postinst  ./debian.scalaris-svn-java.postinst
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
