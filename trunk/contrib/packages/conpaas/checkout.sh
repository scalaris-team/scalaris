#!/bin/bash

SCALARIS_VERSION="0.5.0+svn"
date=`date +"%Y%m%d"`
name="conpaas-scalarix" # folder base name (without version)
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
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs --exclude="${newfoldername}/src" --exclude="${newfoldername}/test" --exclude="${newfoldername}/include" --exclude="${newfoldername}/contrib/benchmark" --exclude="${newfoldername}/contrib/compat" --exclude="${newfoldername}/contrib/contrail" --exclude="${newfoldername}/contrib/log4erl" --exclude="${newfoldername}/contrib/packages" --exclude="${newfoldername}/contrib/pubsub" --exclude="${newfoldername}/contrib/wikipedia" --exclude="${newfoldername}/contrib/yaws" --exclude="${newfoldername}/user-dev-guide" --exclude="${newfoldername}/java-api/*/*" --exclude="${newfoldername}/python-api/*/*" --exclude="${newfoldername}/ruby-api/*/*" --exclude="${newfoldername}/doc" --exclude="${newfoldername}/docroot" && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/conpaas
  sed -e "s/%define pkg_version .*/%define pkg_version ${pkg_version}/g" \
      < ${sourcefolder}/conpaas-scalarix.spec     > ./conpaas-scalarix.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages/conpaas
  sed -e "s/Version: .*-.*/Version: ${pkg_version}-1/g" \
      -e "s/conpaas-scalarix\\.orig\\.tar\\.gz/conpaas-scalarix-${pkg_version}\\.orig\\.tar\\.gz/g" \
      -e "s/conpaas-scalarix\\.diff\\.tar\\.gz/conpaas-scalarix-${pkg_version}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/conpaas-scalarix.dsc           > ./conpaas-scalarix.dsc && \
  sed -e "s/(.*-.*)/(${pkg_version}-1)/g" \
      < ${sourcefolder}/debian.changelog               > ./debian.changelog && \
  cp  ${sourcefolder}/debian.control                     ./debian.control && \
  cp  ${sourcefolder}/debian.rules                       ./debian.rules && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-client.install    ./debian.conpaas-scalarix-one-client.install && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-client.postinst   ./debian.conpaas-scalarix-one-client.postinst && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-frontend.install  ./debian.conpaas-scalarix-one-frontend.install && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-frontend.postinst ./debian.conpaas-scalarix-one-frontend.postinst && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-manager.install   ./debian.conpaas-scalarix-one-manager.install && \
  cp  ${sourcefolder}/debian.conpaas-scalarix-one-manager.postinst  ./debian.conpaas-scalarix-one-manager.postinst

  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
