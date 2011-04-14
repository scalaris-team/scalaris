#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-svn-bindings" # folder base name (without version)
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
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs --exclude=${newfoldername}/src --exclude=${newfoldername}/test --exclude=${newfoldername}/include --exclude=${newfoldername}/contrib --exclude=${newfoldername}/user-dev-guide --exclude=${newfoldername}/doc --exclude=${newfoldername}/docroot && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/bindings
  sed -e "s/%define pkg_version .*/%define pkg_version ${revision}/g" \
      -e "s/Name:           scalaris-bindings/Name:           scalaris-svn-bindings/g" \
      -e "s/%package -n scalaris-java/%package -n scalaris-svn-java\nConflicts:  scalaris-java\nConflicts:  scalaris-client/g" \
      -e "s/%package -n ruby-scalaris/%package -n ruby-scalaris-svn/g" \
      -e "s/%package -n python-scalaris/%package -n python-scalaris-svn/g" \
      -e "s/%package -n python3-scalaris/%package -n python3-scalaris-svn/g" \
      -e "s/%description -n scalaris-java/%description -n scalaris-svn-java/g" \
      -e "s/%description -n ruby-scalaris/%description -n ruby-scalaris-svn/g" \
      -e "s/%description -n python-scalaris/%description -n python-scalaris-svn/g" \
      -e "s/%description -n python3-scalaris/%description -n python3-scalaris-svn/g" \
      -e "s/%files -n scalaris-java/%files -n scalaris-svn-java/g" \
      -e "s/%files -n ruby-scalaris/%files -n ruby-scalaris-svn/g" \
      -e "s/%files -n python-scalaris/%files -n python-scalaris-svn/g" \
      -e "s/%files -n python3-scalaris/%files -n python3-scalaris-svn/g" \
      < ${sourcefolder}/scalaris-bindings.spec > ./scalaris-svn-bindings.spec
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder=${folder}/contrib/packages/bindings
  sed -e "s/scalaris-bindings/scalaris-svn-bindings/g" \
      -e "s/0.2.3/${revision}/g" \
      < ${sourcefolder}/scalaris-bindings.dsc > ./scalaris-svn-bindings.dsc && \
  sed -e "s/scalaris-bindings/scalaris-svn-bindings/g" \
      -e "s/stable/unstable/g" \
      -e "s/(0.2.3-/(${revision}-/g" \
      < ${sourcefolder}/debian.changelog > ./debian.changelog && \
  sed -e "s/Source: scalaris-bindings/Source: scalaris-svn-bindings/g" \
      -e "s/Package: scalaris-java\$/Package: scalaris-svn-java\nConflicts: scalaris-java, scalaris-client/g" \
      < ${sourcefolder}/debian.control > ./debian.control && \
  sed -e 's/BUILD_DIR:=$(CURDIR)\/debian\/scalaris/BUILD_DIR:=$(CURDIR)\/debian\/scalaris-svn/g' \
      < ${sourcefolder}/debian.rules > ./debian.rules && \
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
