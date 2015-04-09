#!/bin/bash

SCALARIS_VERSION="0.7.2+git"
date=`date +"%Y%m%d.%H%M"`
name="scalaris-examples-wiki" # folder base name (without version)
url="https://github.com/scalaris-team/scalaris.git"
deletefolder=0 # set to 1 to delete the folder the repository is checked out to

#####

folder="./${name}"

if [ ! -d "${folder}" ]; then
  echo "checkout ${url} -> ${folder} ..."
  git clone "${url}" "${folder}"
  result=$?
else
  echo "update ${url} -> ${folder} ..."
  cd "${folder}"
  git pull
  result=$?
  cd - >/dev/null
fi

if ! diff -q checkout.sh "${folder}/contrib/packages/examples-wiki/checkout.sh" > /dev/null ; then
  echo "checkout-script changed - re-run ./checkout.sh"
  cp "${folder}/contrib/packages/examples-wiki/checkout.sh" ./ ; exit 1
fi

if [ ${result} -eq 0 ]; then
  echo -n "get git revision ..."
  revision=`cd "${folder}" && git log --pretty=format:'%h' -n 1`
  result=$?
  echo " ${revision}"
  pkg_version="${SCALARIS_VERSION}${date}.${revision}"
fi

if [ ${result} -eq 0 ]; then
  tarfile="${folder}-${pkg_version}.tar.gz"
  newfoldername="${folder}-${pkg_version}"
  echo "making ${tarfile} ..."
  mv "${folder}/contrib/wikipedia" "${newfoldername}" && tar -czf "${tarfile}" "${newfoldername}" --exclude-vcs && mv "${newfoldername}" "${folder}/contrib/wikipedia"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder="${folder}/contrib/packages/examples-wiki"
  sed -e "s/%define pkg_version .*/%define pkg_version ${pkg_version}/g" \
      < "${sourcefolder}/scalaris-examples-wiki.spec"  > ./scalaris-examples-wiki.spec && \
  cp  "${sourcefolder}/scalaris-examples-wiki.changes"   ./scalaris-examples-wiki.changes && \
  cp  "${sourcefolder}/scalaris-examples-wiki-rpmlintrc" ./scalaris-examples-wiki-rpmlintrc
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting Debian package files ..."
  sourcefolder="${folder}/contrib/packages/examples-wiki"
  sed -e "s/Version: .*-.*/Version: ${pkg_version}-1/g" \
      -e "s/scalaris-examples-wiki\\.orig\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.orig\\.tar\\.gz/g" \
      -e "s/scalaris-examples-wiki\\.diff\\.tar\\.gz/scalaris-examples-wiki-${pkg_version}\\.diff\\.tar\\.gz/g" \
      < ${sourcefolder}/scalaris-examples-wiki.dsc  > ./scalaris-examples-wiki.dsc && \
  sed -e "0,/(.*-.*)/s//(${pkg_version}-1)/" \
      -e "0,/ -- Nico Kruber <kruber@zib.de>  .*/s// -- Nico Kruber <kruber@zib.de>  `LANG=C date -R`/" \
      < "${sourcefolder}/debian.changelog"            > ./debian.changelog && \
  cp  "${sourcefolder}/debian.compat"                   ./debian.compat && \
  cp  "${sourcefolder}/debian.control"                  ./debian.control && \
  cp  "${sourcefolder}/debian.rules"                    ./debian.rules && \
  cp  "${sourcefolder}/debian.scalaris-examples-wiki-tomcat5.conffiles" \
                                                      ./debian.scalaris-examples-wiki-tomcat5.conffiles && \
  cp  "${sourcefolder}/debian.scalaris-examples-wiki-tomcat6.conffiles" \
                                                      ./debian.scalaris-examples-wiki-tomcat6.conffiles

  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf "${folder}"
fi
