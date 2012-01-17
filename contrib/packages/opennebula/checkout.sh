#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-svn-one" # folder base name (without version)
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
  mv "${folder}" "${newfoldername}" && tar -czf ${tarfile} ${newfoldername} --exclude-vcs --exclude="${newfoldername}/src" --exclude="${newfoldername}/test" --exclude="${newfoldername}/include" --exclude="${newfoldername}/contrib/benchmark" --exclude="${newfoldername}/contrib/compat" --exclude="${newfoldername}/contrib/contrail" --exclude="${newfoldername}/contrib/log4erl" --exclude="${newfoldername}/contrib/packages" --exclude="${newfoldername}/contrib/pubsub" --exclude="${newfoldername}/contrib/wikipedia" --exclude="${newfoldername}/contrib/yaws" --exclude="${newfoldername}/user-dev-guide" --exclude="${newfoldername}/java-api/*/*" --exclude="${newfoldername}/python-api" --exclude="${newfoldername}/ruby-api" --exclude="${newfoldername}/doc" --exclude="${newfoldername}/docroot" && mv "${newfoldername}" "${folder}"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/opennebula
  sed -e "s/%define pkg_version .*/%define pkg_version ${revision}/g" \
      < ${sourcefolder}/scalaris-svn-one.spec     > ./scalaris-svn-one.spec
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
