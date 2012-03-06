#!/bin/bash

date=`date +"%Y%m%d"`
name="scalaris-one" # folder base name (without version)
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
  tar -czf ${tarfile} ${folder} --exclude-vcs --exclude="${folder}/src" --exclude="${folder}/test" --exclude="${folder}/include" --exclude="${folder}/contrib/benchmark" --exclude="${folder}/contrib/compat" --exclude="${folder}/contrib/contrail" --exclude="${folder}/contrib/log4erl" --exclude="${folder}/contrib/packages" --exclude="${folder}/contrib/pubsub" --exclude="${folder}/contrib/wikipedia" --exclude="${folder}/contrib/yaws" --exclude="${folder}/user-dev-guide" --exclude="${folder}/java-api/*/*" --exclude="${folder}/python-api" --exclude="${folder}/ruby-api" --exclude="${folder}/doc" --exclude="${folder}/docroot"
  result=$?
fi

if [ ${result} -eq 0 ]; then
  echo "extracting .spec file ..."
  sourcefolder=${folder}/contrib/packages/opennebula
  sed -e "s/%define pkg_version .*/%define pkg_version ${rpmversion}/g" \
      < ${sourcefolder}/scalaris-one.spec     > ./scalaris-one.spec
  result=$?
fi

if [ ${result} -eq 0 -a ${deletefolder} -eq 1 ]; then
  echo "removing ${folder} ..."
  rm -rf ${folder}
fi
