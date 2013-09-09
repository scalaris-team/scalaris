#!/bin/bash
# Deploy hook functions to glue maven and svn together
# This should be called by the command "maven deploy".

# scalaris maven repo
url="https://scalaris.googlecode.com/svn/maven/"
# maven repo checkout folder
folder=".maven"

checkout () {
    # check out if maven folder doesn't exist
    # otherwise update
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
        echo "Maven repository has been updated locally."
    else
        echo "Maven repository couldn't be updated."
        exit 1
    fi
}

commit () {

    # put latest erlang jinterface jar into the repository
    file=$(ls lib/OtpErlang-*)
    version=$(basename $file .jar | cut -d "-" -f2,3)
    mvn deploy:deploy-file  -Dfile="$file" \
        -Dversion="$version" -DgroupId="org.erlang.otp" -DartifactId="jinterface" \
        -Dpackaging="jar" -DrepositoryId="scalaris" -Durl="file:$folder"

    # update the remote maven repository
    echo -n "Do you want to update the remote maven repository? [y/N] "
    read -e answer
    if [[ ${answer} == "y" ]]; then
        svn add --force ${folder}/*
        svn commit ${folder}
    fi
}


if [[ $1 == "checkout" ]]; then
    checkout
elif [[ $1 == "commit" ]]; then
    commit
else
    echo "Missing an argument."
    exit 1
fi
