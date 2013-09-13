#/bin/bash
# This script searches for an OtpErlang-*.jar in the lib folder.
# Once found, it adds the jar to the Scalaris maven repository.
# This should be called by the command "maven deploy".

file=$(ls lib/OtpErlang-*)
version=$(basename $file .jar | cut -d "-" -f2,3)
mvn deploy:deploy-file  -Dfile="$file" \
    -Dversion="$version" -DgroupId="org.erlang.otp" -DartifactId="jinterface" \
    -Dpackaging="jar" -DrepositoryId="scalaris-repo" -Durl="file:../../maven"
