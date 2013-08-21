#!/bin/bash
#########
### Generates the Scalaris Maven repository
###
### Input: pom.xml
###
### The script must be supplied with the maven pom.xml that contains
### the dependencies to build the project.  The goal of this script is
### to create a maven repository for Scalaris specific dependencies
### that cannot be found in any official maven repositories. Commonly
### these dependencies will be the jinterface and the java-api of
### Scalaris. The script will ask for a jar for each dependencies it
### finds. Unneeded dependencies can be skipped.
###
### Example: ./create_repository.sh ../YCSB/scalaris/pom.xml


###
# Checks whether a command exists or not
function checkFor {
  $1 > /dev/null 2>&1
  if [ -z $# ]; then
    echo "$1 not found, thus aborting."
    exit 1;
  fi
}

###
# Check for prerequisites
checkFor "mvn --version"
checkFor "xml2 -v"

if [ ! "$#" -eq 1 ] || [ ! -f $1 ] || [ $(basename $1) != "pom.xml" ]; then
  echo "./create_repository POM.XML"
  echo "Generates a repository in the current directory given the project's pom.xml."
  exit 1
fi

###
# Check if directory is empty
if [ "$(ls -l | grep ^d)" ]; then
  echo "Directories exist. Remove all files except for this script. Aborting."
  exit 1
fi

###
# Add jar to local repository
function addJar {
  if [ -f $1 ];then
    mvn install:install-file -Dfile=$1 -DgroupId=$2 -DartifactId=$3 -Dversion=$4 -Dpackaging=jar -DlocalRepositoryPath=. -DcreateChecksum=true &> /dev/null
  else
    echo "Invalid path! Aborting."
    exit 1
  fi
}

# Assumes a valid pom.xml
OUTPUT=$(xml2 < $1 | grep /project/dependencies | sed 's/.*\///')
OUTPUT="$OUTPUT
dependency"

function readValue {
  echo $line | sed "s/.*=//"
}


# Parse Maven xml
for line in $OUTPUT
do
    if [[ $line == "dependency" && $groupId != "com.yahoo.ycsb" ]]; then
        echo "Jar path for $artifactId-$version, belonging to $groupId: "
        echo "Press Return to skip this dependency. It won't be added to the repository then."
        echo -n "> "
        read -e path
        if [[ $path ]]; then
            echo "Adding $artifactId-$version to the repo."
            addJar $path $groupId $artifactId $version
        fi
    fi
    if [[ $line == groupId* ]]; then
        groupId=$(readValue $line)
    elif [[ $line == artifactId* ]]; then
        artifactId=$(readValue $line)
    elif [[ $line == version* ]]; then
        version=$(readValue $line)
    fi
done

echo "Local repository generated."
echo "Will now do the post-processing"

# Removes the -local part of the maven metadata to make the repo public
find . -name "*metadata-local*" -exec sh -c 'mv {} $(echo {} | sed "s/-local//")' \;

echo "Done."
