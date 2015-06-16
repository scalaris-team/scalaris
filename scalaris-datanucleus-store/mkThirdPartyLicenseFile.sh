#!/bin/bash -e


mvn license:aggregate-add-third-party -PlicenseReport
cp target/generated-sources/license/THIRD-PARTY.txt Licences-Third-Party.txt 

