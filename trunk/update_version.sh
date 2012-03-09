#!/bin/sh
# Copyright 2007-2012 Zuse Institute Berlin
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

VERSION=`cat VERSION`

echo "Setting Scalaris version to ${VERSION}..."
sed -e "s/-define(SCALARIS_VERSION, \".*\")\\./-define(SCALARIS_VERSION, \"${VERSION}\")./g" \
    -i include/scalaris.hrl
sed -e "s/AC_INIT(scalaris, .*, schuett@zib.de)/AC_INIT(scalaris, ${VERSION}, schuett@zib.de)/g" \
    -i configure.ac
sed -e "s/public static final String version = \".*\";/public static final String version = \"${VERSION}\";/g" \
    -i contrib/wikipedia/src/de/zib/scalaris/examples/wikipedia/bliki/WikiServlet.java
sed -e "s/version='.*',/version='${VERSION}',/g" \
    -i python-api/setup.py
echo "done"
