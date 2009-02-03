#!/bin/sh
# Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

erl -setcookie "chocolate chip cookie" -pa ../contrib/yaws/ebin -pa ../ebin \
    -yaws embedded true -connect_all false -hidden \
    -chordsharp cs_port 14196 \
    -chordsharp yaws_port 8001 \
    -sname node@localhost -s chordsharp
