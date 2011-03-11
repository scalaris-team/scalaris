@echo off
:: Copyright 2010-2011 Zuse Institute Berlin
::
::    Licensed under the Apache License, Version 2.0 (the "License");
::    you may not use this file except in compliance with the License.
::    You may obtain a copy of the License at
::
::        http://www.apache.org/licenses/LICENSE-2.0
::
::    Unless required by applicable law or agreed to in writing, software
::    distributed under the License is distributed on an "AS IS" BASIS,
::    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::    See the License for the specific language governing permissions and
::    limitations under the License.

:: Script to start a node, that joins a running Scalaris system.
set ID=1
set NODE_NAME=node%ID%
set /a CSPORT=14195+%ID%
set /a YAWSPORT=8000+%ID%
set SCALARIS_ADDITIONAL_PARAMETERS=-scalaris port %CSPORT% -scalaris yaws_port %YAWSPORT%

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: set path to erlang installation
set ERLANG="C:\Program Files\erl5.8.2\bin"
:: scalaris configuration parameters
set SCALARIS_COOKIE=chocolate chip cookie
set SCALARISDIR=%~dp0..
set BEAMDIR=%SCALARISDIR%\ebin
set BACKGROUND=
::set BACKGROUND=-detached
set TOKEFLAGS=
:: note: paths passed as strings to erlang applications need to be escaped!
set LOGDIR=%SCALARISDIR:\=\\%\\log
set DOCROOTDIR=%SCALARISDIR:\=\\%\\docroot
set NODEDOCROOTDIR=%SCALARISDIR:\=\\%\\docroot_node
set ETCDIR=%SCALARISDIR:\=\\%\\bin

@echo on
pushd %BEAMDIR%
%ERLANG%\erl -setcookie "%SCALARIS_COOKIE%" ^
  -pa "%SCALARISDIR%\contrib\yaws\ebin" ^
  -pa "%SCALARISDIR%\contrib\log4erl\ebin" ^
  -pa "%BEAMDIR%" %TOKEFLAGS% %BACKGROUND% ^
  -yaws embedded true ^
  -scalaris log_path "\"%LOGDIR%\"" ^
  -scalaris docroot "\"%NODEDOCROOTDIR%\"" ^
  -scalaris config "\"%ETCDIR%\\scalaris.cfg\"" ^
  -scalaris local_config "\"%ETCDIR%\\scalaris.local.cfg\"" ^
  -scalaris start_dht_node dht_node ^
  -connect_all false -hidden -name %NODE_NAME% ^
  %SCALARIS_ADDITIONAL_PARAMETERS% ^
  -s scalaris %*
popd
@echo off
