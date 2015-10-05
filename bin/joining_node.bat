@echo off
:: Copyright 2010-2015 Zuse Institute Berlin
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
SETLOCAL

for %%x in (%cmdcmdline%) do if %%~x==/c set DOUBLECLICKED=1

:: Script to start a node, that joins a running Scalaris system.
set SCRIPTDIR=%~dp0
set ID=1
set params=%*

:: extract node ID from first command line parameter
IF [%1]==[] goto start
set ID=%1
set params=
:: there is no easy way to get the rest of the parameters
:: -> loop through them and remove the first
:loop
shift
if [%1]==[] goto start
set params=%params% %1
goto loop
)
:start

set NODE_NAME=node%ID%
set /a CSPORT=14195+%ID%
set /a YAWSPORT=8000+%ID%
set SCALARIS_ADDITIONAL_PARAMETERS=-scalaris port %CSPORT% -scalaris yaws_port %YAWSPORT%

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: scalaris configuration parameters
set SCALARIS_COOKIE=chocolate chip cookie
set SCALARISDIR=%SCRIPTDIR%..
set BEAMDIR=%SCALARISDIR%\ebin
set BACKGROUND=
::set BACKGROUND=-detached
set TOKEFLAGS=
:: note: paths passed as strings to erlang applications need to be escaped!
set LOGDIR=%SCALARISDIR:\=\\%\\log
set DOCROOTDIR=%SCALARISDIR:\=\\%\\docroot
set ETCDIR=%SCALARISDIR:\=\\%\\bin
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: set path to erlang installation
call "%SCRIPTDIR%"\find_erlang.bat

@echo on
pushd %BEAMDIR%
%ERLANG_HOME%\bin\erl.exe -setcookie "%SCALARIS_COOKIE%" ^
  -pa "%SCALARISDIR%\contrib\yaws\ebin" ^
  -pa "%SCALARISDIR%\contrib\log4erl\ebin" ^
  -pa "%BEAMDIR%" %TOKEFLAGS% %BACKGROUND% ^
  -yaws embedded true ^
  -scalaris log_path "\"%LOGDIR%\"" ^
  -scalaris docroot "\"%DOCROOTDIR%\"" ^
  -scalaris config "\"%ETCDIR%\\scalaris.cfg\"" ^
  -scalaris local_config "\"%ETCDIR%\\scalaris.local.cfg\"" ^
  -scalaris start_type joining ^
  -connect_all false -hidden -name %NODE_NAME% ^
  %SCALARIS_ADDITIONAL_PARAMETERS% ^
  -s scalaris %ERL_SCHED_FLAGS% %params%
popd
@echo off

if defined DOUBLECLICKED PAUSE
