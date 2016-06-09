@echo off
SETLOCAL

for %%x in (%cmdcmdline%) do if %%~x==/c set DOUBLECLICKED=1

:: set path to erlang installation
call "%~dp0"\bin\find_erlang.bat

::replace @EMAKEFILEDEFINES@ from Emakefile.in and write Emakefile
::(this is what autoconf on *nix would do)
:: depending on your config, you might need to add one or more of the following options:
::  {d, enable_debug}
::  {d, have_ctline_support}
::  {d, have_callback_support}
::  {d, with_crypto_hash}
::  {d, with_rand}
::  {d, with_maps}
:: refer to configure.ac for the appropriate checks for necessity
::if yaws should use the file:sendfile/5 functionality, set @YAWS_OPTIONS@ to
::  {d, 'HAVE_ERLANG_SENDFILE'}
::similarly for yaws, add to @YAWS_OPTIONS@ if required:
::  {d, 'HAVE_CRYPTO_HASH'}
::  {d, 'HAVE_ERLANG_NOW'}
::but note the issues in R15 & R16 mentioned at http://erlang.org/pipermail/erlang-questions/2013-October/075676.html
:: Note: Search & Replace functionality from http://www.dostips.com
if not exist Emakefile (
    echo Creating Emakefile...
    for /f "tokens=1,* delims=&&&" %%a in (Emakefile.in) do (
        set "line=%%a"
        if defined line (
            call set "line=echo.%%line:  @EMAKEFILEDEFINES@=%%"
            call set "line=%%line:  @YAWS_OPTIONS@=%%"
            for /f "delims=" %%X in ('"echo."%%line%%""') do %%~X >> Emakefile
        ) ELSE echo.
    )
    echo ...done!
)
if not exist include/rt.hrl (
    echo Creating include/rt.hrl using rt_chord...
    echo -include^("rt_chord.hrl"^).>> include/rt.hrl
)

@echo on
pushd "%~dp0""\contrib\log4erl\src"
%ERLANG_HOME%\bin\erlc.exe log4erl_parser.yrl
%ERLANG_HOME%\bin\erl.exe -noinput -noshell -s leex file log4erl_lex.xrl -s init stop
popd
%ERLANG_HOME%\bin\erl.exe -pa contrib\yaws -pa ebin -noinput +B -eval "case make:all() of up_to_date -> halt(0); error -> halt(1) end."
@echo off

if defined DOUBLECLICKED PAUSE
