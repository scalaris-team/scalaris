@echo off
:: set path to erlang installation

SETLOCAL ENABLEDELAYEDEXPANSION

set ERL_R18_0=7.0.2 7.0.1 7.0
set ERL_R17_5=6.4.1 6.4
set ERL_R17_4=6.3.1 6.3
set ERL_R17_3=6.2.1 6.2
set ERL_R17_1=6.1.2 6.1.1 6.1
set ERL_R17=6.0.1 6.0
set ERL_R16B03=5.10.4.1 5.10.4
set ERL_R16B02=5.10.3.1 5.10.3
set ERL_R16B01=5.10.2
set ERL_R16B=5.10.1.2 5.10.1.1 5.10.1
set ERL_R15B03=5.9.3.1 5.9.3
set ERL_R15B02=5.9.2
set ERL_R15B01=5.9.1.2 5.9.1.1 5.9.1
set ERL_R15B=5.9.0.1 5.9
set ERL_R14B04=5.8.5
set ERL_ALL=%ERL_R18_0% %ERL_R17_5% %ERL_R17_4% %ERL_R17_3% %ERL_R17_1% %ERL_R17% %ERL_R16B03% %ERL_R16B02% %ERL_R16B01% %ERL_R16B% %ERL_R15B03% %ERL_R15B02% %ERL_R15B01% %ERL_R15B% %ERL_R14B04%
if not defined ERLANG_HOME (
    FOR %%c in (%ERL_ALL%) DO (
        FOR %%p in ("%ProgramFiles%\erl%%c" "%ProgramFiles(x86)%\erl%%c") DO (
            set FULL_PATH=%%p
            if exist !FULL_PATH! (
                set ERLANG_HOME=%%p
                goto found
            )
        )
    )
)

:found

echo ERLANG_HOME=%ERLANG_HOME%

ENDLOCAL & set ERLANG_HOME=%ERLANG_HOME%
