@echo off
REM set path to erlang installation
set ERLANG="c:\program files\erl5.7.1\bin"

%ERLANG%\erl -setcookie "chocolate chip cookie" -pa ..\contrib\log4erl\ebin -pa ..\contrib\yaws\ebin -pa ..\ebin -yaws embedded true -connect_all false -sname boot@localhost -s boot