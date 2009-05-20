@echo off
REM set path to erlang installation
set ERLANG="c:\program files\erl5.7.1\bin"

%ERLANG%\erl -pa contrib\yaws -pa ebin -noinput +B -eval "case make:all() of up_to_date -> halt(0); error -> halt(1) end."