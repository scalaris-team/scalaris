@echo off
:: set path to erlang installation
set ERLANG="c:\program files\erl5.7.5\bin"

::replace @EMAKEFILEDEFINES@ from Emakefile.in and write Emakefile
::(this is what autoconf on *nix would do)
:: depending on your config, you might need to add one of the following options:
::  {d, HAVE_TCERL},
::  {d, types_not_builtin},
::  {d, term_not_builtin},
::  {d, node_not_builtin},
::  {d, module_not_builtin},
::  {d, boolean_not_builtin},
::  {d, recursive_types_are_not_allowed,
::  {d, type_forward_declarations_are_not_allowed},
::  {d, forward_or_recursive_types_are_not_allowed}
:: refer to configure.ac for the appropriate checks for necessity
:: Note: Search & Replace functionality from http://www.dostips.com
if not exist Emakefile (
	@echo Creating Emakefile...
	for /f "tokens=1,* delims=&&&" %%a in (Emakefile.in) do (
		set "line=%%a"
		if defined line (
			call set "line=echo.%%line:  @EMAKEFILEDEFINES@=%%"
			for /f "delims=" %%X in ('"echo."%%line%%""') do %%~X >> Emakefile
		) ELSE echo.
	)
	@echo ...done!
)

@echo on
%ERLANG%\erl -pa contrib\yaws -pa ebin -noinput +B -eval "case make:all() of up_to_date -> halt(0); error -> halt(1) end."
@echo off
