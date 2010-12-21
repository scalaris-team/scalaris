@echo off
:: set path to erlang installation
set ERLANG="C:\Program Files\erl5.8.2\bin"

::replace @EMAKEFILEDEFINES@ from Emakefile.in and write Emakefile
::(this is what autoconf on *nix would do)
:: depending on your config, you might need to add one of the following options:
::  {d, have_toke},
::  {d, types_not_builtin},
::  {d, term_not_builtin},
::  {d, node_not_builtin},
::  {d, module_not_builtin},
::  {d, boolean_not_builtin},
::  {d, recursive_types_are_not_allowed,
::  {d, type_forward_declarations_are_not_allowed},
::  {d, forward_or_recursive_types_are_not_allowed},
::  {d, tid_not_builtin}
:: For older erlang versions that do not have the httpc module but use the (older)
:: http module, the following line needs to be added to Emakefile:
:: ["{\"contrib/compat/httpc.erl\",[debug_info, nowarn_unused_function, nowarn_obsolete_guard, nowarn_unused_vars,{outdir, \"ebin\"}]}."]
:: refer to configure.ac for the appropriate checks for necessity
:: Note: Search & Replace functionality from http://www.dostips.com
if not exist Emakefile (
	@echo Creating Emakefile...
	for /f "tokens=1,* delims=&&&" %%a in (Emakefile.in) do (
		set "line=%%a"
		if defined line (
			call set "line=echo.%%line:  @EMAKEFILEDEFINES@=, {d, tid_not_builtin}%%"
			call set "line=%%line:@EMAKEFILECOMPILECOMPAT@=%%"
			for /f "delims=" %%X in ('"echo."%%line%%""') do %%~X >> Emakefile
		) ELSE echo.
	)
	@echo ...done!
)

@echo on
%ERLANG%\erl -pa contrib\yaws -pa ebin -noinput +B -eval "case make:all() of up_to_date -> halt(0); error -> halt(1) end."
@echo off
