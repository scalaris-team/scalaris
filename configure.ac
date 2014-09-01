#   Copyright 2007-2012 Konrad-Zuse-Zentrum für Informationstechnik Berlin
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

AC_PREREQ(2.65)
AC_INIT(scalaris, 0.7.0+svn, scalaris@googlegroups.com)

EMAKEFILEDEFINES=
EDOCMACROS=
DIALYZER_FLAGS=

###########################################################
#
# check tokyo cabinet and toke
#
###########################################################

ENABLE_TOKE=
TOKEPREFIX=
AC_ARG_ENABLE([toke],
              AS_HELP_STRING([--disable-toke],
                 [disable support for tokyo cabinet through toke]),
              [TOKEPREFIX=no])
AC_ARG_ENABLE([toke],
              AS_HELP_STRING([--enable-toke@<:@=DIR@:>@],
                 [enable support for tokyo cabinet through toke]),
              [ENABLE_TOKE=yes
               TOKEPREFIX=$enableval])

###########################################################
#
# check browser for 'make test-vts'
#
###########################################################

BROWSER=
AC_ARG_WITH([browser],
            [AS_HELP_STRING([--with-browser=BROWSER],
               [absolute path of the browser to use for 'make test-vts'])],
            [if test -n "$withval"; then
               BROWSER="$withval"
               AS_IF([test ! -x "$BROWSER"],
                 [AC_MSG_WARN(["$BROWSER" was not found or is not executable ('make test-vts' will likely fail)])])
             fi],
            [AC_PATH_PROG([BROWSER], [xdg-open])])
AC_SUBST(BROWSER)

AS_IF([test "$BROWSER" = ""],
      [AC_MSG_WARN([xdg-open was not found ('make test-vts' will likely fail)])])

###########################################################
#
# check screen availability for 'scalarisctl -d --screen'
#
###########################################################

AC_PATH_PROG(SCREEN, screen, "/bin/false")
if test "$ac_cv_path_SCREEN" = "/bin/false" ; then
  AC_MSG_NOTICE([screen not found - you won't be able to run scalaris deamonized with screen, i.e. 'scalarisctl -d --screen'])
fi

###########################################################
#
# check sudo, runuser availability for the init.d script
#
###########################################################

AC_PATH_PROG(SUDO, sudo, "/bin/false")

RUNUSER=""
AC_ARG_ENABLE([runuser],
              AS_HELP_STRING([--disable-runuser],
                 [disable support for runuser (use sudo instead)]))
AS_IF([test "$enable_runuser" != "no"],
      [AC_PATH_PROG(RUNUSER, runuser, "", [$PATH$PATH_SEPARATOR/usr/sbin])])

if test "$ac_cv_path_SUDO" = "/bin/false" -a "$ac_cv_path_RUNUSER" = ""; then
  AC_MSG_NOTICE([sudo or runuser not found - you won't be able to use our init.d script])
fi

###########################################################
#
# check whether to compile to native code using HiPE
#
###########################################################

COMPILE_NATIVE=no
AC_ARG_ENABLE([native],
              AS_HELP_STRING([--enable-native],
                [enable compilation to native code using HiPE]),
              [COMPILE_NATIVE=yes])

###########################################################
#
# check wether to enable debugging, e.g. enable the ASSERT macro
#
###########################################################

AC_ARG_ENABLE([debug],
              AS_HELP_STRING([--enable-debug], [enable debugging mode]),
              [AS_IF([test "no" != "$enableval"],
                     [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, enable_debug}"
                      EDOCMACROS="${EDOCMACROS}, {enable_debug, true}"
                      DIALYZER_FLAGS="${DIALYZER_FLAGS} -Denable_debug"])])

###########################################################
#
# check erlang
#
###########################################################
AC_LANG_ERLANG

AC_ERLANG_NEED_ERLC
AC_ERLANG_NEED_ERL

AC_ERLANG_SUBST_ROOT_DIR
AC_ERLANG_SUBST_LIB_DIR

AC_CACHE_CHECK([for Erlang/OTP ERTS version],
    [erlang_cv_erts_ver],
    [AC_LANG_PUSH([Erlang])[]dnl
     AC_RUN_IFELSE(
        [AC_LANG_PROGRAM([], [dnl
            Version = erlang:system_info(version),
            file:write_file("conftest.out", Version),
            ReturnValue = 0,
            halt(ReturnValue)])],
        [erlang_cv_erts_ver=`cat conftest.out`],
        [AC_MSG_FAILURE([test Erlang program execution failed])])
     AC_LANG_POP([Erlang])[]dnl
    ])
AC_SUBST([ERLANG_ERTS_VER], [$erlang_cv_erts_ver])

AC_PATH_PROG(EPMD, epmd, "/bin/false", "$PATH:$ERLANG_ROOT_DIR/bin:$ERLANG_ROOT_DIR/erts-$ERLANG_ERTS_VER/bin")

AS_IF([test "x$COMPILE_NATIVE" != xno], [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, native"])

# required libs:
ERLANG_UNAVAILABLE_LIBS=
AC_ERLANG_CHECK_LIB(erts, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS erts")

AC_ERLANG_CHECK_LIB(kernel, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS kernel")
AC_CHECK_FILE($ERLANG_LIB_DIR_kernel/include/inet.hrl, ,
              AC_MSG_FAILURE([kernel/include/inet.hrl not found - you won't be able to compile the erlang sources]))

AC_ERLANG_CHECK_LIB(stdlib, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS stdlib")

AC_ERLANG_CHECK_LIB(compiler, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS compiler")

AC_ERLANG_CHECK_LIB(crypto, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS crypto")

AC_ERLANG_CHECK_LIB(os_mon, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS os_mon")

AC_ERLANG_CHECK_LIB(tools, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS tools")

AC_ERLANG_CHECK_LIB(inets, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS inets")

AC_ERLANG_CHECK_LIB(ssl, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS ssl")

AC_ERLANG_CHECK_LIB(xmerl, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS xmerl")

AS_IF([test "x$ERLANG_UNAVAILABLE_LIBS" != x], AC_MSG_FAILURE([$ERLANG_UNAVAILABLE_LIBS not found - you won't be able to compile or run the erlang sources]))

# libs for optional build tasks:
AC_ERLANG_CHECK_LIB(common_test, , AC_MSG_NOTICE([erlang-common_test not found - you won't be able to run the unit tests without common_test]))

AC_ERLANG_CHECK_LIB(edoc, , AC_MSG_NOTICE([erlang-edoc not found - you won't be able to create the documentation or run 'make install']))

AC_ERLANG_CHECK_LIB(hanoidb,
                    [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_hanoidb}"
                     EDOCMACROS="${EDOCMACROS}, {have_hanoidb, true}"
                     DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_hanoidb"],
                    [AC_MSG_NOTICE([erlang-hanoidb not found - you won't be able to use the db_hanoidb back-end (if configured in scalaris.hrl)])])

AC_ERLANG_CHECK_LIB(erlang_js, , AC_MSG_NOTICE([erlang_js not found - you won't be able to use JavaScript Map-Reduce jobs]))

AC_CHECK_FILE($ERLANG_LIB_DIR_common_test/priv/bin/run_test,
              [RUN_TEST_FILE=$ERLANG_LIB_DIR_common_test/priv/bin/run_test],
              [AC_CHECK_FILE($ac_cv_erlang_root_dir/bin/run_test,
                       [RUN_TEST_FILE=$ac_cv_erlang_root_dir/bin/run_test],
                       [AC_PATH_PROG([RUN_TEST_FILE],[run_test],"")])])
AC_CHECK_FILE($ERLANG_LIB_DIR_common_test/priv/bin/ct_run,
              [CT_RUN_FILE=$ERLANG_LIB_DIR_common_test/priv/bin/ct_run],
              [AC_CHECK_FILE($ac_cv_erlang_root_dir/bin/ct_run,
                       [CT_RUN_FILE=$ac_cv_erlang_root_dir/bin/ct_run],
                       [AC_PATH_PROG([CT_RUN_FILE],[ct_run],"")])])

AC_MSG_CHECKING([for unit-test runner])
if test -n "$RUN_TEST_FILE"; then
  AC_SUBST(RUN_TEST,"$RUN_TEST_FILE")
  AC_MSG_RESULT([$RUN_TEST_FILE])
elif test -n "$CT_RUN_FILE"; then
  AC_SUBST(RUN_TEST,"$CT_RUN_FILE")
  AC_MSG_RESULT([$CT_RUN_FILE])
else
  AC_MSG_RESULT([neither run_test nor ct_run found - on erlang < R14 consider running install.sh in the common_test directory otherwise you won't be able to run the unit tests])
fi

ERLANG_TOKE_FLAGS=
AS_IF([test "x$TOKEPREFIX" != xno],
            [
            ERLANG_TOKE_MESSAGE=
            AS_IF([test "x$TOKEPREFIX" != x], [export ERL_LIBS="$TOKEPREFIX"])
            AC_ERLANG_CHECK_LIB(toke,
              [AC_CHECK_FILE($ERLANG_LIB_DIR_toke/priv/libtoke.so,
                [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_toke}"
                 EDOCMACROS="${EDOCMACROS}, {have_toke, true}"
                 DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_toke"
                 ERLANG_TOKE_FLAGS="-pa $ERLANG_LIB_DIR_toke/ebin"
                ],
                [ERLANG_TOKE_MESSAGE="toke library libtoke.so not found"])
              ],
              [ERLANG_TOKE_MESSAGE="toke erlang library not found"])
            AS_IF([test "x$TOKEPREFIX" != x], [export ERL_LIBS=""])
            AS_IF([test "x$ERLANG_TOKE_MESSAGE" != x],
              [AS_IF([test "x$ENABLE_TOKE" != xyes],
                [ERLANG_TOKE_MESSAGE="$ERLANG_TOKE_MESSAGE, disabling toke support..."
                 AC_MSG_NOTICE($ERLANG_TOKE_MESSAGE)],
                AC_MSG_FAILURE($ERLANG_TOKE_MESSAGE))])
            ])
AC_SUBST([ERLANG_TOKE_FLAGS])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::gb_tree()).], [ok])],
       [AC_MSG_NOTICE([gb_tree() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, types_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {types_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dtypes_not_builtin"
        AC_MSG_NOTICE([gb_tree() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::reference()).], [ok])],
       [AC_MSG_NOTICE([reference() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, reference_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {reference_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dreference_not_builtin"
        AC_MSG_NOTICE([reference() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::term()).], [ok])],
       [AC_MSG_NOTICE([term() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, term_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {term_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dterm_not_builtin"
        AC_MSG_NOTICE([term() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::node()).], [ok])],
       [AC_MSG_NOTICE([node() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, node_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {node_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dnode_not_builtin"
        AC_MSG_NOTICE([node() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::module()).], [ok])],
       [AC_MSG_NOTICE([module() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, module_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {module_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dmodule_not_builtin"
        AC_MSG_NOTICE([module() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::boolean()).], [ok])],
       [AC_MSG_NOTICE([boolean() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, boolean_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {boolean_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dboolean_not_builtin"
        AC_MSG_NOTICE([boolean() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::tid()).], [ok])],
       [AC_MSG_NOTICE([tid() is a built-in type])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, tid_not_builtin}"
        EDOCMACROS="${EDOCMACROS}, {tid_not_builtin, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dtid_not_builtin"
        AC_MSG_NOTICE([tid() is not built-in])])

etstid_avail=`"${ERL}" -noshell -eval \
  '{ok, {_Module, [[{abstract_code, {_AbstVersion, AbstractCode}}]]}} =
       beam_lib:chunks(code:where_is_file("ets.beam"), [[abstract_code]]),
   Res = lists:any(fun({attribute,_,Type,{tid,_,[[]]}}) when Type =:= type orelse Type =:= opaque -> true;
                      (_) -> false end, AbstractCode),
   io:format("~p~n", [[Res]]), erlang:halt().' | tail -1`
AS_IF([test "${etstid_avail}" = "true"],
      [AC_MSG_NOTICE([ets:tid/0 is available])],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, etstid_not_available}"
       EDOCMACROS="${EDOCMACROS}, {etstid_not_available, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Detstid_not_available"
       AC_MSG_NOTICE([ets:tid/0 is not available])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::{ok, mytype()} | ok).], [ok])],
       [RECURSIVETYPESALLOWED=""
        RECURSIVETYPESALLOWED_EDOC=""
        RECURSIVETYPESALLOWED_DIALYZER=""
        AC_MSG_NOTICE([recursive types are supported])],
       [RECURSIVETYPESALLOWED=", {d, recursive_types_are_not_allowed}"
        RECURSIVETYPESALLOWED_EDOC=", {recursive_types_are_not_allowed, true}"
        RECURSIVETYPESALLOWED_DIALYZER=" -Drecursive_types_are_not_allowed"
        AC_MSG_NOTICE([recursive types are not supported])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::{ok, mytype2()} | ok). -type(mytype2() :: ok).], [ok])],
       [TYPEFORWARDDECLARATIONS=""
        TYPEFORWARDDECLARATIONS_EDOC=""
        TYPEFORWARDDECLARATIONS_DIALYZER=""
        AC_MSG_NOTICE([type forward declarations are supported])],
       [TYPEFORWARDDECLARATIONS=", {d, type_forward_declarations_are_not_allowed}"
        TYPEFORWARDDECLARATIONS_EDOC=", {type_forward_declarations_are_not_allowed, true}"
        TYPEFORWARDDECLARATIONS_DIALYZER=" -Dtype_forward_declarations_are_not_allowed"
        AC_MSG_NOTICE([type forward declarations are not supported])])

if test "x$RECURSIVETYPESALLOWED" != x -o "x$TYPEFORWARDDECLARATIONS" != x; then
   AC_MSG_NOTICE([forward declarations or recursive types are not allowed])
   FORWARDORRECURSIVE=", {d, forward_or_recursive_types_are_not_allowed}"
   FORWARDORRECURSIVE_EDOC=", {forward_or_recursive_types_are_not_allowed, true}"
   FORWARDORRECURSIVE_DIALYZER=" -Dforward_or_recursive_types_are_not_allowed"
fi

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([[-export_type([mytype/0]). -type(mytype()::ok).]], [ok])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_export_type_support}"
        EDOCMACROS="${EDOCMACROS}, {with_export_type_support, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_export_type_support"
        AC_MSG_NOTICE([export_type is supported])],
       [AC_MSG_NOTICE([export_type is not supported])])

crypto_start=`"${ERL}" -noshell -eval 'io:format("~p~n", [[crypto:start()]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_start}" = "ok"],
      [AC_MSG_NOTICE([crypto:start() is available])],
      [AC_MSG_FAILURE([cannot start the crypto subsystem])])

crypto_hash=`"${ERL}" -noshell -eval 'code:load_file(crypto), io:format("~p~n", [[erlang:function_exported(crypto,hash,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_hash}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_crypto_hash}"
       EDOCMACROS="${EDOCMACROS}, {with_crypto_hash, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_crypto_hash"
       AC_MSG_NOTICE([crypto:hash/2 is available and usable])],
      [AC_MSG_NOTICE([crypto:hash/2 is not available or not usable, falling back to crypto:md5/1 and crypto:sha1/1])])

erlang_timestamp_avail=`"${ERL}" -noshell -eval \
  '{ok, {_Module, [[{abstract_code, {_AbstVersion, AbstractCode}}]]}} =
       beam_lib:chunks(code:where_is_file("erlang.beam"), [[abstract_code]]),
   Res = lists:any(fun({attribute,_,Type,{timestamp,_,[[]]}}) when Type =:= type orelse Type =:= opaque -> true;
                      (_) -> false end, AbstractCode),
   io:format("~p~n", [[Res]]), erlang:halt().' | tail -1`
AS_IF([test "${erlang_timestamp_avail}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_erlang_timestamp}"
       AC_MSG_NOTICE([erlang:timestamp() type is available])],
      [AC_MSG_NOTICE([the type erlang:timestamp() is not available])])

httpc_avail=`"${ERL}" -noshell -eval 'io:format("~p~n", [[httpc:set_option(max_sessions, 1)]]), erlang:halt().' | tail -1`
AS_IF([test "${httpc_avail}" = "ok"],
      [AC_MSG_NOTICE([httpc is available])],
      [AC_MSG_NOTICE([httpc unavailable, using wrapper module]);EMAKEFILECOMPILECOMPAT=["{\"contrib/compat/httpc.erl\",[debug_info, nowarn_unused_function, nowarn_obsolete_guard, nowarn_unused_vars,{outdir, \"ebin\"}]}."]])

ct_line=`"${ERL}" -noshell -eval 'code:load_file(ct_line), io:format("~p~n", [[erlang:function_exported(ct_line,parse_transform,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${ct_line}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_ctline_support}"
       EDOCMACROS="${EDOCMACROS}, {have_ctline_support, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_ctline_support"
       AC_MSG_NOTICE([ct_line is available])],
      [AC_MSG_NOTICE([ct_line is not available])])

"${ERL}" -noshell -s ct_run script_start -s erlang halt --help | grep "ct_hooks" 2>/dev/null 1>/dev/null
AS_IF(
       [test $? -eq 0 ],
       [CTHOOKS_FLAGS="-ct_hooks scalaris_cth"
        EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_cthooks_support}"
        EDOCMACROS="${EDOCMACROS}, {have_cthooks_support, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_cthooks_support"
        AC_MSG_NOTICE([ct_hooks is available])],
       [CTHOOKS_FLAGS=""
        AC_MSG_NOTICE([ct_hooks is not available])])

AC_SUBST(CTHOOKS_FLAGS)

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-callback init() -> ok.], [ok])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_callback_support}"
        EDOCMACROS="${EDOCMACROS}, {have_callback_support, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_callback_support"
        AC_MSG_NOTICE([-callback is available])],
       [AC_MSG_NOTICE([-callback is not available])])

###########################################################
#
# check for file:send_file/5 for yaws
#
###########################################################
# split erlang ets version:
set `echo $erlang_cv_erts_ver | sed 's/\./ /g'`
ERTS_MAJOR=$1
ERTS_MINOR=$2
ERTS_MAINT=${3:-0}

HAVE_YAWS_SENDFILE=false
HAVE_ERL_SENDFILE=false

file_sendfile=`"${ERL}" -noshell -eval 'io:format("~p~n",[[erlang:function_exported(file, sendfile, 5)]]), erlang:halt().' | tail -1`

AS_IF([test "$file_sendfile" = true -a \
           '(' $ERTS_MAJOR -gt 5 -o \
           '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -gt 9 ')' -o \
           '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -eq 9 -a $ERTS_MAINT -ge 1 ')' ')'],
      [HAVE_ERL_SENDFILE=true; AC_MSG_NOTICE(found file:sendfile/5)])

AS_IF([test "$HAVE_ERL_SENDFILE" = true],
      [HAVE_YAWS_SENDFILE=""
       HAVE_YAWS_SENDFILE_DIALYZER=""
       AC_MSG_NOTICE([file:sendfile/5 is available for yaws])])
AS_IF([test "$HAVE_ERL_SENDFILE" = false -a "$HAVE_YAWS_SENDFILE" = true],
      [HAVE_YAWS_SENDFILE=", {d, 'HAVE_YAWS_SENDFILE'}"
       HAVE_YAWS_SENDFILE_DIALYZER=" -DHAVE_YAWS_SENDFILE"
       AC_MSG_NOTICE([using yaws_sendfile for yaws])])
AS_IF([test "$HAVE_ERL_SENDFILE" = false -a "$HAVE_YAWS_SENDFILE" = false],
      [HAVE_YAWS_SENDFILE=", {d, 'NO_FILE_SENDFILE'}"
       HAVE_YAWS_SENDFILE_DIALYZER=" -DNO_FILE_SENDFILE"
       AC_MSG_NOTICE([no send_file suitable for yaws, using fallback])])

###########################################################
#
# all defines together...
#
###########################################################

EMAKEFILEDEFINES="${EMAKEFILEDEFINES}${RECURSIVETYPESALLOWED}${TYPEFORWARDDECLARATIONS}${FORWARDORRECURSIVE}"
EDOCMACROS="${EDOCMACROS#, }${RECURSIVETYPESALLOWED_EDOC}${TYPEFORWARDDECLARATIONS_EDOC}${FORWARDORRECURSIVE_EDOC}"
DIALYZER_FLAGS="${DIALYZER_FLAGS## }${RECURSIVETYPESALLOWED_DIALYZER}${TYPEFORWARDDECLARATIONS_DIALYZER}${FORWARDORRECURSIVE_DIALYZER}${HAVE_YAWS_SENDFILE_DIALYZER}"

AC_SUBST(EMAKEFILEDEFINES)
AC_SUBST(EDOCMACROS)
AC_SUBST(DIALYZER_FLAGS)

AC_SUBST(EMAKEFILECOMPILECOMPAT)
AC_SUBST(HAVE_YAWS_SENDFILE)

###########################################################
#
# check java-functions, build-classpath availability for java-api/scalaris
#
###########################################################

JAVAFUNCTIONS=
AC_ARG_WITH([java-functions],
            [AS_HELP_STRING([--with-java-functions=JAVAFUNCTIONS],
               [use the given java-functions script for java-api/scalaris (default is /usr/share/java-utils/java-functions)])],
            [with_java_functions=$withval],
            [with_java_functions=/usr/share/java-utils/java-functions])

AC_CHECK_FILE($with_java_functions, [JAVAFUNCTIONS=$with_java_functions],
              [JAVAFUNCTIONS=$with_java_functions
               AC_MSG_WARN([java-functions was not found in "$with_java_functions" (java-api/scalaris may fail)])])
AC_SUBST(JAVAFUNCTIONS)

BUILDCLASSPATH=
AC_ARG_WITH([build-classpath],
            [AS_HELP_STRING([--with-build-classpath=BUILDCLASSPATH],
               [absolute path of the build-classpath script to use for java-api/scalaris])],
            [if test -n "$withval"; then
               BUILDCLASSPATH="$withval"
             fi],
            [AC_PATH_PROG([BUILDCLASSPATH], [build-classpath])])
AC_SUBST(BUILDCLASSPATH)

AS_IF([test ! -x "$BUILDCLASSPATH"],
      [AC_MSG_WARN([build-classpath was not found in "$BUILDCLASSPATH" or is not executable (java-api/scalaris may fail)])])

###########################################################
#
# check ruby
#
###########################################################

ENABLERUBYINSTALL=
RUBYSITELIBDIR=
AC_ARG_WITH([ruby-sitelibdir],
            [AS_HELP_STRING([--with-ruby-sitelibdir=SITELIBDIR],
            [where to install ruby libraries])],
            [if test -n "$withval"; then
               RUBYSITELIBDIR="$withval"
               ENABLERUBYINSTALL="install-ruby"
             fi],
            [])
AC_SUBST(ENABLERUBYINSTALL)
AC_SUBST(RUBYSITELIBDIR)

###########################################################
#
# check python, python3
#
###########################################################
PYTHON2SITELIBDIR=
ENABLEPYTHON2INSTALL=
AC_CACHE_CHECK([for python 2.x >= 2.6], [ac_cv_path_PYTHON2],
  [AC_PATH_PROGS_FEATURE_CHECK([PYTHON2], [python python2],
    [[$ac_path_PYTHON2 -V 2>&1 | grep "^Python 2.[6789]" > /dev/null && \
      PYTHON2SITELIBDIR=`$ac_path_PYTHON2 -c 'from distutils.sysconfig import get_python_lib; print(get_python_lib(0,0,prefix="${prefix}"))'  2>/dev/null` && \
      ac_cv_path_PYTHON2=$ac_path_PYTHON2 ac_path_PYTHON2_found=:]],
    [AC_MSG_RESULT([not found])])])
AC_SUBST([PYTHON2], [$ac_cv_path_PYTHON2])
AC_SUBST(PYTHON2SITELIBDIR)

AS_IF([test "$PYTHON2" != ""],
      [AC_MSG_CHECKING([for python 2.x sitelibdir])
       AC_MSG_RESULT([$PYTHON2SITELIBDIR])
       ENABLEPYTHON2INSTALL="install-python"
       sed -i -e "1s|#!.*|#!$PYTHON2|g" python-api/*.py])
AC_SUBST(ENABLEPYTHON2INSTALL)

PYTHON3SITELIBDIR=
ENABLEPYTHON3INSTALL=
AC_CACHE_CHECK([for python 3.x], [ac_cv_path_PYTHON3],
  [AC_PATH_PROGS_FEATURE_CHECK([PYTHON3], [python3 python],
    [[$ac_path_PYTHON3 -V 2>&1 | grep "^Python 3." > /dev/null && \
      PYTHON3SITELIBDIR=`$ac_path_PYTHON3 -c 'from distutils.sysconfig import get_python_lib; print(get_python_lib(0,0,prefix="${prefix}"))'  2>/dev/null` && \
      ac_cv_path_PYTHON3=$ac_path_PYTHON3 ac_path_PYTHON3_found=:]],
    [AC_MSG_RESULT([not found])])])
AC_SUBST([PYTHON3], [$ac_cv_path_PYTHON3])
AC_SUBST(PYTHON3SITELIBDIR)

AS_IF([test "$PYTHON3" != ""],
      [AC_MSG_CHECKING([for python 3.x sitelibdir])
       AC_MSG_RESULT([$PYTHON3SITELIBDIR])
       ENABLEPYTHON3INSTALL="install-python3"])
AC_SUBST(ENABLEPYTHON3INSTALL)

###########################################################
#
# check MACOSX vs. Linux for flexbisonparse driver
#
###########################################################
case `uname -s` in
 linux*)
     DRIVER_OS=LINUX
     ;;
 Linux*)
     DRIVER_OS=LINUX
     ;;
 Darwin*)
     DRIVER_OS=MACOSX
     ;;
 darwin*)
     DRIVER_OS=MACOSX
     ;;
esac

echo "detected OS is: '$DRIVER_OS'"
AC_SUBST(DRIVER_OS)

mkdir -p ~/.yaws 2> /dev/null

mkdir -p python3-api
cp python-api/scalaris.in python3-api/
sed -i -e "s|python-api|python3-api|g" \
       -e "s|@PYTHONSITELIBDIR@|@PYTHON3SITELIBDIR@|g" \
       -e "s|PYTHON2|PYTHON3|g" python3-api/scalaris.in

AC_CONFIG_FILES([Emakefile Makefile bin/scalarisctl java-api/scalaris java-api/scalaris-java.conf contrib/init.d/scalaris contrib/init.d/scalaris-monitor python-api/scalaris python3-api/scalaris ruby-api/scalaris])
AC_OUTPUT

chmod u+x bin/scalarisctl
chmod u+x java-api/scalaris
chmod u+x python-api/scalaris
chmod u+x python3-api/scalaris
chmod u+x ruby-api/scalaris
chmod u+x contrib/init.d/scalaris
chmod u+x contrib/init.d/scalaris-monitor

