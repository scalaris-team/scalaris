#   Copyright 2007-2011 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
AC_INIT(scalaris, 0.3.0+svn, schuett@zib.de)

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
             fi],
            [AC_PATH_PROG([BROWSER], [xdg-open])])
AC_SUBST(BROWSER)

AS_IF([test ! -x "$BROWSER"],
      [AC_MSG_WARN(["$BROWSER" was not found or is not executable ('make test-vts' will likely fail)])])

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

ERLANG_NATIVE_FLAGS=
AS_IF([test "x$COMPILE_NATIVE" != xno], [ERLANG_NATIVE_FLAGS=", native"])

# required libs:
ERLANG_UNAVAILABLE_LIBS=
AC_ERLANG_CHECK_LIB(erts, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS erts")

AC_ERLANG_CHECK_LIB(kernel, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS kernel")

AC_ERLANG_CHECK_LIB(stdlib, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS stdlib")

AC_ERLANG_CHECK_LIB(compiler, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS compiler")

AC_ERLANG_CHECK_LIB(crypto, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS crypto")

AC_ERLANG_CHECK_LIB(tools, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS tools")

AC_ERLANG_CHECK_LIB(inets, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS inets")

AC_ERLANG_CHECK_LIB(ssl, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS ssl")

AC_ERLANG_CHECK_LIB(xmerl, , ERLANG_UNAVAILABLE_LIBS="$ERLANG_UNAVAILABLE_LIBS xmerl")

AS_IF([test "x$ERLANG_UNAVAILABLE_LIBS" != x], AC_MSG_FAILURE([$ERLANG_UNAVAILABLE_LIBS not found - you won't be able compile the erlang sources]))

# libs for optional build tasks:
AC_ERLANG_CHECK_LIB(common_test, , AC_MSG_NOTICE([erlang-common_test not found - you won't be able to run the unit tests without common_test]))

AC_ERLANG_CHECK_LIB(edoc, , AC_MSG_NOTICE([erlang-edoc not found - you won't be able to create the documentation or run 'make install']))

AC_CHECK_FILE($ERLANG_LIB_DIR_common_test/priv/bin/run_test,
              [AC_SUBST(RUN_TEST,[$ERLANG_LIB_DIR_common_test/priv/bin/run_test])],
              [AC_CHECK_FILE($ac_cv_erlang_root_dir/bin/run_test,
              [AC_SUBST(RUN_TEST,[$ac_cv_erlang_root_dir/bin/run_test])],
              [AC_PATH_PROG([RUN_TEST],[run_test])
               if test -z $ac_cv_path_RUN_TEST ; then
                  AC_MSG_NOTICE([run_test not found - on erlang < R14 consider running install.sh in the common_test directory otherwise you won't be able to run the unit tests])
               fi])])

ERLANG_TOKE_FLAGS=
AS_IF([test "x$TOKEPREFIX" != xno],
            [
            ERLANG_TOKE_MESSAGE=
            AS_IF([test "x$TOKEPREFIX" != x], [export ERL_LIBS="$TOKEPREFIX"])
            AC_ERLANG_CHECK_LIB(toke,
              [AC_CHECK_FILE($ERLANG_LIB_DIR_toke/priv/libtoke.so,
                [HAVETOKE=", {d, have_toke}"
                 HAVETOKE_EDOC=", {have_toke, true}"
                 HAVETOKE_DIALYZER="-Dhave_toke"
                 ERLANG_TOKE_FLAGS="-pa $ERLANG_LIB_DIR_toke/ebin"
                ],
                [HAVETOKE=""
                 HAVETOKE_EDOC=""
                 HAVETOKE_DIALYZER=""
                 ERLANG_TOKE_MESSAGE="toke library libtoke.so not found"
                ])
              ],
              [HAVETOKE=""
               HAVETOKE_EDOC=""
               HAVETOKE_DIALYZER=""
               ERLANG_TOKE_MESSAGE="toke erlang library not found"
              ])
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
       [GBTREESISBUILTIN=""; GBTREESISBUILTIN_EDOC=""; GBTREESISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([gb_tree() is a built-in type])],
       [GBTREESISBUILTIN=", {d, types_not_builtin}"; GBTREESISBUILTIN_EDOC=", {types_not_builtin, true}"; GBTREESISBUILTIN_DIALYZER="-Dtypes_not_builtin"; AC_MSG_NOTICE([gb_tree() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::term()).], [ok])],
       [TERMISBUILTIN=""; TERMISBUILTIN_EDOC=""; TERMISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([term() is a built-in type])],
       [TERMISBUILTIN=", {d, term_not_builtin}"; TERMISBUILTIN_EDOC=", {term_not_builtin, true}"; TERMISBUILTIN_DIALYZER="-Dterm_not_builtin"; AC_MSG_NOTICE([term() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::node()).], [ok])],
       [NODEISBUILTIN=""; NODEISBUILTIN_EDOC=""; NODEISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([node() is a built-in type])],
       [NODEISBUILTIN=", {d, node_not_builtin}"; NODEISBUILTIN_EDOC=", {node_not_builtin, true}"; NODEISBUILTIN_DIALYZER="-Dnode_not_builtin"; AC_MSG_NOTICE([node() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::module()).], [ok])],
       [MODULEISBUILTIN=""; MODULEISBUILTIN_EDOC=""; MODULEISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([module() is a built-in type])],
       [MODULEISBUILTIN=", {d, module_not_builtin}"; MODULEISBUILTIN_EDOC=", {module_not_builtin, true}"; MODULEISBUILTIN_DIALYZER="-Dmodule_not_builtin"; AC_MSG_NOTICE([module() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::boolean()).], [ok])],
       [BOOLEANISBUILTIN=""; BOOLEANISBUILTIN_EDOC=""; BOOLEANISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([boolean() is a built-in type])],
       [BOOLEANISBUILTIN=", {d, boolean_not_builtin}"; BOOLEANISBUILTIN_EDOC=", {boolean_not_builtin, true}"; BOOLEANISBUILTIN_DIALYZER="-Dboolean_not_builtin"; AC_MSG_NOTICE([boolean() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::tid()).], [ok])],
       [TIDISBUILTIN=""; TIDISBUILTIN_EDOC=""; TIDISBUILTIN_DIALYZER=""; AC_MSG_NOTICE([tid() is a built-in type])],
       [TIDISBUILTIN=", {d, tid_not_builtin}"; TIDISBUILTIN_EDOC=", {tid_not_builtin, true}"; TIDISBUILTIN_DIALYZER="-Dtid_not_builtin"; AC_MSG_NOTICE([tid() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::{ok, mytype()} | ok).], [ok])],
       [RECURSIVETYPESALLOWED=""; RECURSIVETYPESALLOWED_EDOC=""; RECURSIVETYPESALLOWED_DIALYZER=""; AC_MSG_NOTICE([recursive types are supported])],
       [RECURSIVETYPESALLOWED=", {d, recursive_types_are_not_allowed}"; RECURSIVETYPESALLOWED_EDOC=", {recursive_types_are_not_allowed, true}"; RECURSIVETYPESALLOWED_DIALYZER="-Drecursive_types_are_not_allowed"; AC_MSG_NOTICE([recursive types are not supported])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::{ok, mytype2()} | ok). -type(mytype2() :: ok).], [ok])],
       [TYPEFORWARDDECLARATIONS=""; TYPEFORWARDDECLARATIONS_EDOC=""; TYPEFORWARDDECLARATIONS_DIALYZER=""; AC_MSG_NOTICE([type forward declarations are supported])],
       [TYPEFORWARDDECLARATIONS=", {d, type_forward_declarations_are_not_allowed}"; TYPEFORWARDDECLARATIONS_EDOC=", {type_forward_declarations_are_not_allowed, true}"; TYPEFORWARDDECLARATIONS_DIALYZER="-Dtype_forward_declarations_are_not_allowed"; AC_MSG_NOTICE([type forward declarations are not supported])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([[-export_type([mytype/0]). -type(mytype()::ok).]], [ok])],
       [EXPORTTYPESUPPORT=", {d, with_export_type_support}"; EXPORTTYPESUPPORT_EDOC=", {d, with_export_type_support}"; EXPORTTYPESUPPORT_DIALYZER="-Dwith_export_type_support"; DIALYZER_FLAGS="$DIALYZER_FLAGS -Dwith_export_type_support"; AC_MSG_NOTICE([export_type is supported])],
       [EXPORTTYPESUPPORT=""; EXPORTTYPESUPPORT_EDOC=""; EXPORTTYPESUPPORT_DIALYZER=""; AC_MSG_NOTICE([export_type is not supported])])

if test "x$RECURSIVETYPESALLOWED" != x -o "x$TYPEFORWARDDECLARATIONS" != x; then
   AC_MSG_NOTICE([forward declarations or recursive types are not allowed])
   FORWARDORRECURSIVE=", {d, forward_or_recursive_types_are_not_allowed}"
   FORWARDORRECURSIVE_EDOC=", {forward_or_recursive_types_are_not_allowed, true}"
   FORWARDORRECURSIVE_DIALYZER="-Dforward_or_recursive_types_are_not_allowed"
fi

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([], [crypto:start()])],
       [AC_MSG_NOTICE([crypto:start() is available])],
       [AC_MSG_FAILURE([cannot start the crypto subsystem])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::{ok, arity()}).], [ok])],
       [AC_MSG_NOTICE([arity() is available])],
       [AC_MSG_FAILURE([the type arity() is not available; you are probably using Erlang < R13 which is not supported anymore])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([], [httpc:set_option(max_sessions, 1)])],
       [AC_MSG_NOTICE([httpc is available])],
       [AC_MSG_NOTICE([httpc unavailable, using wrapper module]);EMAKEFILECOMPILECOMPAT=["{\"contrib/compat/httpc.erl\",[debug_info, nowarn_unused_function, nowarn_obsolete_guard, nowarn_unused_vars,{outdir, \"ebin\"}]}."]])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-compile({parse_transform, ct_line}).], [ok])],
       [CTLINESUPPORT=", {d, have_ctline_support}"; CTLINESUPPORT_EDOC=", {have_ctline_support, true}"; CTLINESUPPORT_DIALYZER="-Dhave_ctline_support"; AC_MSG_NOTICE([ct_line is available])],
       [CTLINESUPPORT=""; CTLINESUPPORT_EDOC=""; CTLINESUPPORT_DIALYZER=""; AC_MSG_NOTICE([ct_line is not available])])

AS_IF(
       [test -n `$RUN_TEST --help | grep ct_hooks 2>/dev/null && echo ok` ],
       [CTHOOKS_FLAGS="-ct_hooks scalaris_cth"; CTHOOKS_EMAKEFILE=", {d, have_cthooks_support}"; CTHOOKS_EDOC=", {have_cthooks_support, true}"; CTHOOKS_DIALYZER="-Dhave_cthooks_support"; AC_MSG_NOTICE([ct_hooks is available])],
       [CTHOOKS_FLAGS=""; CTHOOKS_EMAKEFILE=""; CTHOOKS_EDOC=""; CTHOOKS_DIALYZER=""; AC_MSG_NOTICE([ct_hooks is not available])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-callback init() -> ok.], [ok])],
       [CALLBACKSUPPORT=", {d, have_callback_support}"; CALLBACKSUPPORT_EDOC=", {have_callback_support, true}"; CALLBACKSUPPORT_DIALYZER="-Dhave_callback_support"; AC_MSG_NOTICE([-callback is available])],
       [CALLBACKSUPPORT=""; CALLBACKSUPPORT_EDOC=""; CALLBACKSUPPORT_DIALYZER=""; AC_MSG_NOTICE([-callback is not available])])

EMAKEFILEDEFINES="$HAVETOKE $GBTREESISBUILTIN $TERMISBUILTIN $NODEISBUILTIN $MODULEISBUILTIN $BOOLEANISBUILTIN $TIDISBUILTIN $RECURSIVETYPESALLOWED $TYPEFORWARDDECLARATIONS $FORWARDORRECURSIVE $EXPORTTYPESUPPORT $CTLINESUPPORT $CTHOOKS_EMAKEFILE $CALLBACKSUPPORT $ERLANG_NATIVE_FLAGS"
EDOCMACROS="$HAVETOKE_EDOC $GBTREESISBUILTIN_EDOC $TERMISBUILTIN_EDOC $NODEISBUILTIN_EDOC $MODULEISBUILTIN_EDOC $BOOLEANISBUILTIN_EDOC $TIDISBUILTIN_EDOC $RECURSIVETYPESALLOWED_EDOC $TYPEFORWARDDECLARATIONS_EDOC $FORWARDORRECURSIVE_EDOC $EXPORTTYPESUPPORT_EDOC $CTLINESUPPORT_EDOC $CTHOOKS_EDOC $CALLBACKSUPPORT_EDOC"
EDOCMACROS="${EDOCMACROS#, }"
DIALYZER_FLAGS="$HAVETOKE_DIALYZER $GBTREESISBUILTIN_DIALYZER $TERMISBUILTIN_DIALYZER $NODEISBUILTIN_DIALYZER $MODULEISBUILTIN_DIALYZER $BOOLEANISBUILTIN_DIALYZER $TIDISBUILTIN_DIALYZER $RECURSIVETYPESALLOWED_DIALYZER $TYPEFORWARDDECLARATIONS_DIALYZER $FORWARDORRECURSIVE_DIALYZER $EXPORTTYPESUPPORT_DIALYZER $CTLINESUPPORT_DIALYZER $CTHOOKS_DIALYZER $CALLBACKSUPPORT_DIALYZER"

AC_SUBST(EMAKEFILEDEFINES)
AC_SUBST(EMAKEFILECOMPILECOMPAT)
AC_SUBST(DIALYZER_FLAGS)
AC_SUBST(CTHOOKS_FLAGS)
AC_SUBST(EDOCMACROS)

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
# check python
#
###########################################################
PYTHONSITELIBDIR=
ENABLEPYTHONINSTALL=
AM_PATH_PYTHON(,, [:])
AS_IF([test "$PYTHON" != ":"],
      [PYTHONSITELIBDIR=$am_cv_python_pythondir
       ENABLEPYTHONINSTALL="install-python"])
AC_SUBST(ENABLEPYTHONINSTALL)
AC_SUBST(PYTHONSITELIBDIR)

###########################################################
#
# check python3
#
###########################################################

ENABLEPYTHON3INSTALL=
PYTHON3SITELIBDIR=
AC_ARG_WITH([python3-sitelibdir],
            [AS_HELP_STRING([--with-python3-sitelibdir=SITELIBDIR],
            [where to install python3 libraries])],
            [if test -n "$withval"; then
               PYTHON3SITELIBDIR="$withval"
               ENABLEPYTHON3INSTALL="install-python3"
             fi],
            [])
AC_SUBST(ENABLEPYTHON3INSTALL)
AC_SUBST(PYTHON3SITELIBDIR)

###########################################################
#
# check opennebula
#
###########################################################

ENABLEONEINSTALL=
ONEPREFIX=
AC_ARG_ENABLE([opennebula],
              AS_HELP_STRING([--disable-opennebula],
                 [disable support for opennebula]),
              [ONEPREFIX=no])
AC_ARG_ENABLE([opennebula],
              AS_HELP_STRING([--enable-opennebula@<:@=DIR@:>@],
                 [enable support for opennebula]),
              [ENABLEONEINSTALL="install-one"
               ONEPREFIX=$enableval])
AC_SUBST(ENABLEONEINSTALL)

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

AC_CONFIG_FILES([Emakefile Makefile bin/scalarisctl java-api/scalaris java-api/scalaris-java.conf])
AC_OUTPUT

chmod u+x bin/scalarisctl
chmod u+x java-api/scalaris

