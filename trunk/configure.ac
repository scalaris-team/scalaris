#   Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
AC_INIT(scalaris, 0.0.1, schuett@zib.de)

###########################################################
#
# check tokyo cabinet and tcerl
#
###########################################################
PKG_PROG_PKG_CONFIG

TCERLPREFIX=no
AC_ARG_ENABLE([tcerl], AS_HELP_STRING([enable support for tokyo cabinet]), [TCERLPREFIX=$enableval])

export PKG_CONFIG_PATH=$TCERLPREFIX/lib/pkgconfig
AS_IF([test "x$TCERLPREFIX" != xno],
            [PKG_CHECK_MODULES([TOKYOCABINET],[tokyocabinet], [])
             AC_SUBST([TOKYOCABINET_CFLAGS])
             AC_SUBST([TOKYOCABINET_LIBS])
             PKG_CHECK_MODULES([TCERLDRV],
                               [tcerldrv-1.0],
                               [[HAVETCERL='-define(HAVE_TCERL, true).']],
                               [HAVETCERL=""])])
AC_SUBST(HAVETCERL)

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

AS_IF([test "x$HAVETCERL" != x],
            [
            export ERL_LIBS="$TCERLPREFIX/lib/erlang/lib"
            AC_ERLANG_CHECK_LIB(tcerl, [ERLANG_TCERL_FLAGS="\"-pa $erlang_cv_lib_dir_tcerl/ebin -tcerl tcerldrvprefix=$TCERLPREFIX\""])
            AC_SUBST([ERLANG_TCERL_FLAGS])
            export ERL_LIBS=""
            ])

AC_ERLANG_CHECK_LIB(common_test, , AC_MSG_NOTICE([you won't be able to run the unit tests without common_test]))

AC_CHECK_FILE($ac_cv_erlang_lib_dir_common_test/priv/bin/run_test, , AC_MSG_NOTICE([please consider running install.sh in the common_test directory otherwise you won't be able to run the unit tests]))

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::gb_tree()).], [ok])],
       [GBTREESISBUILTIN=""; AC_MSG_NOTICE([gb_tree() is a built-in type])],
       [GBTREESISBUILTIN=", {d, types_not_builtin}"; AC_MSG_NOTICE([gb_tree() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::term()).], [ok])],
       [TERMISBUILTIN=""; AC_MSG_NOTICE([term() is a built-in type])],
       [TERMISBUILTIN=", {d, term_not_builtin}"; AC_MSG_NOTICE([term() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::node()).], [ok])],
       [NODEISBUILTIN=""; AC_MSG_NOTICE([node() is a built-in type])],
       [NODEISBUILTIN=", {d, node_not_builtin}"; AC_MSG_NOTICE([node() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::module()).], [ok])],
       [MODULEISBUILTIN=""; AC_MSG_NOTICE([module() is a built-in type])],
       [MODULEISBUILTIN=", {d, module_not_builtin}"; AC_MSG_NOTICE([module() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::boolean()).], [ok])],
       [BOOLEANISBUILTIN=""; AC_MSG_NOTICE([boolean() is a built-in type])],
       [BOOLEANISBUILTIN=", {d, boolean_not_builtin}"; AC_MSG_NOTICE([boolean() is not built-in])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-type(mytype()::ok | mytype()).], [ok])],
       [RECURSIVETYPESALLOWED=""; AC_MSG_NOTICE([recursive types are supported])],
       [RECURSIVETYPESALLOWED=", {d, recursive_types_are_not_allowed}"; AC_MSG_NOTICE([recursive types are not supported])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([], [crypto:start()])],
       [AC_MSG_NOTICE([crypto:start() is available])],
       [AC_MSG_FAILURE([cannot start the crypto subsystem])])

EMAKEFILEDEFINES="$GBTREESISBUILTIN $TERMISBUILTIN $NODEISBUILTIN $MODULEISBUILTIN $BOOLEANISBUILTIN $RECURSIVETYPESALLOWED"

AC_SUBST(GBTREESISBUILTIN)
AC_SUBST(TERMISBUILTIN)
AC_SUBST(NODEISBUILTIN)
AC_SUBST(MODULEISBUILTIN)
AC_SUBST(BOOLEANISBUILTIN)
AC_SUBST(RECURSIVETYPESALLOWED)
AC_SUBST(EMAKEFILEDEFINES)

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

AC_CONFIG_FILES([Emakefile Makefile bin/scalarisctl include/autoconf.hrl])
AC_OUTPUT

chmod u+x bin/scalarisctl

