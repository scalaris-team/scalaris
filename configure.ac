#   Copyright 2007-2017 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
AC_INIT(scalaris, 0.9.0+git, scalaris@googlegroups.com)
AC_CONFIG_MACRO_DIR([m4])
# we will never do automake, just pull in config.guess, config.sub, ... for boost
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE()

EMAKEFILEDEFINES=
EDOCMACROS=
DIALYZER_FLAGS=

AC_PATH_PROG(FALSE, false)
AC_SUBST(FALSE)
AC_PATH_PROG(TRUE, true)
AC_SUBST(TRUE)
AC_PROG_SED
AC_PATH_PROG(DOXYGEN, doxygen)
AC_SUBST(DOXYGEN)

###########################################################
#
# check for C++ and boost
#
###########################################################

ENABLE_CPP=
AC_ARG_ENABLE([cpp],
              AS_HELP_STRING([--disable-cpp],
                 [disable support for the C++ bindings]),
              [ENABLE_CPP=$enableval],
              [ENABLE_CPP=$enableval])

enabled_cxx=0
if test "x$ENABLE_CPP" != xno ; then
  # C++ and boost checks
  AX_CXX_COMPILE_STDCXX_11([noext],[optional])
  if test "x$HAVE_CXX11" = "x1" ; then
    have_boost_147=
    AX_BOOST_BASE([1.47], [have_boost_147=yes], [have_boost_147=no])
    AX_BOOST_SYSTEM
    AX_BOOST_REGEX
    AX_BOOST_ASIO
    AX_BOOST_PROGRAM_OPTIONS
    AX_BOOST_UNIT_TEST_FRAMEWORK

    AX_CHECK_OPENSSL([ENABLE_SSL=yes])

    if test "x$have_boost_147" = "xyes" -a "x$ax_cv_boost_system" = "xyes" -a "x$ax_cv_boost_regex" = "xyes"  -a "x$ax_cv_boost_program_options" = "xyes"  -a "x$ax_cv_boost_unit_test_framework" = "xyes" -a "x$ENABLE_SSL" = "xyes" ; then
      enabled_cxx=1
    fi
  fi
fi

if test "x$enabled_cxx" = "x0" ; then
  AC_MSG_NOTICE([c++ bindings disabled ('make cpp' will fail)])
fi

CXXSHELL=$SHELL
if test -f /usr/local/bin/bash; then
  CXXSHELL=/usr/local/bin/bash
elif test -f /bin/bash; then
  CXXSHELL=/bin/bash
fi

AC_PATH_PROG(CLANGFORMAT, clang-format, $ac_cv_path_TRUE)
AC_PATH_PROG(CLANGTIDY, clang-tidy, $ac_cv_path_TRUE)

AC_SUBST(OPENSSL_INCLUDES)
AC_SUBST(OPENSSL_LDFLAGS)
AC_SUBST(OPENSSL_LIBS)

AC_SUBST(CXX)
AC_SUBST(CXXFLAGS)
AC_SUBST(CXXSHELL)
AC_SUBST(CLANGFORMAT)
AC_SUBST(CLANGTIDY)

###########################################################
#
# check tokyo cabinet and toke
#
###########################################################

ENABLE_TOKE=
TOKEPREFIX=
AC_ARG_ENABLE([toke],
              AS_HELP_STRING([--enable-toke@<:@=DIR@:>@],
                 [enable support for tokyo cabinet through toke]),
              [ENABLE_TOKE=yes
               TOKEPREFIX=$enableval])

###########################################################
#
# check hanoidb
#
###########################################################

ENABLE_HANOIDB=
HANOIDBPREFIX=
AC_ARG_ENABLE([hanoidb],
              AS_HELP_STRING([--enable-hanoidb@<:@=DIR@:>@],
                 [enable support for hanoidb]),
              [ENABLE_HANOIDB=yes
               HANOIDBPREFIX=$enableval])

###########################################################
#
# check bitcask
#
###########################################################

ENABLE_BITCASK=
BITCASKPREFIX=
AC_ARG_ENABLE([bitcask],
              AS_HELP_STRING([--enable-bitcask@<:@=DIR@:>@],
                 [enable support for bitcask]),
              [ENABLE_BITCASK=yes
               BITCASKPREFIX=$enableval])

###########################################################
#
# check erlang_js
#
###########################################################

ENABLE_ERLANGJS=
ERLANGJSPREFIX=
AC_ARG_ENABLE([erlang-js],
              AS_HELP_STRING([--enable-erlang-js@<:@=DIR@:>@],
                 [enable support for erlang_js]),
              [ENABLE_ERLANGJS=yes
               ERLANGJSPREFIX=$enableval])

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
AC_PATH_PROG(SCREEN, screen, [$FALSE])
if test "$ac_cv_path_SCREEN" = "$FALSE" ; then
  AC_MSG_NOTICE([screen not found - you won't be able to run scalaris deamonized with screen, i.e. 'scalarisctl -d --screen'])
fi

###########################################################
#
# check sudo, runuser availability for the init.d script
#
###########################################################

AC_PATH_PROG(SUDO, sudo, [$FALSE])

RUNUSER=""
AC_ARG_ENABLE([runuser],
              AS_HELP_STRING([--disable-runuser],
                 [disable support for runuser (use sudo instead)]))
AS_IF([test "$enable_runuser" != "no"],
      [AC_PATH_PROG(RUNUSER, runuser, "", [$PATH$PATH_SEPARATOR/usr/sbin])])

if test "$ac_cv_path_SUDO" = "$FALSE" -a "$ac_cv_path_RUNUSER" = ""; then
  AC_MSG_NOTICE([sudo or runuser not found - you won't be able to use our init.d script])
fi

###########################################################
#
# check systemd support
#
###########################################################

INSTALL_INIT=install-initd
SYSTEMD_UNITDIR=
AC_ARG_WITH([systemd],
            [AS_HELP_STRING([--with-systemd[[=UNITDIR]]],
                            [use systemd, optionally specify the directory for systemd service files])],
            [if test -d "$withval"; then
               SYSTEMD_UNITDIR="$withval"
               INSTALL_INIT="install-systemd"
             else
               if test "$withval" = "yes"; then
                 SYSTEMD_UNITDIR="\${prefix}/lib/systemd/system/"
                 INSTALL_INIT="install-systemd"
               fi
             fi],
            [])
AC_SUBST(INSTALL_INIT)
AC_SUBST(SYSTEMD_UNITDIR)

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
# enable new transaction protocol
#
###########################################################

AC_ARG_ENABLE([txnew],
              AS_HELP_STRING([--enable-txnew], [enable new transaction protocol]),
              [AS_IF([test "no" != "$enableval"],
                     [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, enable_txnew}"
                      EDOCMACROS="${EDOCMACROS}, {enable_txnew, true}"
                      DIALYZER_FLAGS="${DIALYZER_FLAGS} -Denable_txnew"])])

###########################################################
#
# check erlang
#
###########################################################
AC_LANG([Erlang])

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

# split erlang erts version:
set `echo $erlang_cv_erts_ver | $SED 's/\./ /g'`
ERTS_MAJOR=$1
ERTS_MINOR=$2
ERTS_MAINT=${3:-0}

# require Erlang >= R14B04, i.e. ERTS >= 5.8.5
AS_IF([test $ERTS_MAJOR -gt 5 -o \
       '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -gt 8 ')' -o \
       '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -eq 8 -a $ERTS_MAINT -ge 5 ')'],
      [],
      [AC_MSG_FAILURE([Erlang >= R14B04 required])])

AC_PATH_PROG(EPMD, epmd, [$FALSE], "$PATH:$ERLANG_ROOT_DIR/bin:$ERLANG_ROOT_DIR/erts-$ERLANG_ERTS_VER/bin")

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

# toke
ERLANG_TOKE_FLAGS=
AS_IF([test "x$TOKEPREFIX" != xno],
            [
            ERLANG_TOKE_MESSAGE=
            AS_IF([test "x$TOKEPREFIX" != x], [export ERL_LIBS="$TOKEPREFIX"])
            AC_ERLANG_CHECK_LIB(toke,
              [AC_CHECK_FILE($ERLANG_LIB_DIR_toke/priv/libtoke.so,
                [ERLANG_TOKE_FLAGS="-pa $ERLANG_LIB_DIR_toke/ebin"],
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

# hanoidb
ERLANG_HANOIDB_FLAGS=
AS_IF([test "x$HANOIDBPREFIX" != xno],
            [
            ERLANG_HANOIDB_MESSAGE=
            AS_IF([test "x$HANOIDBPREFIX" != x], [export ERL_LIBS="$HANOIDBPREFIX"])
            AC_ERLANG_CHECK_LIB(hanoidb,
              [ERLANG_HANOIDB_FLAGS="-pa $ERLANG_LIB_DIR_hanoidb/ebin"],
              [ERLANG_HANOIDB_MESSAGE="hanoidb erlang not found"])
            AS_IF([test "x$HANOIDBPREFIX" != x], [export ERL_LIBS=""])
            AS_IF([test "x$ERLANG_HANOIDB_MESSAGE" != x],
              [AS_IF([test "x$ENABLE_HANOIDB" != xyes],
                [ERLANG_HANOIDB_MESSAGE="$ERLANG_HANOIDB_MESSAGE, disabling hanoidb support..."
                 AC_MSG_NOTICE($ERLANG_HANOIDB_MESSAGE)],
                AC_MSG_FAILURE($ERLANG_HANOIDB_MESSAGE))])
            ])
AC_SUBST([ERLANG_HANOIDB_FLAGS])

# bitcask
ERLANG_BITCASK_FLAGS=
BITCASK_LIBS=
AS_IF([test "x$BITCASKPREFIX" != xno],
            [
            ERLANG_BITCASK_MESSAGE=
            AS_IF([test "x$BITCASKPREFIX" != x], [export ERL_LIBS="$BITCASKPREFIX"])
            AC_ERLANG_CHECK_LIB(bitcask,
              [
                EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_bitcask}"
                EDOCMACROS="${EDOCMACROS}, {have_bitcask, true}"
                DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_bitcask"
                ERLANG_BITCASK_FLAGS="-pa $ERLANG_LIB_DIR_bitcask/ebin"
                BITCASK_LIBS="$ERLANG_LIB_DIR_bitcask"
              ],
              [ERLANG_BITCASK_MESSAGE="bitcask erlang not found"])
            AS_IF([test "x$BITCASKPREFIX" != x], [export ERL_LIBS=""])
            AS_IF([test "x$ERLANG_BITCASK_MESSAGE" != x],
              [AS_IF([test "x$ENABLE_BITCASK" != xyes],
                [ERLANG_BITCASK_MESSAGE="$ERLANG_BITCASK_MESSAGE, disabling bitcask support..."
                 AC_MSG_NOTICE($ERLANG_BITCASK_MESSAGE)],
                AC_MSG_FAILURE($ERLANG_BITCASK_MESSAGE))])
            ])
AC_SUBST([ERLANG_BITCASK_FLAGS])
AC_SUBST([BITCASK_LIBS])

# erlang_js
ERLANG_ERLANGJS_FLAGS=
AS_IF([test "x$ERLANGJSPREFIX" != xno],
            [
            ERLANG_ERLANGJS_MESSAGE=
            AS_IF([test "x$ERLANGJSPREFIX" != x], [export ERL_LIBS="$ERLANGJSPREFIX"])
            AC_ERLANG_CHECK_LIB(erlang_js,
              [AC_CHECK_FILE($ERLANG_LIB_DIR_erlang_js/priv/erlang_js_drv.so,
                [ERLANG_ERLANGJS_FLAGS="-pa $ERLANG_LIB_DIR_erlang_js/ebin"],
                [ERLANG_ERLANGJS_MESSAGE="erlang_js library erlang_js_drv.so not found"])
              ],
              [ERLANG_ERLANGJS_MESSAGE="erlang_js erlang library not found"])
            AS_IF([test "x$ERLANGJSPREFIX" != x], [export ERL_LIBS=""])
            AS_IF([test "x$ERLANG_ERLANGJS_MESSAGE" != x],
              [AS_IF([test "x$ENABLE_ERLANGJS" != xyes],
                [ERLANG_ERLANGJS_MESSAGE="$ERLANG_ERLANGJS_MESSAGE, disabling erlang_js support..."
                 AC_MSG_NOTICE($ERLANG_ERLANGJS_MESSAGE)],
                AC_MSG_FAILURE($ERLANG_ERLANGJS_MESSAGE))])
            ])
AC_SUBST([ERLANG_ERLANGJS_FLAGS])

crypto_start=`"${ERL}" -noshell -eval 'io:format("~p~n", [[crypto:start()]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_start}" = "ok"],
      [AC_MSG_NOTICE([crypto:start() is available])],
      [AC_MSG_FAILURE([cannot start the crypto subsystem])])

crypto_rand_uniform=`"${ERL}" -noshell -eval 'code:load_file(crypto), io:format("~p~n", [[erlang:function_exported(crypto,rand_uniform,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_rand_uniform}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_crypto_randuniform_support}"
       EDOCMACROS="${EDOCMACROS}, {have_crypto_randuniform_support, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_crypto_randuniform_support"
       AC_MSG_NOTICE([crypto:rand_uniform/2 is available])],
      [AC_MSG_NOTICE([crypto:rand_uniform/2 is not available])])

crypto_hash=`"${ERL}" -noshell -eval 'code:load_file(crypto), io:format("~p~n", [[erlang:function_exported(crypto,hash,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_hash}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_crypto_hash}"
       EDOCMACROS="${EDOCMACROS}, {with_crypto_hash, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_crypto_hash"
       AC_MSG_NOTICE([crypto:hash/2 is available and usable])],
      [AC_MSG_NOTICE([crypto:hash/2 is not available or not usable, falling back to crypto:md5/1 and crypto:sha1/1])])

crypto_bytes_to_integer=`"${ERL}" -noshell -eval 'code:load_file(crypto), io:format("~p~n", [[erlang:function_exported(crypto,bytes_to_integer,1)]]), erlang:halt().' | tail -1`
AS_IF([test "${crypto_bytes_to_integer}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_crypto_bytes_to_integer}"
       EDOCMACROS="${EDOCMACROS}, {with_crypto_bytes_to_integer, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_crypto_bytes_to_integer"
       AC_MSG_NOTICE([crypto:bytes_to_integer/1 is available and usable])],
      [AC_MSG_NOTICE([crypto:bytes_to_integer/1 is not available or not usable, falling back to rand:uniform/2])])

ct_line=`"${ERL}" -noshell -eval 'code:load_file(ct_line), io:format("~p~n", [[erlang:function_exported(ct_line,parse_transform,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${ct_line}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_ctline_support}"
       EDOCMACROS="${EDOCMACROS}, {have_ctline_support, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_ctline_support"
       AC_MSG_NOTICE([ct_line is available])],
      [AC_MSG_NOTICE([ct_line is not available])])

rand_module=`"${ERL}" -noshell -eval 'code:load_file(rand), io:format("~p~n", [[erlang:function_exported(rand,uniform,0)]]), erlang:halt().' | tail -1`
AS_IF([test "${rand_module}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_rand}"
       EDOCMACROS="${EDOCMACROS}, {with_rand, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_rand"
       AC_MSG_NOTICE([rand:uniform/0 is available and usable])],
      [AC_MSG_NOTICE([rand:uniform/0 is not available or not usable, falling back to random:uniform/0])])

AC_RUN_IFELSE(
       [AC_LANG_PROGRAM([-callback init() -> ok.], [ok])],
       [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_callback_support}"
        EDOCMACROS="${EDOCMACROS}, {have_callback_support, true}"
        DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_callback_support"
        AC_MSG_NOTICE([-callback is available])],
       [AC_MSG_NOTICE([-callback is not available])])

maps_module=`"${ERL}" -noshell -eval 'code:load_file(maps), io:format("~p~n", [[erlang:function_exported(maps,new,0)]]), erlang:halt().' | tail -1`
AS_IF([test "${maps_module}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, with_maps}"
       EDOCMACROS="${EDOCMACROS}, {with_maps, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dwith_maps"
       AC_MSG_NOTICE([maps are available and seem to be usable])],
      [AC_MSG_NOTICE([maps are not available or not usable, falling back to gb_trees])])

# creates forms specifying a minimal erlang module which spezifies a
# type based on dict:dict() and checks if compilation of this module fails
dict_type=`"${ERL}" -noshell -eval 'X = fun(S) -> element(2,erl_parse:parse_form(element(2,erl_scan:string(S)))) end, io:format("~p~n", [[element(1, compile:forms([X("-module(test)."), X("-type test() :: dict:dict().")]))]]), erlang:halt().' 2> /dev/null | tail -1`
AS_IF([test "${dict_type}" = "ok"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, namespaced_dict}"
       EDOCMACROS="${EDOCMACROS}, {namespaced_dict, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dnamespaced_dict"
       AC_MSG_NOTICE([erlang dict type is defined as dict:dict()])],
      [AC_MSG_NOTICE([erlang dict type is defined as dict()])])

###########################################################
#
# check for file:send_file/5 for yaws
#
###########################################################
YAWS_OPTIONS=
HAVE_ERLANG_SENDFILE=false

AC_MSG_CHECKING([for file:sendfile/5])
file_sendfile=`"${ERL}" -noshell -eval 'code:ensure_loaded(file), io:format("~p~n",[[erlang:function_exported(file,sendfile,5)]]), erlang:halt().' | tail -1`
if test "$file_sendfile" = true; then
  AS_IF([test $ERTS_MAJOR -gt 5 -o \
         '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -gt 9 ')' -o \
         '(' $ERTS_MAJOR -eq 5 -a $ERTS_MINOR -eq 9 -a $ERTS_MAINT -ge 1 ')'],
        [AC_MSG_RESULT([yes])
         HAVE_ERLANG_SENDFILE=true],
        [AC_MSG_RESULT([no])])
else
        AC_MSG_RESULT([no])
fi

AS_IF([test "$HAVE_ERLANG_SENDFILE" = true],
      [YAWS_OPTIONS=", {d, 'HAVE_ERLANG_SENDFILE'}"
       YAWS_OPTIONS_DIALYZER=" -DHAVE_ERLANG_SENDFILE"
       AC_MSG_NOTICE([using file:sendfile/5 for yaws])],
      [YAWS_OPTIONS=""
       YAWS_OPTIONS_DIALYZER=""
       AC_MSG_NOTICE([using fallback gen_tcp:send/2 for yaws])])

AS_IF([test "${crypto_hash}" = "true"],
      [YAWS_OPTIONS="${YAWS_OPTIONS}, {d, 'HAVE_CRYPTO_HASH'}"
       YAWS_OPTIONS_DIALYZER="${YAWS_OPTIONS_DIALYZER} -DHAVE_CRYPTO_HASH"])

###########################################################
#
# check for erlang:timestamp/0 for yaws
#
###########################################################
erlang_timestamp=`"${ERL}" -noshell -eval 'io:format("~p~n", [[erlang:function_exported(erlang,timestamp,0)]]), erlang:halt().' | tail -1`
AS_IF([test "${erlang_timestamp}" = "true"],
      [AC_MSG_NOTICE([erlang:timestamp() is available])],
      [YAWS_OPTIONS="${YAWS_OPTIONS}, {d, 'HAVE_ERLANG_NOW'}"
       YAWS_OPTIONS_DIALYZER="${YAWS_OPTIONS_DIALYZER} -DHAVE_ERLANG_NOW"
       AC_MSG_NOTICE([erlang:timestamp() is not available])])

###########################################################
#
# check for ssl:getstat/2 for ssl
#
###########################################################
## https://github.com/erlang/otp/commit/84fd2c325c9e38b5ea2307b6133c3d15b33a3241
SSL_GETSTAT=false
ssl_getstat=`"${ERL}" -noshell -eval 'io:format("~p~n", [[erlang:function_exported(ssl,getstat,2)]]), erlang:halt().' | tail -1`
AS_IF([test "${ssl_getstat}" = "true"],
      [EMAKEFILEDEFINES="${EMAKEFILEDEFINES}, {d, have_ssl_getstat}"
       EDOCMACROS="${EDOCMACROS}, {have_ssl_getstat, true}"
       DIALYZER_FLAGS="${DIALYZER_FLAGS} -Dhave_ssl_getstat"
       SSL_GETSTAT=true
       AC_MSG_NOTICE([ssl:getstat() is available])],
      [AC_MSG_NOTICE([ssl:getstat() is not available])])

###########################################################
#
# all defines together...
#
###########################################################

EMAKEFILEDEFINES="${EMAKEFILEDEFINES}"
EDOCMACROS="${EDOCMACROS#, }"
DIALYZER_FLAGS="${DIALYZER_FLAGS## }${YAWS_OPTIONS_DIALYZER}"

AC_SUBST(EMAKEFILEDEFINES)
AC_SUBST(EDOCMACROS)
AC_SUBST(DIALYZER_FLAGS)

AC_SUBST(EMAKEFILECOMPILECOMPAT)
AC_SUBST(YAWS_OPTIONS)

###########################################################
#
# check for routing table type
#
###########################################################

RT=rt_chord
AC_ARG_WITH([rt],
            [AS_HELP_STRING([--with-rt[[=RTFILE]]],
                            [optionally specify the routing table type (default=rt_chord)])],
            [if test "$withval"; then
               RT="$withval"
             fi],
            [])
AC_SUBST(RT)

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

AS_IF([test "$ac_cv_path_PYTHON2" != ""],
      [PYTHON2=$ac_cv_path_PYTHON2
       AC_MSG_CHECKING([for python 2.x sitelibdir])
       AC_MSG_RESULT([$PYTHON2SITELIBDIR])
       ENABLEPYTHON2INSTALL="install-python"],
      [PYTHON2=$FALSE])
AC_SUBST(PYTHON2)
AC_SUBST(PYTHON2SITELIBDIR)
AC_SUBST(ENABLEPYTHON2INSTALL)

PYTHON3SITELIBDIR=
ENABLEPYTHON3INSTALL=
AC_CACHE_CHECK([for python 3.x], [ac_cv_path_PYTHON3],
  [AC_PATH_PROGS_FEATURE_CHECK([PYTHON3], [python3 python],
    [[$ac_path_PYTHON3 -V 2>&1 | grep "^Python 3." > /dev/null && \
      PYTHON3SITELIBDIR=`$ac_path_PYTHON3 -c 'from distutils.sysconfig import get_python_lib; print(get_python_lib(0,0,prefix="${prefix}"))'  2>/dev/null` && \
      ac_cv_path_PYTHON3=$ac_path_PYTHON3 ac_path_PYTHON3_found=:]],
    [AC_MSG_RESULT([not found])])])

PYTHON3=
PYTHON3_2TO3=
ENABLEPYTHON3=python3-not-found
ENABLEPYTHON3INSTALL=
AS_IF([test "$ac_cv_path_PYTHON3" != ""],
      [PYTHON3=$ac_cv_path_PYTHON3
       AC_MSG_CHECKING([for python 3.x sitelibdir])
       AC_MSG_RESULT([$PYTHON3SITELIBDIR])
       AC_PATH_PROG([PYTHON3_2TO3], [2to3], "")
       AS_IF([test -x "$PYTHON3_2TO3"],
             [ENABLEPYTHON3="python3-compile"
              ENABLEPYTHON3INSTALL="install-python3"],
             [AC_MSG_WARN([2to3 not found - you won't be able to build the Python3 API])])],
      [PYTHON3=$FALSE])
AC_SUBST(PYTHON3)
AC_SUBST(PYTHON3_2TO3)
AC_SUBST(PYTHON3SITELIBDIR)
AC_SUBST(ENABLEPYTHON3)
AC_SUBST(ENABLEPYTHON3INSTALL)

###########################################################
#
# check MACOSX vs. Linux for flexbisonparse driver
#
###########################################################

#http://unix.stackexchange.com/questions/35183/how-do-i-identify-which-linux-distro-is-running

LINUXVERSION="unknown linux"
if test -f /etc/SuSE-release ; then
  LINUXVERSION=`cat /etc/SuSE-release`
elif test -f /etc/redhat-release ; then
  LINUXVERSION=`cat /etc/redhat-release`
elif test -f /etc/fedora-release ; then
  LINUXVERSION=`cat /etc/fedora-release`
elif test -f /etc/debian_release ; then
  LINUXVERSION=`cat /etc/debian_release`
elif test -f /etc/debian_version ; then
  LINUXVERSION=`cat /etc/debian_version`
elif test -f /etc/gentoo-release ; then
  LINUXVERSION=`cat /etc/gentoo-release`
elif test -f /etc/arch-release ; then
  LINUXVERSION=`uname -a`
fi

OS=""
case `uname -s` in
 linux*)
     NIFFLAGS="-shared"
     DRIVER_OS=LINUX
     OS=$LINUXVERSION
     ;;
 Linux*)
     NIFFLAGS="-shared"
     DRIVER_OS=LINUX
     OS=$LINUXVERSION
     ;;
 Darwin*)
     NIFFLAGS="-flat_namespace -undefined dynamic_lookup"
     DRIVER_OS=MACOSX
     OS="OSX `sw_vers -productVersion`"
     ;;
 darwin*)
     NIFFLAGS="-flat_namespace -undefined dynamic_lookup"
     DRIVER_OS=MACOSX
     OS="OSX `sw_vers -productVersion`"
     ;;
esac

GIT=`command -v git`
STAT=`command -v stat`
SHA1SUM=`command -v sha1sum`
SRC="download"
CONFIGURESTATUS=""

if test "x$GIT" != "x" -a -d .git; then
  SRC=`git log --pretty=format:'%h' -n 1 .`
  CONFIGURESTATUS=`git status -s configure | cut -d ' ' -f 2`
elif test "x$GIT" != "x"; then
  CONFIGURESTATUS=`git hash-object configure`
elif test "x$DRIVER_OS" = "xMACOSX" ; then
  # BSD
  SIZE=`/usr/bin/stat -f "%z" configure`
  CONFIGURESTATUS=`(/usr/bin/printf "blob $SIZE\0" ; /bin/cat configure) | /usr/bin/openssl sha1`
elif test "x$STAT" != "x" -a "x$SHA1SUM" != "x" ; then
  CONFIGURESTATUS=`(stat --printf="blob %s\0" configure; cat configure) | sha1sum -b | cut -d " " -f1`
else
  CONFIGURESTATUS=""
fi

echo "detected OS is: '$OS' $SRC $CONFIGURESTATUS"
AC_SUBST(DRIVER_OS)
AC_SUBST(NIFFLAGS)

mkdir -p ~/.yaws 2> /dev/null

mkdir -p python3-api
$SED -e "s|python-api|python3-api|g" \
     -e "s|@PYTHONSITELIBDIR@|@PYTHON3SITELIBDIR@|g" \
     -e "s|PYTHON2|PYTHON3|g" python-api/scalaris.in > python3-api/scalaris.in

AC_CONFIG_FILES([Emakefile Makefile bin/scalarisctl include/rt.hrl cpp-api/Makefile java-api/scalaris java-api/scalaris-java.conf contrib/init.d/scalaris contrib/init.d/scalaris-monitor python-api/scalaris python3-api/scalaris ruby-api/scalaris])
AC_OUTPUT

chmod u+x bin/scalarisctl
chmod u+x java-api/scalaris
chmod u+x python-api/scalaris
chmod u+x python3-api/scalaris
chmod u+x ruby-api/scalaris
chmod u+x contrib/init.d/scalaris
chmod u+x contrib/init.d/scalaris-monitor

echo "Erlang"
if test "x$BITCASK_LIBS" = "x" ; then
  echo "  Bitcask              : no"
else
  echo "  Bitcask              : yes"
fi

if test "x$ERLANG_HANOIDB_FLAGS" = "x" ; then
  echo "  hanoidb              : no"
else
  echo "  hanoidb              : yes"
fi

if test "x$ERLANG_TOKE_FLAGS" = "x" ; then
  echo "  toke                 : no"
else
  echo "  toke                 : yes"
fi

if test "x$SSL_GETSTAT_FLAGS" = "xtrue" ; then
  echo "  ssl                  : no"
else
  echo "  ssl                  : yes"
fi

if test "x$enabled_cxx" = "x0" ; then
  echo "C++ API                : no"
else
  echo "C++ API                : yes"
fi

if test "x$ENABLEPYTHON2INSTALL" = "xinstall-python" ; then
  echo "Python API             : yes"
else
  echo "Python API             : no"
fi

if test "x$ENABLEPYTHON3" = "xpython3-compile" ; then
  echo "Python3 API            : yes"
else
  echo "Python3 API            : no"
fi
