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

AC_PREREQ(2.53)
AC_INIT(chordsharp, 0.0.1, schuett@zib.de)

###########################################################
#
# check xml2 and xslt
#
###########################################################
#AM_PATH_XML2
#AM_PATH_XSLT

###########################################################
#
# check erlang
#
###########################################################
AC_LANG_ERLANG

AC_ERLANG_PATH_ERLC
AC_ERLANG_NEED_ERL

AC_ERLANG_SUBST_ROOT_DIR
AC_ERLANG_SUBST_LIB_DIR

#ERTS_ROOT_DIR=`find $ERLANG_ROOT_DIR -name "erts-?.?*" | sort | tail -n 1`
#echo "checking for erts dir... $ERTS_ROOT_DIR"
#echo "checking for lib dir... $ERLANG_LIB_DIR"

AC_ERLANG_CHECK_LIB(common_test, , AC_MSG_NOTICE([you won't be able to run the unit tests without common_test]))

AC_CHECK_FILE($erlang_cv_lib_dir_common_test/priv/bin/run_test, , AC_MSG_NOTICE([please consider running install.sh in the common_test directory otherwise you won't be able to run the unit tests]))

#AC_SUBST(ERTS_ROOT_DIR)

#AC_PROG_LEX
#AC_PROG_YACC
#AC_PROG_CC

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

#SCALARIS_ROOT=`pwd`
#AC_SUBST(SCALARIS_ROOT)

AC_CONFIG_FILES([Makefile bin/scalarisctl bin/scalarisclient])

AC_OUTPUT

chmod u+x bin/scalarisctl
chmod u+x bin/scalarisclient
