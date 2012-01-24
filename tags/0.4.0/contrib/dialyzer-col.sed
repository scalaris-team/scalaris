# for color codes see http://en.wikipedia.org/wiki/ANSI_escape_code
# blue bold
s/^.*erl:[0123456789]*/\x1b[1;34m\0\x1b[0m/
# blue normal
s/is a supertype of the success typing/\x1b[34m\0\x1b[0m/
s/that the function might also return/\x1b[34m\0\x1b[0m/
s/but the inferred return is/\x1b[34m\0\x1b[0m/
s/but this value is unmatched/\x1b[34m\0\x1b[0m/
# green bold
s/will never be called/\x1b[1;32m\0\x1b[0m/
s/has no local return/\x1b[1;32m\0\x1b[0m/
# red bold
s/can never succeed/\x1b[1;31m\0\x1b[0m/
s/Call to missing or unexported function/\x1b[1;31m\0\x1b[0m/
s/contains an opaque term/\x1b[1;31m\0\x1b[0m/
s/contains opaque terms/\x1b[1;31m\0\x1b[0m/
s/might fail due to a possible race condition/\x1b[1;31m\0\x1b[0m/
s/breaks the contract/\x1b[1;31m\0\x1b[0m/
s/breaks the opaqueness of the term/\x1b[1;31m\0\x1b[0m/
s/can never match/\x1b[1;31m\0\x1b[0m/
s/Invalid type specification/\x1b[1;31m\0\x1b[0m/
s/Unknown types:/\x1b[1;31m\0\x1b[0m/
s/Unknown functions:/\x1b[1;31m\0\x1b[0m/

