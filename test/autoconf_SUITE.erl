% @copyright 2018 Zuse Institute Berlin

%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Unit tests for autoconf
%% @end
-module(autoconf_SUITE).
-author('schuett@zib.de').

-compile(export_all).

-include_lib("unittest.hrl").

all() ->
    [test_have_crypto_randuniform_support,
     test_with_crypto_hash,
     test_with_crypto_bytes_to_integer,
     test_with_maps,
     test_with_rand,
     test_have_ssl_handshake,
     test_have_ssl_getstat,
     test_have_new_stacktrace,
     test_namespaced_dict,
     test_HAVE_ERLANG_NOW
     ].


suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% have_crypto_randuniform_support
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(have_crypto_randuniform_support).
test_have_crypto_randuniform_support(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B", "17", "18", "19",
                                                           "20", "21", "22"])),
    ok.
-else.
test_have_crypto_randuniform_support(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), [])),
    ok.
-endif.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% with_crypto_hash
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(with_crypto_hash).
test_with_crypto_hash(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R16B", "17", "18", "19",
                                                           "20", "21", "22"])),
    ok.
-else.
test_with_crypto_hash(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01"])),
    ok.
-endif.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% with_crypto_bytes_to_integer
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(with_crypto_bytes_to_integer).
test_with_crypto_bytes_to_integer(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["17", "18", "19", "20",
                                                           "21", "22"])),
    ok.
-else.
test_with_crypto_bytes_to_integer(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B"])),
    ok.
-endif.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% with_maps
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(with_maps).
test_with_maps(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["18", "19",
                                                           "20", "21", "22"])),
    ok.
-else.
test_with_maps(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B",
                                                           "R15B01", "R16B",
                                                           "17"])),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% with_rand
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(with_rand).
test_with_rand(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["18", "19", "20", "21",
                                                           "22"])),
    ok.
-else.
test_with_rand(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B", "17"])),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% have_ssl_handshake
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(have_ssl_handshake).
test_have_ssl_handshake(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["21", "22"])),
    ok.
-else.
test_have_ssl_handshake(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04" , "R15B", "R15B01",
                                                           "R16B", "17", "18", "19",
                                                           "20"])),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% have_ssl_getstat
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(have_ssl_getstat).
test_have_ssl_getstat(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["19", "20", "21",
                                                           "22"])),
    ok.
-else.
test_have_ssl_getstat(_Config) ->
    ?assert_w_note(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B",
                                                                  "R15B01" "R16B",
                                                                  "17", "18"]),
                   erlang:system_info(otp_release)),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% have_new_stacktrace
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(have_new_stacktrace).
test_have_new_stacktrace(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["21", "22"])),
    ok.
-else.
test_have_new_stacktrace(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B", "17", "18", "19",
                                                           "20"])),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% namespaced_dict
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(namespaced_dict).
test_namespaced_dict(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["17", "18", "19", "20",
                                                           "21", "22"])),
    ok.
-else.
test_namespaced_dict(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B"])),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% HAVE_ERLANG_NOW
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(HAVE_ERLANG_NOW).
test_HAVE_ERLANG_NOW(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), [])),
    ok.
-else.
test_HAVE_ERLANG_NOW(_Config) ->
    ?assert(lists:member(erlang:system_info(otp_release), ["R14B04", "R15B", "R15B01",
                                                           "R16B", "17", "18", "19",
                                                           "20", "21", "22"])),
    ok.
-endif.
