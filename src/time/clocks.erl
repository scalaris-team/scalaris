%  @copyright 2016 Zuse Institute Berlin

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
%% @doc    functions for querying clocks

%% @version $Id$
-module(clocks).
-author('schuett@zib.de').
-vsn('$Id$').

%-on_load(on_load/0).

-export([init/0,
         get_monotonic_clock/0, get_monotonic_clock_res/0,
         get_realtime_clock/0, get_realtime_clock_res/0,
         get_ptp0_clock/0, get_ptp0_clock_res/0,
         get_ptp1_clock/0, get_ptp1_clock_res/0,
         get_ptp2_clock/0, get_ptp2_clock_res/0]).

-export([test/0]).

-spec init() -> ok.
init() ->
    %% loads the shared library
    SoName = filename:join(filename:join(code:lib_dir(scalaris), bin), ?MODULE),
    erlang:load_nif(SoName, 0).

-spec get_monotonic_clock() -> failed | {non_neg_integer(), non_neg_integer()}.
get_monotonic_clock() ->
    "NIF library not loaded".

-spec get_monotonic_clock_res() -> failed | {non_neg_integer(), non_neg_integer()}.
get_monotonic_clock_res() ->
    "NIF library not loaded".

-spec get_realtime_clock() -> failed | {non_neg_integer(), non_neg_integer()}.
get_realtime_clock() ->
    "NIF library not loaded".

-spec get_realtime_clock_res() -> failed | {non_neg_integer(), non_neg_integer()}.
get_realtime_clock_res() ->
    "NIF library not loaded".

-spec get_ptp0_clock() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp0_clock() ->
    "NIF library not loaded".

-spec get_ptp0_clock_res() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp0_clock_res() ->
    "NIF library not loaded".

-spec get_ptp1_clock() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp1_clock() ->
    "NIF library not loaded".

-spec get_ptp1_clock_res() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp1_clock_res() ->
    "NIF library not loaded".

-spec get_ptp2_clock() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp2_clock() ->
    "NIF library not loaded".

-spec get_ptp2_clock_res() -> failed | {non_neg_integer(), non_neg_integer()}.
get_ptp2_clock_res() ->
    "NIF library not loaded".

-spec test() -> ok.
test() ->
    init(),
    io:format("monotonic ~p:~p~n", [get_monotonic_clock(), get_monotonic_clock_res()]),
    io:format("realtime ~p:~p~n", [get_realtime_clock(), get_realtime_clock_res()]),
    io:format("ptp0 ~p:~p~n", [get_ptp0_clock(), get_ptp0_clock_res()]),
    io:format("ptp1 ~p:~p~n", [get_ptp1_clock(), get_ptp1_clock_res()]),
    io:format("ptp2 ~p:~p~n", [get_ptp2_clock(), get_ptp2_clock_res()]),
    ok.
