% @copyright 2010-2012 Zuse Institute Berlin

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
%% @doc Unit tests for random tester
%% @end
%% @version $Id$
-module(tester_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include_lib("unittest.hrl").

all() ->
    [test_is_binary,
     test_sort].


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
% simple tester:test/3
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_is_binary_feeder(integer()) -> {integer()}.
prop_is_binary_feeder(Int) ->
    {Int}.

-spec prop_is_binary(integer()) -> binary().
prop_is_binary(Bin) ->
    term_to_binary(Bin).

test_is_binary(_Config) ->
    tester:test(?MODULE, prop_is_binary, 1, 25, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% tester:test/3 with value-creator and custom type checker
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type sorted_list() :: list(integer()).

is_sorted_list([]) ->
    true;
is_sorted_list([_I]) ->
    true;
is_sorted_list([I,J|L]) ->
    I =< J andalso is_sorted_list([J|L]).

-spec create_sorted_list(list(integer()), list(integer())) -> sorted_list().
create_sorted_list(L1, L2) ->
    %ct:pal("creating sorted list from ~p ~p", [L1, L2]),
    lists:sort(lists:append(L1, L2)).

-spec do_sort(sorted_list()) -> sorted_list().
do_sort(L) ->
    lists:sort(L).

test_sort(_Config) ->
    tester:register_type_checker({typedef, tester_SUITE, sorted_list, []}, tester_SUITE, is_sorted_list),
    tester:register_value_creator({typedef, tester_SUITE, sorted_list, []}, tester_SUITE, create_sorted_list, 2),
    tester:test(?MODULE, do_sort, 1, 25, []),
    tester:unregister_type_checker({typedef, tester_SUITE, sorted_list, []}),
    tester:unregister_value_creator({typedef, tester_SUITE, sorted_list, []}).
