%%%-------------------------------------------------------------------
%%% File    : intervals_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/intervals.erl
%%%
%%% Created :  1 Apr 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(tester_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

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
    _ = crypto:start(),
    Config.

end_per_suite(_Config) ->
    %crypto:stop(), ???
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
    tester:register_type_checker({typedef, tester_SUITE, sorted_list}, tester_SUITE, is_sorted_list),
    tester:register_value_creator({typedef, tester_SUITE, sorted_list}, tester_SUITE, create_sorted_list, 2),
    tester:test(?MODULE, do_sort, 1, 25, []),
    tester:unregister_type_checker({typedef, tester_SUITE, sorted_list}),
    tester:unregister_value_creator({typedef, tester_SUITE, sorted_list}).
