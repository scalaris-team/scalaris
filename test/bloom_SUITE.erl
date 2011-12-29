%  @copyright 2010-2011 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    bloom_SUITE.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Tests for bloom filter module.
%%% @end
%%% Created : 06/04/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: $

-module(bloom_SUITE).

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

-define(BLOOM, bloom).
-define(HFS, hfs_lhsp).

-define(Fpr_Test_NumTests, 30).

all() -> [
          tester_add,
          tester_add_list,
          tester_join,
          tester_equals,
          tester_fpr,
          %fprof,
          performance
         ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    _ = crypto:start(),
    Config.

end_per_suite(_Config) ->
    crypto:stop(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_add(?BLOOM:key(), ?BLOOM:key()) -> true.
prop_add(X, Y) ->
    B1 = newBloom(10, 0.1),
    B2 = ?BLOOM:add(B1, X),
    ?assert(?BLOOM:is_element(B2, X)),
    B3 = ?BLOOM:add(B2, Y),
    ?assert(?BLOOM:is_element(B3, X)),
    ?assert(?BLOOM:is_element(B3, Y)).

tester_add(_) ->
    tester:test(?MODULE, prop_add, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_add_list([?BLOOM:key(),...]) -> true.
prop_add_list(Items) ->
    B1 = newBloom(erlang:length(Items), 0.1),
    B2 = ?BLOOM:add_list(B1, Items),
    lists:foreach(fun(X) -> ?assert(?BLOOM:is_element(B2, X)) end, Items),
    true.

tester_add_list(_) ->
    tester:test(?MODULE, prop_add_list, 1, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_join([?BLOOM:key(),...], [?BLOOM:key(),...]) -> true.
prop_join(List1, List2) ->
    BSize = erlang:length(List1) + erlang:length(List2),
    B1 = ?BLOOM:add_list(newBloom(BSize, 0.1), List1),
    B2 = ?BLOOM:add_list(newBloom(BSize, 0.1), List2),
    B3 = ?BLOOM:join(B1, B2),
    lists:foreach(fun(X) -> ?assert(?BLOOM:is_element(B1, X) andalso
                                        ?BLOOM:is_element(B3, X)) end, List1),
    lists:foreach(fun(X) -> ?assert(?BLOOM:is_element(B2, X) andalso
                                        ?BLOOM:is_element(B3, X)) end, List2),
    true.

tester_join(_) ->
    tester:test(?MODULE, prop_join, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_equals([?BLOOM:key(),...]) -> true.
prop_equals(List) ->
    B1 = ?BLOOM:add_list(newBloom(erlang:length(List), 0.1), List),
    B2 = ?BLOOM:add_list(newBloom(erlang:length(List), 0.1), List),
    ?assert(?BLOOM:equals(B1, B2)).

tester_equals(_) ->
    tester:test(?MODULE, prop_equals, 1, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_fpr(100..10000) -> true.
prop_fpr(ElementCount) ->
    DestFpr = randoms:rand_uniform(1, 500) / 1000,
    %DestFpr = 0.01,
    FalsePositives = util:p_repeatAndAccumulate(
                       fun measure_fp/2,
                       [DestFpr, ElementCount],
                       ?Fpr_Test_NumTests,
                       fun(X, Y) -> X + Y end,
                       0),
    AvgFpr = FalsePositives / ?Fpr_Test_NumTests,
    ct:pal("FalsePositives=~p - NumberOfTests=~p - Elements=~p~n"
           "DestFpr: ~f~nMeasured Fpr: ~f~n
            BloomFilter=~p~n",
           [FalsePositives, ?Fpr_Test_NumTests, ElementCount, DestFpr, AvgFpr,
            ?BLOOM:print(?BLOOM:new(ElementCount, DestFpr))]),
    %?assert(DestFpr >= AvgFpr orelse DestFpr * 1.3 >= AvgFpr),
    true.

tester_fpr(_) ->
    tester:test(?MODULE, prop_fpr, 1, 1, [{threads, 2}]).

%% @doc measures false positives by adding 1..MaxElements into a new BF
%%      and checking number of found items which are not in the BF
measure_fp(DestFpr, MaxElements) ->
    BF = newBloom(MaxElements, DestFpr),
    BF1 = for_to_ex(1, MaxElements, fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, BF),
    NumNotIn = trunc(10 / DestFpr),
    %count found items which should not be in the bloom filter
    NumFound = for_to_ex(MaxElements + 1,
                         MaxElements + 1 + NumNotIn,
                         fun(I) -> case ?BLOOM:is_element(BF1, I) of
                                       true -> 1;
                                       false -> 0
                                   end
                         end,
                         fun(X, Y) -> X + Y end,
                         0),
    NumFound / NumNotIn.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

performance(_) ->
    %parameter
    ExecTimes = 100,
    ToAdd = 2000, % req: mod 2 = 0

    %measure Build times
    BuildTime =
        measure_util:time_avg(fun() -> for_to_ex(1, ToAdd,
                                                 fun(I) -> I end,
                                                 fun(I, B) -> ?BLOOM:add(B, I) end,
                                                 newBloom(ToAdd, 0.1))
                              end,
                              [], ExecTimes, false),
    %measure join time
    BF1 = for_to_ex(1,
                    round(ToAdd / 2),
                    fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(ToAdd, 0.1)),
    BF2 = for_to_ex(round(ToAdd / 2) + 1,
                    ToAdd,
                    fun(I) -> I end, fun(I, B) -> ?BLOOM:add(B, I) end, newBloom(ToAdd, 0.1)),
    JoinTime =
        measure_util:time_avg(fun() -> ?BLOOM:join(BF1, BF2) end, [], ExecTimes, false),

    %print results
    ct:pal("EXECUTION TIMES in microseconds
           PARAMETER: AddedItems=~p ; ExecTimes=~p
           BuildTimes: ~p
           JoinTimes : ~p",
           [ToAdd, ExecTimes, 
            measure_util:print_result(BuildTime, ms), 
            measure_util:print_result(JoinTime, us)]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fprof(_) ->
    fprof:trace(start, "bloom_fprof.trace"),
    BF = newBloom(100, 0.1),
    _ = ?BLOOM:add(BF, 5423452345),
    fprof:trace(stop),
    fprof:profile(file, "bloom_fprof.trace"),
    fprof:analyse([{dest, "bloom_fprof.analysis"}, {cols, 120}]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

newBloom(ElementNum, Fpr) ->
    HFCount = ?BLOOM:calc_HF_numEx(ElementNum, Fpr),
    Hfs = ?HFS:new(HFCount),
    ?BLOOM:new(ElementNum, Fpr, Hfs).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% UTILS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

for_to_ex(I, N, Fun, AccuFun, Accu) ->
    NewAccu = AccuFun(Fun(I), Accu),
    if
        I < N ->
            for_to_ex(I + 1, N, Fun, AccuFun, NewAccu);
        I =:= N ->
            NewAccu;
        I > N ->
            failed
    end.
