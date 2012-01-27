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

-define(Fpr_Test_NumTests, 25).

all() -> [
          tester_add,
          tester_add_list,
          tester_join,
          tester_equals
          %tester_fpr
          %fprof,
         ].

suite() ->
    [
     {timetrap, {seconds, 45}}
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
    ?assert(?BLOOM:is_element(B3, Y)),
    ?equals(?BLOOM:get_property(B1, items_count), 0),
    ?equals(?BLOOM:get_property(B2, items_count), 1),
    ?equals(?BLOOM:get_property(B3, items_count), 2).

tester_add(_) ->
    tester:test(?MODULE, prop_add, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_add_list([?BLOOM:key(),...]) -> true.
prop_add_list(Items) ->
    B1 = newBloom(erlang:length(Items), 0.1),
    B2 = ?BLOOM:add(B1, Items),
    lists:foreach(fun(X) -> ?assert(?BLOOM:is_element(B2, X)) end, Items),
    ?equals(?BLOOM:get_property(B2, items_count), length(Items)).

tester_add_list(_) ->
    tester:test(?MODULE, prop_add_list, 1, 10, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_join([?BLOOM:key(),...], [?BLOOM:key(),...]) -> true.
prop_join(List1, List2) ->
    BSize = erlang:length(List1) + erlang:length(List2),
    B1 = ?BLOOM:add(newBloom(BSize, 0.1), List1),
    B2 = ?BLOOM:add(newBloom(BSize, 0.1), List2),
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
    B1 = ?BLOOM:add(newBloom(erlang:length(List), 0.1), List),
    B2 = ?BLOOM:add(newBloom(erlang:length(List), 0.1), List),
    ?assert(?BLOOM:equals(B1, B2)).

tester_equals(_) ->
    tester:test(?MODULE, prop_equals, 1, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_fpr(500..10000, string | int) -> true.
prop_fpr(ItemCount, ItemType) ->
    InList = random_list(ItemType, ItemCount),
    
    DestFpr = randoms:rand_uniform(1, 100) / 1000,
    DestFPRList = [DestFpr, DestFpr*0.8, DestFpr*0.5],
    HFCount = ?BLOOM:calc_HF_numEx(ItemCount, DestFpr),
    
    FPs = [{util:p_repeatAndAccumulate(
              fun measure_fpr/3, [{Fpr, HFCount}, {InList, ItemCount}, ItemType],
              ?Fpr_Test_NumTests, fun(X, Y) -> X + Y end, 
              0) / ?Fpr_Test_NumTests, Fpr} 
           || Fpr <- DestFPRList],
    FPs2 = [{D, M, (1 - D/M) * 100, 
             if M-D =< 0 -> "ok"; true -> "fail" end } 
           || {M, D} <- FPs],
    ct:pal("ItemCount=~p ; ItemType=~p ; Tests=~p ; Functions=~p ; CompressionRate=~.2f~n"
               "DestFpr, Measured, Diff in %, Status~n~p",
               [ItemCount, ItemType, ?Fpr_Test_NumTests, HFCount, 
                ?BLOOM:calc_least_size(ItemCount, DestFpr) / ItemCount, FPs2]),
    true.    

measure_fpr({DestFpr, HFCount}, {InList, ItemCount}, ListItemType) ->
    Hfs = ?HFS:new(HFCount),    
    InitBF = ?BLOOM:new(ItemCount, DestFpr, Hfs),    
    BF = ?BLOOM:add(InitBF, InList),
    
    Count = trunc(10 / ?BLOOM:get_property(BF, fpr)),
    _NotInList = random_list(ListItemType, Count),
    NotInList = lists:filter(fun(I) -> not lists:member(I, InList) end, _NotInList),    
    Found = lists:foldl(fun(I, Acc) -> 
                                Acc + case ?BLOOM:is_element(BF, I) of
                                          true -> 1;
                                          false -> 0
                                      end
                        end, 0, NotInList),
    Found / Count.

tester_fpr(_) ->
    tester:test(?MODULE, prop_fpr, 2, 2, [{threads, 1}]).

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

-spec random_list(ItemType :: int | string, pos_integer()) -> [string() | pos_integer()].
random_list(int, Count) ->
    util:for_to_ex(1, Count, fun(_) -> randoms:getRandomInt() end);
random_list(string, Count) ->
    util:for_to_ex(1, Count, fun(_) -> randoms:getRandomString() end).

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
