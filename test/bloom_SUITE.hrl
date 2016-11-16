%  @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Maik Lange <MLange@informatik.hu-berlin.de>

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_p_add_list(BF0Items::[?BLOOM:key()], Items::[?BLOOM:key()]) -> true.
prop_p_add_list(BF0Items, Items) ->
    BF0 = newBloom(erlang:max(10, erlang:length(Items)), 0.1),
    BF = ?BLOOM:add_list(BF0, BF0Items),
    
    ?equals(lists:foldl(fun(Item, Acc) -> ?BLOOM:add(Acc, Item) end, BF, Items),
            ?BLOOM:add_list(BF, Items)).

tester_p_add_list(_) ->
    prop_p_add_list([], []),
    prop_p_add_list([], [6,7,8]),
    prop_p_add_list([6,7,8], [88,103,15,128,219]),
    tester:test(?MODULE, prop_p_add_list, 2, 10000, [{threads, 2}]).

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
    prop_add(one, 0.5359298222471391),
    tester:test(?MODULE, prop_add, 2, 100, [{threads, 2}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_add_list([?BLOOM:key(),...]) -> true.
prop_add_list(Items) ->
    B1 = newBloom(erlang:length(Items), 0.1),
    B2 = ?BLOOM:add_list(B1, Items),
    lists:foreach(fun(X) -> ?assert(?BLOOM:is_element(B2, X)) end, Items),
    ?equals(?BLOOM:get_property(B2, items_count), length(Items)).

tester_add_list(_) ->
    tester:test(?MODULE, prop_add_list, 1, 10, [{threads, 2}]).

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

-spec prop_fpr(500..10000, string | int) -> true.
prop_fpr(ItemCount, ItemType) ->
    InList = random_list(ItemType, ItemCount),
    
    DestFpr = randoms:rand_uniform(1, 100) / 1000,
    DestFPRList = [DestFpr, DestFpr*0.8, DestFpr*0.5],
    ExampleBF = ?BLOOM:new_fpr(ItemCount, DestFpr),
    
    FPs = [{util:repeat(
              fun measure_fpr/3, [Fpr, {InList, ItemCount}, ItemType],
              ?Fpr_Test_NumTests,
              [parallel, {accumulate, fun(X, Y) -> X + Y end, 0}])
               / ?Fpr_Test_NumTests,
            Fpr}
           || Fpr <- DestFPRList],
    FPs2 = [{D, M, (1 - D/M) * 100,
             if M-D =< 0 -> "ok"; true -> "fail" end }
           || {M, D} <- FPs],
    ct:pal("ItemCount=~p ; ItemType=~p ; Tests=~p ; Functions=~p ; CompressionRate=~.2f~n"
               "DestFpr, Measured, Diff in %, Status~n~p",
               [ItemCount, ItemType, ?Fpr_Test_NumTests,
                ?BLOOM:get_property(ExampleBF, hfs_size),
                ?BLOOM:get_property(ExampleBF, size) / ItemCount, FPs2]),
    true.

measure_fpr(DestFpr, {InList, ItemCount}, ListItemType) ->
    InitBF = ?BLOOM:new_fpr(ItemCount, DestFpr),
    BF = ?BLOOM:add_list(InitBF, InList),
    
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

eprof(_) ->
    Count = 1000,
    BF = newBloom(Count, 0.1),
    Items = [randoms:getRandomInt() || _ <- lists:seq(1, Count)],
        
    _ = eprof:start(),
    Fun = fun() -> ?BLOOM:add_list(BF, Items) end,
    eprof:profile([], Fun),
    eprof:stop_profiling(),
    eprof:analyze(procs, [{sort, time}]),
    
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fprof(_) ->
    Count = 1000,
    BF = newBloom(Count, 0.1),
    Items = [randoms:getRandomInt() || _ <- lists:seq(1, Count)],
        
    fprof:apply(?BLOOM, add, [BF, Items]),
    fprof:profile(),
    fprof:analyse([{cols, 120}]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

newBloom(ElementNum, Fpr) ->
    ?BLOOM:new_fpr(ElementNum, Fpr).


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
