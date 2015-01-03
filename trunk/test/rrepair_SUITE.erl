%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @author Nico Kruber <kruber@zib.de>
%% @doc    Tests for rep update module.
%% @end
%% @version $Id$
-module(rrepair_SUITE).
-author('malange@informatik.hu-berlin.de').
-author('kruber@zib.de').
-vsn('$Id$').

%% no proto scheduler for this suite
-define(proto_sched(_Action), ok).

-include("rrepair_SUITE.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [
     {group, basic},
     {group, tester_tests},
     {group, gsession_ttl},
     {group, repair}
    ].

groups() ->
    [
     {gsession_ttl,  [sequence], [session_ttl]},
     {tester_tests, [parallel], [
                           tester_map_key_to_interval,
                           tester_map_key_to_quadrant,
                           tester_map_interval,
                           tester_find_sync_interval,
                           tester_merkle_compress_hashlist%,
%%                            tester_merkle_compress_cmp_result
                                ]},
     {basic,  [parallel], [
                           get_symmetric_keys_test,
                           check_quadrant_intervals
                          ]},
     {repair, [sequence], [
                           {upd_trivial,  [sequence], repair_default()},
                           {upd_shash,    [sequence], repair_default()},
                           {upd_bloom,    [sequence], repair_default()}, %{repeat_until_any_fail, 1000}
                           {upd_merkle,   [sequence], repair_default()},
                           {upd_art,      [sequence], repair_default()},
                           {regen_trivial,[sequence], repair_default() ++ regen_special()},
                           {regen_shash,  [sequence], repair_default() ++ regen_special()},
                           {regen_bloom,  [sequence], repair_default() ++ regen_special()},
                           {regen_merkle, [sequence], repair_default() ++ regen_special()},
                           {regen_art,    [sequence], repair_default() ++ regen_special()},
                           {mixed_trivial,[sequence], repair_default()},
                           {mixed_shash,  [sequence], repair_default()},
                           {mixed_bloom,  [sequence], repair_default()},
                           {mixed_merkle, [sequence], repair_default()},
                           {mixed_art,    [sequence], repair_default()}
                          ]}
    ].

suite() -> [{timetrap, {seconds, 15}}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Basic Functions Group
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_symmetric_keys_test(Config) ->
    Conf2 = unittest_helper:start_minimal_procs(Config, [], true),
    ToTest = lists:sort(get_symmetric_keys(4)),
    ToBe = lists:sort(?RT:get_replica_keys(?MINUS_INFINITY)),
    unittest_helper:stop_minimal_procs(Conf2),
    ?equals_w_note(ToTest, ToBe,
                   io_lib:format("GenKeys=~w~nRTKeys=~w", [ToTest, ToBe])),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_quadrant_intervals(_) ->
    Quadrants = rr_recon:quadrant_intervals(),
    ?equals(lists:foldl(fun intervals:union/2, intervals:empty(), Quadrants),
            intervals:all()),
    % all continuous:
    ?equals([Q || Q <- Quadrants, not intervals:is_continuous(Q)],
            []),
    % pair-wise non-overlapping:
    ?equals([{Q1, Q2} || Q1 <- Quadrants,
                         Q2 <- Quadrants,
                         Q1 =/= Q2,
                         not intervals:is_empty(intervals:intersection(Q1, Q2))],
            []).

-spec prop_map_key_to_interval(?RT:key(), intervals:interval()) -> true.
prop_map_key_to_interval(Key, I) ->
    Mapped = rr_recon:map_key_to_interval(Key, I),
    RGrp = ?RT:get_replica_keys(Key),
    InGrp = [X || X <- RGrp, intervals:in(X, I)],
    case intervals:in(Key, I) of
        true ->
            ?equals_w_note(Mapped, Key,
                           io_lib:format("Violation: if key is in i than mapped key equals key!~n"
                                             "Key=~p~nMapped=~p", [Key, Mapped]));
        false when Mapped =/= none ->
            ?compare(fun erlang:'=/='/2, InGrp, []),
            case InGrp of
                [W] -> ?equals(Mapped, W);
                [_|_] ->
                    ?assert(intervals:in(Mapped, I)),
                    % mapped should always be the closest one to Key in I
                    ?compare(fun({A1, _}, {A2, _}) -> A1 =:= A2 end,
                             {rr_recon:key_dist(Key, Mapped), Mapped},
                             lists:min([{rr_recon:key_dist(Key, M), M} || M <- InGrp]))
            end;
        _ -> ?equals(InGrp, [])
    end.

tester_map_key_to_interval(_) ->
    [Q1, Q2, Q3 | _] = ?RT:get_replica_keys(?MINUS_INFINITY),
    prop_map_key_to_interval(Q1, intervals:new('[', Q1, Q2, ']')),
    prop_map_key_to_interval(Q2, intervals:new('[', Q1, Q2, ']')),
    prop_map_key_to_interval(Q3, intervals:new('[', Q1, Q2, ']')),
    prop_map_key_to_interval(Q2, intervals:union(intervals:new(Q1), intervals:new(Q3))),
    tester:test(?MODULE, prop_map_key_to_interval, 2, 1000, [{threads, 4}]).

-spec prop_map_key_to_quadrant(?RT:key(), Quadrant::1..4) -> true.
prop_map_key_to_quadrant(Key, Quadrant) ->
    ?equals(rr_recon:map_key_to_quadrant(Key, Quadrant),
            rr_recon:map_key_to_interval(Key, lists:nth(Quadrant, rr_recon:quadrant_intervals()))).

tester_map_key_to_quadrant(_) ->
    tester:test(?MODULE, prop_map_key_to_quadrant, 2, 1000, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_map_interval(A::intervals:continuous_interval(),
                        B::intervals:continuous_interval()) -> true.
prop_map_interval(A, B) ->
    Quadrants = rr_recon:quadrant_intervals(),
    % need a B that is in a single quadrant - just use the first one to get
    % deterministic behaviour:
    BQ = hd(rr_recon:quadrant_subints_(B, rr_recon:quadrant_intervals(), [])),
    SA = rr_recon:map_interval(A, BQ),

    % SA must be a sub-interval of A
    ?compare(fun intervals:is_subset/2, SA, A),

    % SA must be in a single quadrant
    ?equals([I || Q <- Quadrants,
                  not intervals:is_empty(
                    I = intervals:intersection(SA, Q))],
            ?IIF(intervals:is_empty(SA), [], [SA])),

    % if mapped back, must at least be a subset of BQ:
    case intervals:is_empty(SA) of
        true -> true;
        _ ->
            ?compare(fun intervals:is_subset/2, rr_recon:map_interval(BQ, SA), BQ)
    end.

tester_map_interval(_) ->
    case rt_SUITE:default_rt_has_chord_keys() of
        true ->
            prop_map_interval(intervals:new(?MINUS_INFINITY),
                              intervals:new('[', 45418374902990035001132940685036047259, ?MINUS_INFINITY, ']')),
            prop_map_interval(intervals:new(?MINUS_INFINITY), intervals:all()),
            prop_map_interval([{'[',0,52800909270899328435375133601130059363,')'}],
                              [{'[',234596648080609640182865804133877994395,293423227623586592154289572207917413067,')'}]);
        _ -> ok
    end,
    tester:test(?MODULE, prop_map_interval, 2, 1000, [{threads, 1}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_find_sync_interval(intervals:continuous_interval(), intervals:continuous_interval()) -> true.
prop_find_sync_interval(A, B) ->
    SyncI = rr_recon:find_sync_interval(A, B),
    case intervals:is_empty(SyncI) of
        true -> true;
        _ ->
            % continuous:
            ?assert_w_note(intervals:is_continuous(SyncI), io_lib:format("SyncI: ~p", [SyncI])),
            % mapped to A, subset of A:
            ?assert_w_note(intervals:is_subset(SyncI, A), io_lib:format("SyncI: ~p", [SyncI])),
            Quadrants = rr_recon:quadrant_intervals(),
            % only in a single quadrant:
            ?equals([SyncI || Q <- Quadrants,
                              not intervals:is_empty(intervals:intersection(SyncI, Q))],
                    [SyncI]),
            % SyncI must be a subset of B if mapped back
            ?compare(fun intervals:is_subset/2, rr_recon:map_interval(B, SyncI), B)
    end.

tester_find_sync_interval(_) ->
    tester:test(?MODULE, prop_find_sync_interval, 2, 100, [{threads, 4}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_merkle_compress_hashlist(Nodes::[merkle_tree:mt_node()], SigSizeI::0..160, SigSizeL::0..160) -> true.
prop_merkle_compress_hashlist(Nodes0, SigSizeI, SigSizeL) ->
    % fix node list which may contain nil hashes:
    % let it crash if the format of a merkle tree node changes
    Nodes = [begin
                 case N of
                     {nil, Count, LeafCount, ItemCount, Interval, ChildList} ->
                         {randoms:getRandomInt(), Count, LeafCount, ItemCount,
                          Interval, ChildList};
                     {_Hash, _Cnt, _LCnt, _ICnt, _Interval, _ChildList} ->
                         N;
                     {nil, ItemCount, Bucket, Interval} ->
                         {randoms:getRandomInt(), ItemCount, Bucket, Interval};
                     {_Hash, _ItemCount, _Bucket, _Interval} ->
                         N
                 end
             end || N <- Nodes0],
    Bin = rr_recon:merkle_compress_hashlist(Nodes, <<>>, SigSizeI, SigSizeL),
    HashesRed = [begin
                     H0 = merkle_tree:get_hash(N),
                     SigSize = ?IIF(merkle_tree:is_leaf(N), SigSizeL, SigSizeI),
                     <<H:SigSize/integer-unit:1>> = <<H0:SigSize>>,
                     {H, merkle_tree:is_leaf(N)}
                 end || N <- Nodes],
    ?equals(rr_recon:merkle_decompress_hashlist(Bin, [], SigSizeI, SigSizeL), HashesRed).

tester_merkle_compress_hashlist(_) ->
    tester:test(?MODULE, prop_merkle_compress_hashlist, 3, 1000, [{threads, 4}]).

%% -spec prop_merkle_compress_cmp_result(CmpRes::[rr_recon:merkle_cmp_result()],
%%                                       SigSize::0..160) -> true.
%% prop_merkle_compress_cmp_result(CmpRes, SigSize) ->
%%     {Flags, HashesBin} =
%%         rr_recon:merkle_compress_cmp_result(CmpRes, <<>>, <<>>, SigSize),
%%     CmpResRed = [case Cmp of
%%                      {H0} ->
%%                          <<H:SigSize/integer-unit:1>> = <<H0:SigSize>>,
%%                          {H};
%%                      X -> X
%%                  end || Cmp <- CmpRes],
%%     ?equals(rr_recon:merkle_decompress_cmp_result(Flags, HashesBin, [], SigSize),
%%             CmpResRed).
%%
%% tester_merkle_compress_cmp_result(_) ->
%%     tester:test(?MODULE, prop_merkle_compress_cmp_result, 2, 1000, [{threads, 4}]).
