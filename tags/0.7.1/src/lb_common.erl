%  @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    TODO: Add description to lb_common
%% @end
%% @version $Id$
-module(lb_common).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([calculateStddev/2, bestStddev/2, bestStddev/3,
         split_by_key/2, split_my_range/2]).


% Avg2 = average of load ^2
-spec calculateStddev(Avg::number(), Avg2::number()) -> float().
calculateStddev(Avg, Avg2) ->
    math:sqrt(Avg2 - Avg * Avg).

-spec default_sort_fun(Op1::{lb_op:lb_op(), integer()},
                       Op2::{lb_op:lb_op(), integer()}) -> boolean().
default_sort_fun({_Op1, Op1Change}, {_Op2, Op2Change}) ->
    Op1Change =< Op2Change.

% MinSum2Change = minimal required change of sum(load ^2)
-spec bestStddev(Ops::[lb_op:lb_op()], MinSum2Change::integer() | plus_infinity)
        -> [lb_op:lb_op()].
bestStddev(Ops, MinSum2Change) ->
    bestStddev(Ops, MinSum2Change, fun default_sort_fun/2).

-spec bestStddev(Ops::[lb_op:lb_op()], MinSum2Change::integer() | plus_infinity,
                 SortFun::fun((Op1::{lb_op:lb_op(), integer()},
                               Op2::{lb_op:lb_op(), integer()})
                                -> boolean()))
        -> [lb_op:lb_op()].
bestStddev(Ops, MinSum2Change, SortFun) ->
%%     log:pal("[ ~.0p ] bestStddev(~.0p, ~.0p)~n", [self(), Ops, MinSum2Change]),
    OpsWithSum2Change =
        [{Op, Sum2Change} || Op <- Ops,
                             not lb_op:is_no_op(Op),
                             Sum2Change <- [calc_sum2Change(Op)],
                             MinSum2Change =:= plus_infinity orelse
                                 Sum2Change < MinSum2Change],
%%     log:pal("[ ~.0p ] OpsWithSum2Change: ~.0p~n", [self(), OpsWithSum2Change]),
    OpsWithSum2Change_sort = lists:sort(SortFun, OpsWithSum2Change),
%%     log:pal("[ ~.0p ] OpsWithSum2Change_sort: ~.0p~n", [self(), OpsWithSum2Change_sort]),
    [Op || {Op, _Sum2Change} <- OpsWithSum2Change_sort].

%% @doc Calculates the change of the sum of the square of the nodes' loads
%%      after applying the given operation. 
-spec calc_sum2Change(Op::lb_op:lb_op()) -> integer().
calc_sum2Change(Op) ->
    N1Load = node_details:get(lb_op:get(Op, n1), load),
    N1NewLoad = node_details:get(lb_op:get(Op, n1_new), load),
    N1SuccLoad = node_details:get(lb_op:get(Op, n1succ), load),
    N1SuccNewLoad = node_details:get(lb_op:get(Op, n1succ_new), load),
    Sum2ChangeOp1 = - (N1Load * N1Load) + (N1NewLoad * N1NewLoad)
                    - (N1SuccLoad * N1SuccLoad) + (N1SuccNewLoad * N1SuccNewLoad),
    case lb_op:is_jump(Op) of
        true ->
            N3Load = node_details:get(lb_op:get(Op, n3), load),
            N3NewLoad = node_details:get(lb_op:get(Op, n3_new), load),
            (Sum2ChangeOp1 - (N3Load * N3Load) + (N3NewLoad * N3NewLoad));
        _ -> Sum2ChangeOp1
    end.

%% @doc Returns the given SplitKey and the load that would be split off by
%%      using this key.
-spec split_by_key(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key())
        -> {SplitKey::?RT:key(), TargetLoadNew::non_neg_integer()}.
split_by_key(DhtNodeState, SelectedKey) ->
    MyPredId = dht_node_state:get(DhtNodeState, pred_id),
    DB = dht_node_state:get(DhtNodeState, db),
    Interval = node:mk_interval_between_ids(MyPredId, SelectedKey),
    TargetLoadNew = db_dht:get_load(DB, Interval),
%%     log:pal("[ ~.0p ]  TN: ~.0p, SK: ~.0p~n", [self(), TargetLoadNew, SelectedKey]),
    {SelectedKey, TargetLoadNew}.

%% @doc Returns the SplitKey that splits the current node's address range in
%%      two (almost) equal halves.
-spec split_my_range(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key())
        -> {SplitKey::?RT:key(), TargetLoadNew::non_neg_integer()}.
split_my_range(DhtNodeState, SelectedKey) ->
    MyNodeId = dht_node_state:get(DhtNodeState, node_id),
    MyPredId = dht_node_state:get(DhtNodeState, pred_id),
    % note: MyNodeId cannot be ?PLUS_INFINITY so this split key is valid
    SplitKey = try ?RT:get_split_key(MyPredId, MyNodeId, {1, 2})
               catch throw:not_supported -> SelectedKey
               end,
%%     log:pal("[ ~.0p ] Pred: ~.0p, My: ~.0p,~nSplit: ~.0p, Selected: ~.0p~n",
%%            [self(), MyPredId, MyNodeId, SplitKey, SelectedKey]),
    Interval = node:mk_interval_between_ids(MyPredId, SplitKey),
    DB = dht_node_state:get(DhtNodeState, db),
    TargetLoadNew = db_dht:get_load(DB, Interval),
    {SplitKey, TargetLoadNew}.
