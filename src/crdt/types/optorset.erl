%  @copyright 2008-2018 Zuse Institute Berlin

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc Implementation of an optimized OR-Set state-based CRDT
%% as specified by https://arxiv.org/pdf/1210.3368.pdf
%% @end
%% @version $Id$

-module(optorset).
-author('skrzypczak@zib.de').
-vsn('Id$').

-include("scalaris.hrl").
-define(R, config:read(replication_factor)).
-define(SET, ordsets).

-export([lteq2/2]).

-export([update_add/3]).
-export([update_remove/2]).

-export([query_contains/2]).
-export([query_elements/1]).

-behaviour(crdt_beh).

-type vector() :: [non_neg_integer()].
-type element() :: {Element::term(), TimeStamp::term(), ReplicaID::term()}.
-opaque crdt() :: {E::?SET:ordset(element()), V::vector()}.

-include("crdt_beh.hrl").

-spec new() -> crdt().
new() -> {?SET:new(), [0 || _ <- lists:seq(1, ?R)]}.

-spec merge(crdt(), crdt()) -> crdt().
merge(_CRDT1={E1, V1}, _CRDT2={E2, V2}) ->
    %% TODO: make this more efficient?
    M = ?SET:union(E1, E2),
    T2 = ?SET:subtract(E1, E2),
    M2 = ?SET:filter(fun({_E, C, I}) -> C > vector_get(I, V2) end, T2),
    T3 = ?SET:subtract(E2, E1),
    M3 = ?SET:filter(fun({_E, C, I}) -> C > vector_get(I, V1) end, T3),
    U = ?SET:union([M, M2, M3]),

    % find the largest C of all {E, _, I} tripples in linear time
    D = ?SET:fold(fun({E, C, I}, DictAcc) ->
                        dict:update({E,I}, fun(A) -> max(A, C) end, C, DictAcc)
                   end, dict:new(), U),
    % from dict to set again...
    NewE =  dict:fold(fun({E, I}, C, SetAcc) ->
                        ?SET:add_element({E, C, I}, SetAcc)
                      end, ?SET:new(), D),
    NewV = vector_max(V1, V2),

    {NewE, NewV}.

-spec eq(crdt(), crdt()) -> boolean().
eq(_CRDT1={E1, V1}, _CRDT2={E2, V2}) ->
    vector_eq(V1, V2) andalso ?SET:is_subset(E1, E2) andalso ?SET:is_subset(E2, E1).

-spec lteq(crdt(), crdt()) -> boolean().
lteq(_CRDT1={E1, V1}, _CRDT2={E2, V2}) ->
    case vector_lteq(V1, V2) of
        false -> false;
        true ->
            %% optimized implementation compared to paper exploiting the fact
            %% that V1 =< V2
            Fun = fun({_, C, I}, SetAcc) ->
                        case C =< vector_get(I, V1) of
                            true -> ?SET:add_element({C, I}, SetAcc);
                            false -> SetAcc
                        end
                  end,

            X = ?SET:fold(Fun, ?SET:new(), E1),
            X2 = ?SET:fold(Fun, ?SET:new(), E2),

            %% Is the same as is_subset(D - X,D - X2) with
            %% D = set of all possible (C,I) pairs
            ?SET:is_subset(X2, X)
    end.

%% lteq implementation as specified in paper... used for testing purposes
-spec lteq2(crdt(), crdt()) -> boolean().
lteq2(_CRDT1={E1, V1}, _CRDT2={E2, V2}) ->
    case vector_lteq(V1, V2) of
        false -> false;
        true ->
            TFun = fun({_, C, I}, SetAcc) -> ?SET:add_element({C,I}, SetAcc) end,
            X1 = ?SET:fold(TFun, ?SET:new(), E1),
            X2 = ?SET:fold(TFun, ?SET:new(), E2),

            Possible1 = [{B, A} || A <- lists:seq(1, ?R), B <- lists:seq(1, vector_get(A, V1))],
            Possible2 = [{B, A} || A <- lists:seq(1, ?R), B <- lists:seq(1, vector_get(A, V2))],

            R1 = lists:foldl(fun(E, Acc) ->
                                     case ?SET:is_element(E, X1) of
                                         true -> Acc;
                                         false -> ?SET:add_element(E, Acc)
                                     end
                             end, ?SET:new(), Possible1),

            R2 = lists:foldl(fun(E, Acc) ->
                                     case ?SET:is_element(E, X2) of
                                         true -> Acc;
                                         false -> ?SET:add_element(E, Acc)
                                     end
                             end, ?SET:new(), Possible2),

            ?SET:is_subset(R1, R2)
    end.

%%%%%%%%%%%%%%%%%%%%% Update and Query functions
-spec update_add(ReplicaId::non_neg_integer(), Element::term(), CRDT::crdt()) -> crdt().
update_add(ReplicaId, Element, _CRDT={E, V}) ->
    {TimeStamp, V2} = vector_inc_and_get(ReplicaId, V),
    E2 = ?SET:add_element({Element, TimeStamp, ReplicaId}, E),
    E3 = ?SET:filter(fun({TE, TC, _TI}) ->
                            not (TE =:= Element andalso TC < TimeStamp)
                     end, E2),
    {E3, V2}.

-spec update_remove(Element::term(), C::crdt()) -> crdt().
update_remove(Element, _CRDT={E, V}) ->
    E2 = ?SET:filter(fun({TE, _TC, _TI}) -> Element =/= TE end, E),
    {E2, V}.

-spec query_elements(crdt()) -> ?SET:ordset(element()).
query_elements(_CRDT={Elements, _V}) ->
    ?SET:fold(fun({E, _C, _I}, AccSet) -> ?SET:add_element(E, AccSet) end,
              ?SET:new(), Elements).

-spec query_contains(term(), crdt()) -> boolean().
query_contains(Element, _CRDT={Elements, _V}) ->
    ?SET:fold(fun({E, _C, _I}, _Acc) when Element =:= E -> true;
                 (_C, Acc) -> Acc
              end, false, Elements).

%%%%%%%%%%%%%%%%%%%%% Some helper funcitons for vector manipulation
%% TODO use dict instead of list?
-spec vector_max(vector(), vector()) -> vector().
vector_max([], []) -> [];
vector_max([H1 | T1], [H2 | T2]) -> [max(H1, H2) | vector_max(T1, T2)].

-spec vector_get(non_neg_integer(), vector()) -> non_neg_integer().
vector_get(Idx, V) -> lists:nth(Idx, V).

-spec vector_inc_and_get(non_neg_integer(), vector()) -> {non_neg_integer(), vector()}.
vector_inc_and_get(Idx, V) ->
    TimeStamp = lists:nth(Idx, V) + 1,
    V2 = lists:sublist(V, Idx - 1) ++ [TimeStamp] ++ lists:nthtail(Idx, V),
    {TimeStamp, V2}.

-spec vector_eq(vector(), vector()) -> boolean().
vector_eq(V1, V2) -> V1 =:= V2.

-spec vector_lteq(vector(), vector()) -> boolean().
vector_lteq([], []) -> true;
vector_lteq([H1 | _T1], [H2 | _T2]) when H1 > H2 -> false;
vector_lteq([_H1 | T1], [_H2 | T2]) -> vector_lteq(T1, T2).



