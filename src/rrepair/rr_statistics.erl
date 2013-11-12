% @copyright 2011, 2012 Zuse Institute Berlin

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
%% @doc    replica repair statistics module
%%         count outdated replicas
%% @end
%% @version $Id$
-module(rr_statistics).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([requests/0]).
-endif.


-type requests() ::
          {count_old_replicas, Requestor::comm:mypid(), Interval::intervals:interval()}.

-record(state,
        {
            dhtNodePid = ?required(state, dhtNodePid) :: comm:erl_local_pid(),
            operation  = {}                           :: requests() | {}
        }).
-type state() :: #state{}.

-type message() ::
    {get_state_response, intervals:interval()} |
    {get_chunk_response, {intervals:interval(), [any()]}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state() | kill.
on({count_old_replicas, _, _} = Op, State) ->
    comm:send_local(State#state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#state{ operation = Op };

on({get_state_response, MyI},
   State = #state{operation = {count_old_replicas, Req, ReqI},
                  dhtNodePid = DhtPid}) ->
    I = intervals:intersection(MyI, ReqI),
    case intervals:is_empty(I) of
        true ->
            comm:send(Req, {count_old_replicas_reply, 0}),
            kill;
        _ ->
            comm:send_local(
              DhtPid,
              {get_chunk, self(), I,
               fun(Item) -> db_entry:get_version(Item) =/= -1 end,
               fun(Item) -> {db_entry:get_key(Item), db_entry:get_version(Item)} end,
               all})
    end,
    State;

on({get_chunk_response, {_, DBList}},
   #state{operation = {count_old_replicas, Req, _}}) ->
    This = comm:this(),
    Outdated =
        lists:foldl(
          fun({Key, Ver}, Acc) ->
                  _ = [api_dht_raw:unreliable_lookup(K, {?read_op, This, 0, K, ?write})
                         || K <- ?RT:get_replica_keys(Key), K =/= Key],
                  % note: receive wrapped in anonymous functions to allow
                  %       ?SCALARIS_RECV in multiple receive statements
                  V1 = fun() -> receive ?SCALARIS_RECV({?read_op_with_id_reply, 0, _, ?ok, ?value_dropped, V}, V) end end(),
                  V2 = fun() -> receive ?SCALARIS_RECV({?read_op_with_id_reply, 0, _, ?ok, ?value_dropped, V}, V) end end(),
                  V3 = fun() -> receive ?SCALARIS_RECV({?read_op_with_id_reply, 0, _, ?ok, ?value_dropped, V}, V) end end(),
                  case Ver =:= lists:max([V1, V2, V3]) of
                      true -> Acc;
                      false -> Acc + 1
                  end
          end, 0, DBList),
    comm:send(Req, {count_old_replicas_reply, Outdated}),
    kill.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    State.

-spec start() -> {ok, pid()}.
start() ->
    State = #state{ dhtNodePid = pid_groups:get_my(dht_node) },
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).
