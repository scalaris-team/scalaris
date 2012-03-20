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
%% @version $Id:  $
-module(rr_statistics).

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
    {get_chunk_response, {intervals:interval(), [any()]}} |
    {shutdown}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({count_old_replicas, _, _} = Op, State) ->
    comm:send_local(State#state.dhtNodePid, {get_state, comm:this(), my_range}),
    State#state{ operation = Op };

on({get_state_response, MyI}, State = #state{ operation = Op,
                                              dhtNodePid = DhtPid }) ->
    case Op of
        {count_old_replicas, Req, ReqI} ->
            I = intervals:intersection(MyI, ReqI),
            case intervals:is_empty(I) of
                true -> 
                    comm:send(Req, {count_old_replicas_reply, 0}),
                    comm:send_local(self(), {shutdown});
                _ ->
                    comm:send_local(DhtPid, 
                                    {get_chunk, self(), I, 
                                     fun(Item) -> db_entry:get_version(Item) =/= -1 end,
                                     fun(Item) -> {db_entry:get_key(Item), db_entry:get_version(Item)} end,
                                     all})                    
            end
    end,    
    State;

on({get_chunk_response, {_, DBList}}, 
   State = #state{ operation = {count_old_replicas, Req, _} }) ->
    Outdated = lists:foldl(
                 fun({Key, Ver}, Acc) -> 
                         _ = [api_dht_raw:unreliable_lookup(K, {get_key_entry, comm:this(), K}) 
                                || K <- ?RT:get_replica_keys(Key), K =/= Key],
                         V1 = db_entry:get_version(receive {get_key_entry_reply, E1} -> E1 end),
                         V2 = db_entry:get_version(receive {get_key_entry_reply, E2} -> E2 end),
                         V3 = db_entry:get_version(receive {get_key_entry_reply, E3} -> E3 end),
                         case Ver < lists:max([V1, V2, V3]) of
                             true -> Acc + 1;
                             false -> Acc
                         end
                 end, 
                 0, DBList),
    comm:send(Req, {count_old_replicas_reply, Outdated}),
    comm:send_local(self(), {shutdown}),
    State;

on({shutdown}, _) ->
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
