%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    routing table process
%%% @end
%% @version $Id$
-module(rt_loop).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

% for routing table implementation
-export([start_link/1]).
-export([init/1, on/2, get_base_interval/0, check_config/0]).

% state of the routing table loop
-type(state_init() :: {Id           :: ?RT:key(),
                  Pred         :: node:node_type(),
                  Succ         :: node:node_type(),
                  RTState      :: ?RT:rt(),
                  TriggerState :: trigger:state()}).
-type(state_uninit() :: {uninit, TriggerState :: trigger:state()}).
-type(state() :: state_init() | state_uninit()).

% accepted messages of rt_loop processes
-type(message() ::
      {init, Id::?RT:key(), Pred::node:node_type(), Succ::node:node_type()}
     | {stabilize}
     | {{get_node_details, NewNodeDetails::node_details:node_details()}, pred_succ}
     | {crash, DeadPid::comm:mypid()}
     | ?RT:custom_message()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the routing tabe maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(routingtable_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, routing_table}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> {uninit, trigger:state()}.
init(Trigger) ->
    log:log(info,"[ RT ~p ] starting routingtable", [comm:this()]),
    TriggerState = trigger:init(Trigger, ?MODULE),
    {uninit, TriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Private Code
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc message handler
-spec on(message(), state()) -> state() | unknown_event.

on({init, Id, Pred, Succ}, {uninit, TriggerState}) ->
    TriggerState2 = trigger:next(TriggerState),
    {Id, Pred, Succ, ?RT:empty(Succ), TriggerState2};

on(Message, {uninit, TriggerState}) ->
    comm:send_local(self() , Message),
    {uninit, TriggerState};

% re-initialize routing table
on({init, Id2, NewPred, NewSucc}, {_, _, _, RTState,TriggerState}) ->
    ?RT:check(RTState, ?RT:empty(NewSucc), Id2, NewPred, NewSucc),
    {Id2, NewPred, NewSucc, ?RT:empty(NewSucc),TriggerState};

% start new periodic stabilization
on({trigger}, {Id, Pred, Succ, RTState, TriggerState}) ->
    %io:format("[ RT ] stabilize~n"),
    Pid = process_dictionary:get_group_member(dht_node),
    % get new pred and succ from dht_node
    comm:send_local(Pid , {get_node_details, comm:this_with_cookie(pred_succ), [pred, succ]}),
    % start periodic stabilization
    NewRTState = ?RT:init_stabilize(Id, Succ, RTState),
    ?RT:check(RTState, NewRTState, Id, Pred, Succ),
    % trigger next stabilization
    NewTriggerState = trigger:next(TriggerState),
    {Id, Pred, Succ, NewRTState, NewTriggerState};

% got new predecessor/successor
on({{get_node_details_response, NewNodeDetails}, pred_succ}, {Id, _, _, RTState, TriggerState}) ->
    NewPred = node_details:get(NewNodeDetails, pred),
    NewSucc = node_details:get(NewNodeDetails, succ),
    {Id, NewPred, NewSucc, RTState, TriggerState};

% failure detector reported dead node
on({crash, DeadPid}, {Id, Pred, Succ, RTState, TriggerState}) ->
    NewRT = ?RT:filterDeadNode(RTState, DeadPid),
    ?RT:check(RTState, NewRT, Id, Pred, Succ, false),
    {Id, Pred, Succ, NewRT, TriggerState};

% debug_info for web interface
on({'$gen_cast', {debug_info, Requestor}}, {_Id, _Pred, _Succ, RTState, _TriggerState} = State) ->
    KeyValueList =
        [{"rt_size", ?RT:get_size(RTState)},
         {"rt (index, node):", ""} | ?RT:dump(RTState)],
    comm:send_local(Requestor, {debug_info_response, KeyValueList}),
    State;

on({dump, Pid}, {_Id, _Pred, _Succ, RTState, _TriggerState} = State) ->
    comm:send_local(Pid, {dump_response, RTState}),
    State;

% unknown message
on(Message, State) ->
    ?RT:handle_custom_message(Message, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_base_interval() ->
    config:read(pointer_base_stabilization_interval).

%% @doc Checks whether config parameters of the rt_loop process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(pointer_base_stabilization_interval) and
        config:is_greater_than_equal(pointer_base_stabilization_interval, 1000).
