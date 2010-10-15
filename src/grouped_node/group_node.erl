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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    grouped_node main file
%% @end
%% @version $Id$
-module(group_node).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").
-include("group.hrl").

%% a group_node has two different kinds of state: (a)
%% group_state() and (b)local_state(). the former is synchronized between the
%% nodes by the group membership protocol. The latter has to be
%% synchronized by an external mechanism, like e.g. the transaction
%% API for the db, or can be lazily synchronized, like e.g. the
%% successor/predecessor list.

-export([start_link/2]).

-export([on/2, init/1]).

-export([is_alive/1, get_base_interval/0]).

-type(message() :: any()).

%% @doc message handler
-spec on(message(), group_types:state()) -> group_types:state().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% join protocol
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_dht_nodes_response, []}, State) when element(1, State) == joining ->
    State;

on({get_dht_nodes_response, Nodes},
   {joining, NodeState, GroupState, TriggerState}) ->
    Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
    Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
    comm:send(hd(Nodes), {ops, {group_node_join, comm:this(), Acceptor, Learner}}),
    {joining_sent_request, NodeState, GroupState, TriggerState};
on({group_state, NewGroupState, Pred, Succ},
   {Mode, nil, _GroupState, TriggerState}) when Mode == joining_sent_request orelse Mode == joining->
    % @todo: assert that I am a member of this group
    NewVersion = group_state:get_version(NewGroupState),
    fd:subscribe(group_state:get_members(NewGroupState)),
    io:format("joined: ~w ~w~n", [NewVersion, self()]),
    % @todo sync databases
    DB = ?DB:new(group_state:get_group_id(NewGroupState)),
    NewNodeState = group_local_state:new(Pred, Succ, DB),
    {joined, NewNodeState, group_paxos_utils:init_paxos(NewGroupState),
     TriggerState};
on({group_node_join_response, retry, Reason},
   {joining_sent_request, nil, GroupState, TriggerState}) ->
    % retry
    io:format("retry join: ~p ~p ~n", [Reason, self()]),
    comm:send_local_after(500, self(), {known_nodes_timeout}),
    trigger_known_nodes(),
    {joining, nil, GroupState, TriggerState};
on({group_node_join_response,is_already_member},
   {joining_sent_request, nil, GroupState, TriggerState}) ->
    io:format("got is_already_member on joining~n", []),
    {joining_sent_request, nil, GroupState, TriggerState};
on({known_nodes_timeout}, State) when element(1, State) =:= joining ->
    trigger_known_nodes(),
    comm:send_local_after(500, self(), {known_nodes_timeout}),
    State;
on({known_nodes_timeout}, State) when element(1, State) =:= joining_send_request ->
    trigger_known_nodes(),
    comm:send_local_after(500, self(), {known_nodes_timeout}),
    State;

% group protocol (total ordered broadcast)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_node_join
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_node_join, _Pid, _Acceptor, _Learner} = Proposal},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    group_ops_join_node:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_node_remove
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_node_remove, _Pid} = Proposal},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    group_ops_remove_node:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_split
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_split, _Pid, _SplitKey, _LeftGroup, _RightGroup} = Proposal},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    group_ops_split_group:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% deliver
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({learner_decide, _Cookie, PaxosId, Proposal},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    group_tob:deliver(PaxosId, Proposal, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% retry
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({group_split_response,retry},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    % @todo: do nothing?
    State;
on({group_node_join_response,retry},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    % @todo: do nothing? well, we are already in the group.
    State;
on({group_node_remove_response,retry, Pid},
   {joined, _NodeState, GroupState, _TriggerState} = State) ->
    case group_state:is_member(GroupState, Pid) of
        true ->
            comm:send_local(self(), {ops, {group_node_remove, Pid}}),
            State;
        false ->
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rest
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({crash, Pid},
   {joined, _NodeState, GroupState, _TriggerState} = State) ->
    case group_state:is_member(GroupState, Pid) of
        true ->
            comm:send_local(self(), {ops, {group_node_remove, Pid}}),
            State;
        false ->
            State
    end;
on({group_node_join_response, retry, _Reason},
   {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    State;
on({group_state, NewGroupState, Pred, Succ},
   {joined, NodeState, GroupState, TriggerState} = State) ->
    OldVersion = group_state:get_version(GroupState),
    NewVersion = group_state:get_version(NewGroupState),
    % @todo has somebody kicked my out?
    % @todo cannot compare versions like this !!!
    if
        NewVersion == OldVersion + 1 ->
            io:format("accepted newer group_state at ~p~n", [self()]),
            {joined, group_local_state:update_pred_succ(NodeState, Pred, Succ),
             group_paxos_utils:init_paxos(NewGroupState), TriggerState};
        NewVersion =< OldVersion ->
            State;
        NewVersion > OldVersion ->
            %@todo
            io:format("panic! 7~n", []),
            State
    end;
on({succ_update, Succ},
   {joined, NodeState, GroupState, TriggerState} = _State) ->
    {joined, group_local_state:update_succ(NodeState, Succ), GroupState,
     TriggerState};
on({pred_update, Pred},
   {joined, NodeState, GroupState, TriggerState} = _State) ->
    {joined, group_local_state:update_pred(NodeState, Pred), GroupState,
     TriggerState};

on({trigger}, {joined, _NodeState, _GroupState, _TriggerState} = State) ->
    group_node_trigger:trigger(State);
on({trigger}, {Mode, NodeState, GroupState, TriggerState} = _State) ->
    {Mode, NodeState, GroupState, trigger:next(TriggerState)};


% normal protocol
on({get_node_details, Pid, Which},
   {_Mode, NodeState, _GroupState, _TriggerState} = State) ->
    % @todo
    comm:send(Pid, {get_node_details_response, dht_node_state:details(NodeState)}),
    State;
on({known_nodes_timeout}, State) ->
    State;
on({get_dht_nodes_response, _Nodes}, State) ->
    % too late !?!
    State;

on({group_split_response,success}, State) ->
    State.

%% @doc joins this node in the ring and calls the main loop
-spec init(list()) -> {joining, nil, nil, trigger:state()} |
                          {joined, nil, group_state:group_state(),
                           trigger:state()}.
init(Options) ->
    % first node in this vm and also vm is marked as first
    % or unit-test
    io:format("group_node~n", []),
    Trigger = config:read(group_node_trigger),
    case dht_node:is_first(Options) of
        true ->
            io:format("first~n", []),
            trigger_known_nodes(),
            Interval = intervals:new('[', 0, 16#100000000000000000000000000000000, ')'),
            GroupState = group_state:new_group_state(Interval),
            We = group_state:get_group_node(GroupState),
            DB = ?DB:new(group_state:get_group_id(GroupState)),
            TriggerState = trigger:now(trigger:init(Trigger, ?MODULE)),
            {joined, group_local_state:new(We, We, DB),
             group_paxos_utils:init_paxos(GroupState), TriggerState};
        _ ->
            trigger_known_nodes(),
            io:format("joining~n", []),
            TriggerState = trigger:now(trigger:init(Trigger, ?MODULE)),
            comm:send_local_after(500, self(), {known_nodes_timeout}),
            {joining, nil, nil, TriggerState}
    end.

-spec start_link(pid_groups:groupname(), list()) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, Options,
                             [{pid_groups_join_as, DHTNodeGroup, group_node},
                              wait_for_init]).

% @doc find existing nodes and initialize the comm_layer
-spec trigger_known_nodes() -> ok.
trigger_known_nodes() ->
    KnownHosts = config:read(known_hosts),
    % note, comm:this() may be invalid at this moment
    [comm:send(KnownHost, {get_dht_nodes, comm:this()})
     || KnownHost <- KnownHosts],
    comm:send_local(pid_groups:find_a(service_per_vm),
                    {get_dht_nodes, comm:this()}),
    timer:sleep(100),
    case comm:is_valid(comm:this()) of
        true ->
            ok;
        false ->
            trigger_known_nodes()
    end.

-spec is_alive(pid()) -> boolean().
is_alive(Pid) ->
    element(1, gen_component:get_state(Pid)) =:= joined.

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(group_node_base_interval).
% @todo:
% - periodically check learners and acceptors (update group_state)
% - cleanup old acceptors and learners
% - group_remove dead members
% - add read and write operations
% - read is not really fail-safe, what happens if the proposer dies?
% - on join: check whether it is already a member
% - when my proposal failed, then do what?
% - DO NOT propose in future paxos instances!!!
% - who should answer read/write requests from users?
% - check if my proposal was proposed (and accepted) by somebody else
% - check if the proposal was for this group instance
% - add pred and succ list for routing
% - synchronize db, especially on join/leave/split
% - ?DB:new() what is the/my Id?
% - key-space is hard-coded to 0-2^128-1
% - specify type for group_state messages, it should be a well-defined subset of group_state:state().

% facts:
% - when we get the decision for PaxosId, we cancel the learner  for PaxosId - 2
% - when we get the decision for PaxosId, we cancel the proposer for PaxosId - 2
