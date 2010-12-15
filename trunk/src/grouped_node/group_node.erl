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

%% a group_node has two different kinds of state: (a) synchronized
%% state and (b) lazily synchronized state. changes to the former
%% state require a consensus among the group participants, read
%% paxos. changes to the latter can be performed locally (see
%% group_local_state), e.g. the routing table.

-export([start_link/2]).

-export([on/2, on_joining/2, init/1]).

-export([is_alive/1, get_base_interval/0]).

-type(message() :: any()).

%% @doc message handler for join protocol
-spec on_joining(message(), group_state:state()) -> group_state:state().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% join protocol
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on_joining({get_dht_nodes_response, []}, State) ->
    State;
on_joining({get_dht_nodes_response, Nodes}, State) ->
    Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
    Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
    comm:send(hd(Nodes), {ops, {group_node_join, comm:this(), Acceptor, Learner}}),
    group_state:set_mode(State, joining_sent_request);
on_joining({group_state, View, Pred, Succ}, State) ->
    % @todo: assert that I am a member of this group
    NewView = group_view:recalculate_index(View),
    NewVersion = group_view:get_version(NewView),
    Members = group_view:get_members(NewView),
    fd:subscribe(Members),
    io:format("joined: ~w ~w~n", [NewVersion, self()]),
    % @todo sync databases
    Interval = group_view:get_interval(View),
    UUID = util:get_global_uid(),
    RepairJob = {UUID, Interval, Members, group_view:get_version(View)},
    DB = group_db:start_repair_job(group_db:new_replica(), RepairJob),
    NodeState = group_local_state:new(Pred, Succ),
    NewView2 = group_paxos_utils:init_paxos(NewView),
    NewState = group_state:set_db(group_state:set_mode(
                                    group_state:set_node_state(
                                      group_state:set_view(
                                        State,
                                        NewView2),
                                      NodeState),
                                    joined),
                                  DB),
    gen_component:change_handler(NewState, on);
on_joining({group_node_join_response, retry, Reason}, State) ->
    % retry
    io:format("retry join: ~p ~p ~n", [Reason, self()]),
    comm:send_local_after(500, self(), {known_nodes_timeout}),
    trigger_known_nodes(),
    group_state:set_mode(State, joining);
on_joining({group_node_join_response,is_already_member}, State) ->
    io:format("got is_already_member on joining~n", []),
    State;
on_joining({known_nodes_timeout}, State) ->
    trigger_known_nodes(),
    comm:send_local_after(500, self(), {known_nodes_timeout}),
    State;
on_joining({trigger}, State) ->
    group_node_trigger:trigger(State);
on_joining({route, _, _, _}, State) ->
    State;
on_joining({pred_update, _Succ}, State) ->
    State;
on_joining({succ_update, _Succ}, State) ->
    State.

-spec on(message(), group_state:state()) -> group_state:state().

% group protocol (total ordered broadcast)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_node_join
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_node_join, _Pid, _Acceptor, _Learner} = Proposal}, State) ->
    group_ops_join_node:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_node_remove
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_node_remove, _DeadPid, _Proposer} = Proposal}, State) ->
    group_ops_remove_node:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% group_split
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {group_split, _Pid, _SplitKey, _LeftGroup, _RightGroup} = Proposal},
   State) ->
    group_ops_split_group:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% paxos_read
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ops, {read, _HashedKey, _Value, _Version, _Client, _Proposer} = Proposal},
   State) ->
    group_ops_db:ops_request(State, Proposal);

on({ops, {write, _HashedKey, _Value, _Version, _Client, _Proposer} = Proposal},
   State) ->
    group_ops_db:ops_request(State, Proposal);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% deliver
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({learner_decide, _Cookie, PaxosId, Proposal},
   State) ->
    group_tob:deliver(PaxosId, Proposal, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% retry
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({group_split_response,retry_outdated_member_list}, State) ->
    % @todo: do nothing?
    State;
on({group_split_response,success}, State) ->
    % @todo: do nothing?
    State;
on({group_split_response,member_list_has_changed}, State) ->
    % @todo: do nothing?
    State;
on({group_split_response,retry}, State) ->
    % @todo: do nothing?
    State;
on({group_node_join_response,retry}, State) ->
    % @todo: do nothing? well, we are already in the group.
    State;
on({group_node_join_response,is_already_member}, State) ->
    % @todo: do nothing? well, we are already in the group.
    State;
on({group_node_remove_response, is_no_member, _DeadPid}, State) ->
    State;
on({group_node_remove_response, retry, DeadPid}, State) ->
    View = group_state:get_view(State),
    case group_view:is_member(View, DeadPid) of
        true ->
            comm:send_local(self(), {ops, {group_node_remove, DeadPid, comm:this()}}),
            State;
        false ->
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% routing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({route, Key, Hops, Message}, State) ->
    group_router:route(Key, Hops, Message, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% simple DB
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({quorum_read, Client, HashedKey}, State) ->
    % @todo
   State;
on({paxos_read, Client, HashedKey}, State) ->
    DB = group_state:get_db(State),
    case group_db:read(DB, HashedKey) of
        {value, {ok, Value, Version}} ->
            Proposal = {read, HashedKey, Value, Version, Client, comm:this()},
            on({ops, Proposal}, State);
        is_not_current ->
            comm:send(Client, {paxos_read_response, retry, db_is_not_current}),
            State
    end;
on({paxos_write, Client, HashedKey, Value}, State) ->
    DB = group_state:get_db(State),
    case group_db:read(DB, HashedKey) of
        {value, {ok, _OldValue, OldVersion}} ->
            Proposal = {write, HashedKey, Value, OldVersion + 1, Client, comm:this()},
            on({ops, Proposal}, State);
        is_not_current ->
            comm:send(Client, {paxos_write_response, retry, db_is_not_current}),
            State
    end;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% DB repair
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({db_repair_request, Range, ChunkSize, _Version, UUID, Client}, State) ->
    DB = group_state:get_db(State),
    % @todo check version
    case group_db:get_chunk(DB, Range, ChunkSize) of
        is_not_current ->
            comm:send(Client, {db_repair_response, is_not_current, Range, Range, [],
                               UUID, comm:this()});
        {Last, Chunk} ->
            comm:send(Client, {db_repair_response, ok, Range, Last, Chunk,
                               UUID, comm:this()})
    end,
    State;
on({db_repair_response, Error, GivenInterval, RemainingInterval, Chunk, UUID, Sender}, State) ->
    group_db:repair(Error, GivenInterval, RemainingInterval, Chunk, Sender, UUID,
                    State);
on({group_repair, timeout, UUID, Start}, State) ->
    group_db:repair_timeout(State, UUID, Start);

on({db_delete_chunked, Range, ChunkSize}, State) ->
    DB = group_state:get_db(State),
    DB2 = group_db:delete_chunk(DB, Range, ChunkSize),
    group_state:set_db(State, DB2);
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rm_get_succ, Node, NodesPred}, State) ->
    group_rm:handle_get_succ(State, Node, NodesPred);

on({rm_get_succ_response, Node, NodesSucc}, State) ->
    group_rm:handle_get_succ_response(State, Node, NodesSucc);

on({rm_get_pred, Node, NodesSucc}, State) ->
    group_rm:handle_get_pred(State, Node, NodesSucc);

on({rm_get_pred_response, Node, NodesPred}, State) ->
    group_rm:handle_get_pred_response(State, Node, NodesPred);
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rest
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({crash, DeadPid}, State) ->
    View = group_state:get_view(State),
    case group_view:is_member(View, DeadPid) of
        true ->
            comm:send_local(self(), {ops, {group_node_remove, DeadPid, comm:this()}}),
            State;
        false ->
            State
    end;
on({group_node_join_response, retry, _Reason}, State) ->
    State;
on({group_state, _NewGroupState, _Pred, _Succ}, State) ->
    % @todo ignore for the moment
    State;
on({get_pred_succ, Pid}, State) ->
    NodeState = group_state:get_node_state(State),
    Pred = group_local_state:get_predecessor(NodeState),
    Succ = group_local_state:get_successor(NodeState),
    Interval = group_view:get_interval(group_state:get_view(State)),
    comm:send(Pid, {get_pred_succ_response, Pred, Succ, Interval}),
    State;
on({succ_update, Succ}, State) ->
    NodeState = group_state:get_node_state(State),
    Range = group_view:get_interval(group_state:get_view(State)),
    NewNodeState = group_local_state:update_succ(NodeState, Succ, Range),
    group_state:set_node_state(State, NewNodeState);
on({pred_update, Pred}, State) ->
    NodeState = group_state:get_node_state(State),
    Range = group_view:get_interval(group_state:get_view(State)),
    NewNodeState = group_local_state:update_pred(NodeState, Pred, Range),
    group_state:set_node_state(State, NewNodeState);
on({trigger}, State) ->
    group_node_trigger:trigger(State);


% normal protocol
on({known_nodes_timeout}, State) ->
    State;
on({get_dht_nodes_response, _Nodes}, State) ->
    % too late !?!
    State.


%% @doc joins this node in the ring and calls the main loop
-spec init(list()) -> group_state:state() | {'$gen_component', list(), group_state:state()}.
init(Options) ->
    io:format("group_node~n", []),
    {my_sup_dht_node_id, MySupDhtNode} = lists:keyfind(my_sup_dht_node_id, 1, Options),
    erlang:put(my_sup_dht_node_id, MySupDhtNode),
    % first node in this vm and also vm is marked as first
    % or unit-test
    Trigger = config:read(group_node_trigger),
    case dht_node:is_first(Options) of
        true ->
            io:format("first~n", []),
            trigger_known_nodes(),
            Interval = group_types:all(),
            View = group_paxos_utils:init_paxos(group_view:new(Interval)),
            We = group_view:get_group_node(View),
            TriggerState = trigger:now(trigger:init(Trigger, ?MODULE)),
            DB = group_db:new_empty(),
            NodeState = group_local_state:new(We, We),
            % starts with on-handler
            group_state:new_primary(NodeState, View, DB, TriggerState);
        _ ->
            trigger_known_nodes(),
            io:format("joining~n", []),
            TriggerState = trigger:now(trigger:init(Trigger, ?MODULE)),
            comm:send_local_after(500, self(), {known_nodes_timeout}),
            % starts with on-joining-handler
            gen_component:change_handler(group_state:new_replica(TriggerState), on_joining)
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
    group_state:get_mode(gen_component:get_state(Pid)) =:= joined.

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
% - key-space is hard-coded to 0-2^128-1
% - specify type for group_state messages, it should be a well-defined subset of group_state:state().

% facts:
% - when we get the decision for PaxosId, we cancel the learner  for PaxosId - 2
% - when we get the decision for PaxosId, we cancel the proposer for PaxosId - 2
