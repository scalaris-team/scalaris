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
%%% @doc    dht_node join procedure
%%% @end
%% @version $Id$
-module(dht_node_join).
-author('schuett@zib.de').
-vsn('$Id$').

-export([join_request/2, process_join_msg/2]).

-ifdef(with_export_type_support).
-export_type([join_state/0]).
-endif.

-include("scalaris.hrl").

-type(join_message() ::
    {idholder_get_id_response, Id::?RT:key(), IdVersion::non_neg_integer()} |
    {known_hosts_timeout} |
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {lookup_timeout} |
    {get_node_response, Key::?RT:key(), Succ::node:node_type()} |
    {join_response, Pred::node:node_type(), Data::any()}).

-type(join_state() ::
    {join, {as_first}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase1}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase2, MyKey::?RT:key()}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase3, Nodes::[node:node_type()], MyKey::?RT:key()}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase4, MyKey::?RT:key(), Succ::node:node_type(), Me::node:node_type()}, QueuedMessages::msg_queue:msg_queue()}).

%% @doc handle the join request of a new node
%% userdevguide-begin dht_node_join:join_request
-spec join_request(dht_node_state:state(), NewPred::node:node_type()) ->
        dht_node_state:state().
join_request(State, NewPred) ->
    MyNewInterval =
        intervals:mk_from_nodes(NewPred, dht_node_state:get(State, node)),
    {DB, HisData} = ?DB:split_data(dht_node_state:get(State, db), MyNewInterval),
    
    %%TODO: split data [{Key, Value, Version}], schedule transfer
    
    comm:send(node:pidX(NewPred),
              {join_response, dht_node_state:get(State, pred), HisData}),
    % TODO: better already update our range here directly than waiting for an
    % updated state from the ring_maintenance?
    rm_beh:notify_new_pred(comm:this(), NewPred),
    dht_node_state:set_db(State, DB).
%% userdevguide-end dht_node_join:join_request



% join protocol
%% @doc Processes a DHT node's join messages (the node joining a Scalaris DHT).
-spec process_join_msg(join_message() | any(), join_state()) -> dht_node_state:state().
% first node
%% userdevguide-begin dht_node_join:join_first
process_join_msg({idholder_get_id_response, Id, IdVersion},
                 {join, {as_first}, QueuedMessages}) ->
    log:log(info,"[ Node ~w ] joining as first: ~p",[self(), Id]),
    Me = node:new(comm:this(), Id, IdVersion),
    rt_beh:initialize(Id, Me, Me),
    NewState = dht_node_state:new(?RT:empty_ext(Me),
                                  nodelist:new_neighborhood(Me),
                                  ?DB:new(Id)),
    comm:send_local(get_local_dht_node_reregister_pid(), {go}),
    msg_queue:send(QueuedMessages),
    %log:log(info,"[ Node ~w ] joined",[self()]),
    NewState;  % join complete, State is the first "State"
%% userdevguide-end dht_node_join:join_first

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% !first node
%% userdevguide-begin dht_node_join:join_other_p12
% 1. get my key
process_join_msg({idholder_get_id_response, Id, IdVersion},
                 {join, {phase1}, QueuedMessages}) ->
    %io:format("p1: got key~n"),
    log:log(info,"[ Node ~w ] joining",[self()]),
    % send message to avoid code duplication
    comm:send_local(self(), {known_hosts_timeout}),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

% 2. Find known hosts
process_join_msg({known_hosts_timeout},
                 {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: known hosts timeout~n"),
    KnownHosts = config:read(known_hosts),
    % contact all known VMs
    _Res = [comm:send(KnownHost, {get_dht_nodes, comm:this()})
           || KnownHost <- KnownHosts],
    %io:format("~p~n", [_Res]),
    % timeout just in case
    comm:send_local_after(1000, self(), {known_hosts_timeout}),
    State;

process_join_msg({get_dht_nodes_response, []},
                 {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: got empty dht_nodes_response~n"),
    % there is a VM with no nodes
    State;

process_join_msg({get_dht_nodes_response, Nodes = [_|_]},
                 {join, {phase2, Id, IdVersion}, QueuedMessages} = State) ->
    %io:format("p2: got dht_nodes_response ~p~n", [lists:delete(comm:this(), Nodes)]),
    case lists:delete(comm:this(), Nodes) of
        [] ->
            State;
        [First | Rest] ->
            comm:send(First, {lookup_aux, Id, 0, {get_node, comm:this(), Id}}),
            comm:send_local_after(3000, self(), {lookup_timeout}),
            {join, {phase3, Rest, Id, IdVersion}, QueuedMessages}
    end;
%% userdevguide-end dht_node_join:join_other_p12

%% userdevguide-begin dht_node_join:join_other_p3
% 3. lookup my position
process_join_msg({lookup_timeout},
                 {join, {phase3, [], Id, IdVersion}, QueuedMessages}) ->
    %io:format("p3: lookup_timeout~n"),
    % no more nodes left, go back to step 2
    comm:send_local(self(), {known_hosts_timeout}),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

process_join_msg({get_node_response, Id, Succ},
                 {join, {phase3, _DHTNodes, Id, IdVersion}, QueuedMessages}) ->
    %io:format("p3: lookup success~n"),
    % got my successor
    Me = node:new(comm:this(), Id, IdVersion),
    % announce join request
    comm:send(node:pidX(Succ), {join, Me}),
    {join, {phase4, Succ, Me}, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p3

%% userdevguide-begin dht_node_join:join_other_p4
% 4. joining my neighbors
process_join_msg({join_response, Pred, Data},
                 {join, {phase4, Succ, Me}, QueuedMessages}) ->
    Id = node:id(Me),
    %io:format("p4: join_response~n"),
    % @TODO data shouldn't be moved here, might be large
    log:log(info, "[ Node ~w ] got pred ~w",[self(), Pred]),
    DB = ?DB:add_data(?DB:new(Id), Data),
    rt_beh:initialize(Id, Pred, Succ),
    State = 
        case node:is_valid(Pred) of
            true ->
                rm_beh:notify_new_succ(node:pidX(Pred), Me),
                dht_node_state:new(?RT:empty_ext(Succ),
                                   nodelist:new_neighborhood(Pred, Me, Succ),
                                   DB);
            false ->
                dht_node_state:new(?RT:empty_ext(Succ),
                                   nodelist:new_neighborhood(Me, Succ),
                                   DB)
        end,
    cs_replica_stabilization:recreate_replicas(dht_node_state:get(State, my_range)),
    comm:send_local(get_local_dht_node_reregister_pid(), {go}),
    msg_queue:send(QueuedMessages),
    State;
%% userdevguide-end dht_node_join:join_other_p4

% ignore some messages that appear too late for them to be used, e.g. if a new
% phase has already been started

process_join_msg({known_hosts_timeout}, State) ->
    State;

process_join_msg({get_dht_nodes_response, _KnownHosts}, State) ->
    State;

process_join_msg({lookup_timeout}, State) ->
    State;

% Catch all other messages until the join procedure is complete
process_join_msg(Msg, {join, Details, QueuedMessages}) ->
    %log:log(info("[dhtnode] [~p] postponed delivery of ~p~n", [self(), Msg]),
    {join, Details, msg_queue:add(QueuedMessages, Msg)}.

-spec get_local_dht_node_reregister_pid() -> pid().
get_local_dht_node_reregister_pid() ->
    process_dictionary:get_group_member(dht_node_reregister).
