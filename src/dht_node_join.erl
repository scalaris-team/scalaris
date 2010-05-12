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
-vsn('$Id$ ').

-export([join_request/4, process_join_msg/2]).

-include("scalaris.hrl").

-type(join_message() ::
    {idholder_get_id_response, Id::?RT:key(), IdVersion::non_neg_integer()} |
    {known_hosts_timeout} |
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {lookup_timeout} |
    {get_node_response, Key::?RT:key(), Succ::node:node_type()} |
    {join_response, Pred::node:node_type(), Data::any()}).

-type(join_state() ::
    {join, {as_first}, QueuedMessages::list()} |
    {join, {phase1}, QueuedMessages::list()} |
    {join, {phase2, MyKey::?RT:key()}, QueuedMessages::list()} |
    {join, {phase3, Nodes::[node:node_type()], MyKey::?RT:key()}, QueuedMessages::list()} |
    {join, {phase4, MyKey::?RT:key(), Succ::node:node_type(), Me::node:node_type()}, QueuedMessages::list()}).

%% @doc handle the join request of a new node
%% @spec join_request(state:state(), pid(), Id) -> state:state()
%%   Id = term()

%% userdevguide-begin dht_node_join:join_request
-spec join_request(dht_node_state:state(), cs_send:mypid(), Id::?RT:key(), IdVersion::non_neg_integer()) -> dht_node_state:state().
join_request(State, Source_PID, Id, IdVersion) ->
    Pred = node:new(Source_PID, Id, IdVersion),
    {DB, HisData} = ?DB:split_data(dht_node_state:get_db(State), dht_node_state:id(State), Id),
    
    %%TODO: split data [{Key, Value, Version}], schedule transfer
    
    cs_send:send(Source_PID, {join_response, dht_node_state:pred(State), HisData}),
    % TODO: better update our range here directly instead of rm_beh sending dht_node a message?
    rm_beh:update_preds([Pred]),
    dht_node_state:set_db(State, DB).
%% userdevguide-end dht_node_join:join_request



% join protocol
%% @doc Processes a DHT node's join messages (the node joining a Scalaris DHT).
-spec process_join_msg(join_message() | any(), join_state()) -> dht_node_state:state().
% first node
process_join_msg({idholder_get_id_response, Id, IdVersion}, {join, {as_first}, QueuedMessages}) ->
    log:log(info,"[ Node ~w ] joining as first: ~p",[self(), Id]),
    Me = node:new(cs_send:this(), Id, IdVersion),
    rt_beh:initialize(Id, Me, Me),
    NewState = dht_node_state:new(?RT:empty(Me), Me, Me, Me, {Id, Id}, dht_node_lb:new(), ?DB:new(Id)),
    cs_send:send_local(get_local_dht_node_reregister_pid(), {go}),
    send_queued_messages(QueuedMessages),
    %log:log(info,"[ Node ~w ] joined",[self()]),
    NewState;  % join complete, State is the first "State"

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% !first node
% 1. get my key
process_join_msg({idholder_get_id_response, Id, IdVersion}, {join, {phase1}, QueuedMessages}) ->
    %io:format("p1: got key~n"),
    log:log(info,"[ Node ~w ] joining",[self()]),
    % send message to avoid code duplication
    cs_send:send_local(self(), {known_hosts_timeout}),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

% 2. Find known hosts
process_join_msg({known_hosts_timeout}, {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: known hosts timeout~n"),
    KnownHosts = config:read(known_hosts),
    % contact all known VMs
    _Res = [cs_send:send(KnownHost, {get_dht_nodes, cs_send:this()})
           || KnownHost <- KnownHosts],
    %io:format("~p~n", [_Res]),
    % timeout just in case
    cs_send:send_local_after(1000, self(), {known_hosts_timeout}),
    State;

process_join_msg({get_dht_nodes_response, []}, {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: got empty dht_nodes_response~n"),
    % there is a VM with no nodes
    State;

process_join_msg({get_dht_nodes_response, Nodes}, {join, {phase2, Id, IdVersion}, QueuedMessages} = State) ->
    %io:format("p2: got dht_nodes_response ~p~n", [lists:delete(cs_send:this(), Nodes)]),
    case lists:delete(cs_send:this(), Nodes) of
        [] ->
            State;
        [First | Rest] ->
            cs_send:send(First, {lookup_aux, Id, 0, {get_node, cs_send:this(), Id}}),
            cs_send:send_local_after(3000, self(), {lookup_timeout}),
            {join, {phase3, Rest, Id, IdVersion}, QueuedMessages}
    end;

% 3. lookup my position
process_join_msg({lookup_timeout}, {join, {phase3, [], Id, IdVersion}, QueuedMessages}) ->
    %io:format("p3: lookup_timeout~n"),
    % no more nodes left, go back to step 2
    cs_send:send_local(self(), {known_hosts_timeout}),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

process_join_msg({get_node_response, Id, Succ}, {join, {phase3, _DHTNodes, Id, IdVersion}, QueuedMessages}) ->
    %io:format("p3: lookup success~n"),
    % got my successor
    Me = node:new(cs_send:this(), Id, IdVersion),
    % announce join request
    cs_send:send(node:pidX(Succ), {join, cs_send:this(), Id, IdVersion}),
    {join, {phase4, Succ, Me}, QueuedMessages};

% 4. joining my neighbors
process_join_msg({join_response, Pred, Data}, {join, {phase4, Succ, Me}, QueuedMessages}) ->
    Id = node:id(Me),
    %io:format("p4: join_response~n"),
    % @TODO data shouldn't be moved here, might be large
    log:log(info, "[ Node ~w ] got pred ~w",[self(), Pred]),
    DB = ?DB:add_data(?DB:new(Id), Data),
    rt_beh:initialize(Id, Pred, Succ),
    State = case node:is_valid(Pred) of
                true ->
                    cs_send:send(node:pidX(Pred), {update_succ, Me}),
                    dht_node_state:new(?RT:empty(Succ), Succ, Pred, Me,
                                       {node:id(Pred), Id}, dht_node_lb:new(),
                                       DB);
                false ->
                    dht_node_state:new(?RT:empty(Succ), Succ, Pred, Me,
                                       {Id, Id}, dht_node_lb:new(), DB)
            end,
    cs_replica_stabilization:recreate_replicas(dht_node_state:get_my_range(State)),
    cs_send:send_local(get_local_dht_node_reregister_pid(), {go}),
    send_queued_messages(QueuedMessages),
    State;

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
    {join, Details, [Msg | QueuedMessages]}.

%% @doc Sends queued messages to the dht node itself in the order they have been
%%      received.
-spec send_queued_messages(list()) -> ok.
send_queued_messages(QueuedMessages) ->
    lists:foldr(fun(Msg, _) -> cs_send:send_local(self(), Msg) end, ok, QueuedMessages).

-spec get_local_dht_node_reregister_pid() -> pid().
get_local_dht_node_reregister_pid() ->
    process_dictionary:get_group_member(dht_node_reregister).
