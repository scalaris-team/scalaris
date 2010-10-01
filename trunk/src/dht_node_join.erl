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

-export([process_join_state/2, process_join_msg/2, check_config/0]).

-ifdef(with_export_type_support).
-export_type([join_state/0, join_message/0]).
-endif.

-include("scalaris.hrl").

-type(join_message() ::
    {join, join_request, NewPred::node:node_type()} | % join request
    {idholder_get_id_response, Id::?RT:key(), IdVersion::non_neg_integer()} |
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {join, get_node, Source_PID::comm:mypid(), Key::?RT:key()} |
    {join, get_node_response, Key::?RT:key(), Succ::node:node_type()} |
    {join, join_response, Pred::node:node_type(), MoveFullId::slide_op:id()} |
    {join, join_response, not_responsible} |
    {join, join_response_timeout, NewPred::node:node_type(), MoveFullId::slide_op:id()} |
    {join, lookup_timeout} |
    {join, known_hosts_timeout} |
    {join, timeout}).

-type(join_state() ::
    {join, {as_first}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase1}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase2, MyKey::?RT:key(), MyKeyVersion::non_neg_integer()}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase3, MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[node:node_type()]}, QueuedMessages::msg_queue:msg_queue()} |
    {join, {phase4, ContactNodes::[node:node_type()], Succ::node:node_type(), Me::node:node_type()}, QueuedMessages::msg_queue:msg_queue()}).

% join protocol
%% @doc Process a DHT node's join messages during the join phase.
-spec process_join_state(dht_node:message(), join_state()) -> dht_node_state:state().
% first node
%% userdevguide-begin dht_node_join:join_first
process_join_state({idholder_get_id_response, Id, IdVersion},
                   {join, {as_first}, QueuedMessages}) ->
    log:log(info, "[ Node ~w ] joining as first: ~p",[self(), Id]),
    Me = node:new(comm:this(), Id, IdVersion),
    finish_join(Me, Me, Me, ?DB:new(Id), QueuedMessages);  % join complete, State is the first "State"
%% userdevguide-end dht_node_join:join_first

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% !first node
%% userdevguide-begin dht_node_join:join_other_p12
% 1. get my key
process_join_state({idholder_get_id_response, Id, IdVersion},
                   {join, {phase1}, QueuedMessages}) ->
    %io:format("p1: got key~n"),
    log:log(info,"[ Node ~w ] joining",[self()]),
    get_known_nodes(),
    msg_delay:send_local(get_join_timeout() div 1000, self(), {join, timeout}),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

% 2. Find known hosts
process_join_state({join, known_hosts_timeout},
                   {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: known hosts timeout~n"),
    get_known_nodes(),
    State;

process_join_state({get_dht_nodes_response, []},
                   {join, {phase2, _Id, _IdVersion}, _QueuedMessages} = State) ->
    %io:format("p2: got empty dht_nodes_response~n"),
    % there is a VM with no nodes
    State;

process_join_state({get_dht_nodes_response, Nodes = [_|_]},
                   {join, {phase2, Id, IdVersion}, QueuedMessages} = State) ->
    %io:format("p2: got dht_nodes_response ~p~n", [lists:delete(comm:this(), Nodes)]),
    ContactNodes = [Node || Node <- Nodes, Node =/= comm:this()],
    % note: lookup will start in phase 2 when it gets an empty ContactNodes list
    case ContactNodes of
        [] -> State;
        _  -> lookup(Id, IdVersion, QueuedMessages, ContactNodes)
    end;
%% userdevguide-end dht_node_join:join_other_p12

%% userdevguide-begin dht_node_join:join_other_p3
% 3. lookup my position
process_join_state({get_dht_nodes_response, Nodes = [_|_]},
                   {join, {phase3, Id, IdVersion, ContactNodes}, QueuedMessages}) ->
    % although in phase3, collect further nodes to contact (messages have been send anyway):
    FurtherNodes = [Node || Node <- Nodes, Node =/= comm:this()],
    {join, {phase3, Id, IdVersion, lists:append(ContactNodes, FurtherNodes)}, QueuedMessages};

process_join_state({join, lookup_timeout},
                   {join, {phase3, Id, IdVersion, []}, QueuedMessages}) ->
    %io:format("p3: lookup_timeout~n"),
    % no more nodes left, go back to step 2
    get_known_nodes(),
    {join, {phase2, Id, IdVersion}, QueuedMessages};

process_join_state({join, get_node_response, Id, Succ},
                   {join, {phase3, Id, IdVersion, ContactNodes}, QueuedMessages}) ->
    %io:format("p3: lookup success~n"),
    % got my successor
    case Id =:= node:id(Succ) of
        true ->
            log:log(warn, "[ Node ~w ] chosen ID already exists, trying a new ID (~w retries)",
                    [self(), IdVersion]),
            try_new_id(IdVersion, QueuedMessages, ContactNodes);
        _ ->
            Me = node:new(comm:this(), Id, IdVersion),
            send_join_request(Me, Succ, 0),
            {join, {phase4, ContactNodes, Succ, Me}, QueuedMessages}
    end;
%% userdevguide-end dht_node_join:join_other_p3

%% userdevguide-begin dht_node_join:join_other_p4
% 4. joining my neighbor
process_join_state({get_dht_nodes_response, Nodes = [_|_]},
                   {join, {phase4, ContactNodes, Succ, Me}, QueuedMessages}) ->
    % although in phase4, collect further nodes to contact (messages have been send anyway):
    FurtherNodes = [Node || Node <- Nodes, Node =/= comm:this()],
    {join, {phase4, lists:append(ContactNodes, FurtherNodes), Succ, Me}, QueuedMessages};

process_join_state({join, join_request_timeout, Timeouts},
                   {join, {phase4, ContactNodes, Succ, Me}, QueuedMessages} = State) ->
    case Timeouts < 3 of
        true ->
            send_join_request(Me, Succ, Timeouts + 1),
            State;
        _ ->
            % no response from responsible node -> select new Id and try again
            log:log(warn, "[ Node ~w ] no response on join request for the chosen ID ~w, trying a new ID (~w retries)",
                    [self(), node:id(Me), node:id_version(Me)]),
            try_new_id(node:id_version(Me), QueuedMessages, ContactNodes)
    end;

process_join_state({join, join_response, not_responsible},
                   {join, {phase4, ContactNodes, _Succ, Me}, QueuedMessages}) ->
    % the node we contacted is not responsible for our key (anymore)
    % -> start a new lookup (back to phase 3)
    lookup(node:id(Me), node:id_version(Me), QueuedMessages, ContactNodes);

process_join_state({join, join_response, Pred, MoveId},
                   {join, {phase4, ContactNodes, Succ, Me}, QueuedMessages}) ->
    %io:format("p4: join_response~n"),
    MyKey = node:id(Me),
    case MyKey =:= node:id(Succ) orelse MyKey =:= node:id(Pred) of
        true ->
            log:log(warn, "[ Node ~w ] chosen ID already exists, trying a new ID (~w retries)",
                    [self(), node:id_version(Me)]),
            try_new_id(node:id_version(Me), QueuedMessages, ContactNodes);
        _ ->
            log:log(info, "[ Node ~w ] joined between ~w and ~w", [self(), Pred, Succ]),
            rm_loop:notify_new_succ(node:pidX(Pred), Me),
            rm_loop:notify_new_pred(node:pidX(Succ), Me),
            
            State = finish_join(Me, Pred, Succ, ?DB:new(MyKey), QueuedMessages),
            SlideOp = slide_op:new_receiving_slide_join(MoveId, Pred, Succ, MyKey),
            SlideOp1 = slide_op:set_phase(SlideOp, wait_for_node_update),
            State1 = dht_node_state:set_slide(State, succ, SlideOp1),
            State2 = dht_node_state:add_msg_fwd(State1, slide_op:get_interval(SlideOp1),
                                                slide_op:get_node(SlideOp1)),
            NewMsgQueue = msg_queue:add(QueuedMessages, {move, node_update, Me}),
            msg_queue:send(NewMsgQueue),
            State2
    end;
%% userdevguide-end dht_node_join:join_other_p4

% ignore some messages that appear too late for them to be used, e.g. if a new
% phase has already been started
process_join_state({join, known_hosts_timeout}, State) -> State;
process_join_state({get_dht_nodes_response, _KnownHosts}, State) -> State;
process_join_state({join, lookup_timeout}, State) -> State;
process_join_state({join, get_node_response, _KnownHosts}, State) -> State;
process_join_state({join, join_request_timeout, _Timeouts}, State) -> State;
process_join_state({join, join_response, _Pred, _MoveId}, State) -> State;
process_join_state({join, join_response, not_responsible}, State) -> State;

% a join timeout message re-starts the complete join
process_join_state({join, timeout},
                   {join, {phase2, _Id, IdVersion}, QueuedMessages}) ->
    restart_join(IdVersion, QueuedMessages);
process_join_state({join, timeout},
                   {join, {phase3, _Id, IdVersion, _ContactNodes}, QueuedMessages}) ->
    restart_join(IdVersion, QueuedMessages);
process_join_state({join, timeout},
                   {join, {phase4, _ContactNodes, _Succ, Me}, QueuedMessages}) ->
    restart_join(node:id_version(Me), QueuedMessages);
process_join_state({join, timeout}, State) ->
    State;

% Catch all other messages until the join procedure is complete
process_join_state(Msg, {join, Details, QueuedMessages}) ->
    %log:log(info("[dhtnode] [~p] postponed delivery of ~p", [self(), Msg]),
    {join, Details, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Process requests from a joining node at a existing node:.
-spec process_join_msg(dht_node:message(), dht_node_state:state()) -> dht_node_state:state().
process_join_msg({join, get_node, Source_PID, Key}, State) ->
    comm:send(Source_PID, {join, get_node_response, Key, dht_node_state:get(State, node)}),
    State;
%% userdevguide-begin dht_node_join:join_request1
process_join_msg({join, join_request, NewPred}, State) when (not is_atom(NewPred)) ->
    TargetId = node:id(NewPred),
    % only reply to join request with keys in our range:
    KeyInRange = dht_node_state:is_responsible(node:id(NewPred), State),
    case KeyInRange andalso dht_node_move:can_slide_pred(State, TargetId) of
        true ->
            % TODO: implement step-wise join
            MoveFullId = {util:get_pids_uid(), comm:this()},
            SlideOp = slide_op:new_sending_slide_join(MoveFullId,
                                                      node:pidX(NewPred),
                                                      TargetId, State),
            SlideOp1 = slide_op:set_phase(SlideOp, wait_for_pred_update),
            rm_loop:subscribe(self(), fun dht_node_move:rm_pred_changed/2,
                              fun dht_node_move:rm_notify_new_pred/3),
            State1 = dht_node_state:add_db_range(
                       State, slide_op:get_interval(SlideOp1)),
            send_join_response(State1, SlideOp1, NewPred, MoveFullId);
        _ when not KeyInRange ->
            comm:send(node:pidX(NewPred), {join, join_response, not_responsible}),
            State;
        _ -> State
    end;
process_join_msg({join, join_response_timeout, NewPred, MoveFullId}, State) ->
    % almost the same as dht_node_move:safe_operation/5 but we tolerate wrong pred:
    case dht_node_state:get_slide_op(State, MoveFullId) of
        {pred, SlideOp} ->
            ResponseReceived =
                lists:member(slide_op:get_phase(SlideOp),
                             [wait_for_req_data, wait_for_pred_update]),
            case (slide_op:get_timeouts(SlideOp) < 3) of
                _ when ResponseReceived -> State;
                true ->
                    NewSlideOp = slide_op:inc_timeouts(SlideOp),
                    send_join_response(State, NewSlideOp, NewPred, MoveFullId);
                _ ->
                    % abort the slide operation set up for the join:
                    % (similar to dht_node_move:abort_slide/*)
                    log:log(warn, "abort_join(op: ~p, reason: timeout)~n",
                            [SlideOp]),
                    slide_op:reset_timer(SlideOp), % reset previous timeouts
                    rm_loop:unsubscribe(self(),
                                        fun dht_node_move:rm_pred_changed/2,
                                        fun dht_node_move:rm_notify_new_pred/3),
                    State1 = dht_node_state:rm_db_range(
                               State, slide_op:get_interval(SlideOp)),
                    dht_node_state:set_slide(State1, pred, null)
            end;
        not_found -> State
    end;
%% userdevguide-end dht_node_join:join_request1
process_join_msg({join, known_hosts_timeout}, State) -> State;
process_join_msg({get_dht_nodes_response, _KnownHosts}, State) -> State;
process_join_msg({join, lookup_timeout}, State) -> State;
process_join_msg({join, get_node_response, _KnownHosts}, State) -> State;
process_join_msg({join, join_request_timeout, _Timeouts}, State) -> State;
process_join_msg({join, join_response, _Pred, _MoveId}, State) -> State;
process_join_msg({join, join_response, not_responsible}, State) -> State;
process_join_msg({join, timeout}, State) -> State.

%% @doc Contacts all nodes set in the known_hosts config parameter and request
%%      a list of dht_node instances in their VMs.
-spec get_known_nodes() -> ok.
get_known_nodes() ->
    KnownHosts = config:read(known_hosts),
    % contact all known VMs
    [comm:send(Host, {get_dht_nodes, comm:this()}) || Host <- KnownHosts],
    % timeout just in case
    msg_delay:send_local(get_known_hosts_timeout() div 1000, self(),
                         {join, known_hosts_timeout}).

%% @doc Tries to do a lookup for the given key by contacting the first node
%%      among the ContactNodes, then go to phase 3. If there is no node to
%%      contact, try to get new nodes to contact and cnotinue in phase 2.
-spec lookup(MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), QueuedMessages::msg_queue:msg_queue(), ContactNodes::[node:node_type()])
        -> NewState::{join, {phase2, MyKey::?RT:key(), MyKeyVersion::non_neg_integer()}, QueuedMessages::msg_queue:msg_queue()} |
                     {join, {phase3, MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), IdVersion::non_neg_integer(), ContactNodes::[node:node_type()]}, QueuedMessages::msg_queue:msg_queue()}.
lookup(Id, IdVersion, QueuedMessages, []) ->
    log:log(warn, "[ Node ~w ] no further nodes to contact, trying to get new nodes...", [self()]),
    get_known_nodes(),
    {join, {phase2, Id, IdVersion}, QueuedMessages};
lookup(Id, IdVersion, QueuedMessages, [First | Rest]) ->
    comm:send(First, {lookup_aux, Id, 0, {join, get_node, comm:this(), Id}}),
    msg_delay:send_local(get_lookup_timeout() div 1000, self(),
                         {join, lookup_timeout}),
    {join, {phase3, Id, IdVersion, Rest}, QueuedMessages}.

%% @doc Select new (random) Id and try again (phase 2 or 3 depending on the
%%      number of available contact nodes).
-spec try_new_id(OldIdVersion::pos_integer(),
                 QueuedMessages::msg_queue:msg_queue(), ContactNodes::[node:node_type()])
        -> NewState::{join, {phase2, MyKey::?RT:key(), MyKeyVersion::non_neg_integer()}, QueuedMessages::msg_queue:msg_queue()} |
                     {join, {phase3, MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), IdVersion::non_neg_integer(), ContactNodes::[node:node_type()]}, QueuedMessages::msg_queue:msg_queue()}.
try_new_id(OldIdVersion, QueuedMessages, ContactNodes) ->
    NewId = ?RT:get_random_node_id(),
    NewIdVersion = OldIdVersion + 1,
    idholder:set_id(NewId, NewIdVersion),
    lookup(NewId, NewIdVersion, QueuedMessages, ContactNodes).

%% @doc Sends a join request to the given successor. Timeouts is the number of
%%      join_request_timeout messages previously received.
-spec send_join_request(Me::node:node_type(), Succ::node:node_type(), Timeouts::non_neg_integer()) -> ok.
send_join_request(Me, Succ, Timeouts) ->
    comm:send(node:pidX(Succ), {join, join_request, Me}),
    msg_delay:send_local(get_join_request_timeout_timeout() div 1000,
                         self(), {join, join_request_timeout, Timeouts}).

%% @doc Sends a join response message to the new predecessor and sets the given
%%      slide operation in the dht_node state (adding a timeout to it as well).
%% userdevguide-begin dht_node_join:join_request
-spec send_join_response(State::dht_node_state:state(),
                         NewSlideOp::slide_op:slide_op(),
                         NewPred::node:node_type(), MoveFullId::slide_op:id())
        -> dht_node_state:state().
send_join_response(State, SlideOp, NewPred, MoveFullId) ->
    NewSlideOp = slide_op:set_timer(SlideOp, get_join_response_timeout(),
                                   {join, join_response_timeout, NewPred, MoveFullId}),
    MyOldPred = dht_node_state:get(State, pred),
    comm:send(node:pidX(NewPred), {join, join_response, MyOldPred, MoveFullId}),
    % no need to tell the ring maintenance -> the other node will trigger an update
    % also this is better in case the other node dies during the join
%%     rm_loop:notify_new_pred(comm:this(), NewPred),
    dht_node_state:set_slide(State, pred, NewSlideOp).
%% userdevguide-end dht_node_join:join_request

-spec restart_join(OldIdVersion::non_neg_integer(), QueuedMessages::msg_queue:msg_queue())
        -> {join, {phase1}, QueuedMessages::msg_queue:msg_queue()}.
restart_join(OldIdVersion, QueuedMessages) ->
    log:log(warn, "[ Node ~w ] join procedure taking longer than ~Bs, re-starting...", [self(), get_join_timeout()]),
    NewId = ?RT:get_random_node_id(),
    NewIdVersion = OldIdVersion + 1,
    idholder:set_id(NewId, NewIdVersion),
    comm:send_local(self(), {idholder_get_id_response, NewId, NewIdVersion}),
    {join, {phase1}, QueuedMessages}.

-spec finish_join(Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type(), DB::?DB:db(),
                  QueuedMessages::msg_queue:msg_queue())
        -> dht_node_state:state().
finish_join(Me, Pred, Succ, DB, QueuedMessages) ->
    rm_loop:activate(Me, Pred, Succ),
    % wait for the ring maintenance to initialize and tell us its table ID
    NeighbTable = rm_loop:get_neighbors_table(),
    Id = node:id(Me),
    rt_loop:activate(Id, Pred, Succ),
    cyclon:activate(),
    vivaldi:activate(),
    dc_clustering:activate(),
    gossip:activate(),
    comm:send_local(pid_groups:get_my(dht_node_reregister), {register}),
    msg_queue:send(QueuedMessages),
    dht_node_state:new(?RT:empty_ext(Succ), NeighbTable, DB).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(join_response_timeout) and
    config:is_greater_than_equal(join_response_timeout, 1000) and

    config:is_integer(join_request_timeout) and
    config:is_greater_than_equal(join_request_timeout, 1000) and

    config:is_integer(join_lookup_timeout) and
    config:is_greater_than_equal(join_lookup_timeout, 1000) and

    config:is_integer(join_known_hosts_timeout) and
    config:is_greater_than_equal(join_known_hosts_timeout, 1000) and

    config:is_integer(join_timeout) and
    config:is_greater_than_equal(join_timeout, 1000).

%% @doc Gets the max number of ms to wait for a joining node's reply after
%%      it send a join request (set in the config files).
-spec get_join_response_timeout() -> pos_integer().
get_join_response_timeout() ->
    config:read(join_response_timeout).

%% @doc Gets the max number of ms to wait for a scalaris node to reply on a
%%      join request (set in the config files).
-spec get_join_request_timeout_timeout() -> pos_integer().
get_join_request_timeout_timeout() ->
    config:read(join_request_timeout).

%% @doc Gets the max number of ms to wait for a key lookup response
%%      (set in the config files).
-spec get_lookup_timeout() -> pos_integer().
get_lookup_timeout() ->
    config:read(join_lookup_timeout).

%% @doc Gets the max number of ms to wait for a key lookup response
%%      (set in the config files).
-spec get_known_hosts_timeout() -> pos_integer().
get_known_hosts_timeout() ->
    config:read(join_known_hosts_timeout).

%% @doc Gets the max number of ms to wait for a join to be completed
%%      (set in the config files).
-spec get_join_timeout() -> pos_integer().
get_join_timeout() ->
    config:read(join_timeout).
