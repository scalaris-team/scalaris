%  @copyright 2007-2015 Zuse Institute Berlin

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
%% @doc    dht_node join procedure
%% @end
%% @version $Id$
-module(dht_node_join).
-author('schuett@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE_JOIN1(Msg, JoinState),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  Phase: ~.0p~n  JoinUUID: ~.0p"
               "~n  Version: ~.0p~n  Contacts: ~.0p~n  JoinIDs: ~.0p"
               "~n  Candidates: ~.0p~n",
               [self(), Msg, get_phase(JoinState), get_join_uuid(JoinState), get_id_version(JoinState),
                lists:sublist(lists:reverse(get_connections(JoinState)), erlang:min(5, length(get_connections(JoinState)))),
                get_join_ids(JoinState),
                lists:sublist(get_candidates(JoinState), erlang:min(5, length(get_candidates(JoinState))))])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).
-define(TRACE_JOIN_STATE(State), ?TRACE("[ ~.0p ]~n  Phase: ~.0p~n  JoinUUID: ~.0p"
        "~n  Version: ~.0p~n  Contacts: ~.0p~n  JoinIDs: ~.0p"
        "~n  Candidates: ~.0p~n",
        [self(), get_phase(State), get_join_uuid(State), get_id_version(State),
         lists:sublist(lists:reverse(get_connections(State)), erlang:min(5, length(get_connections(State)))),
         get_join_ids(State),
         lists:sublist(get_candidates(State), erlang:min(5, length(get_candidates(State))))])).

-define(VALID_PASSIVE_ALGORITHMS, [lb_psv_simple, lb_psv_split, lb_psv_gossip]).

-export([join_as_first/3, join_as_other/3,
         process_join_state/2, process_join_msg/2, check_config/0]).

% for join_leave_SUITE:
-export([reject_join_response/4]).

-export_type([join_state/0, join_message/0, connection/0]).

-include("scalaris.hrl").

-type connection() :: {null | pos_integer(), comm:mypid()}.

-type(join_message() ::
    % messages at the joining node:
    {join, start} |
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {join, get_number_of_samples, Samples::non_neg_integer(), Conn::connection()} |
    {join, get_candidate_response, OrigJoinId::?RT:key(), Candidate::lb_op:lb_op(), Conn::connection()} |
    {join, join_response, Succ::node:node_type(), Pred::node:node_type(), MoveFullId::slide_op:id(),
     CandId::lb_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} |
    {join, join_response, not_responsible | busy, CandId::lb_op:id()} |
    {join, known_hosts_timeout, JoinUUId::pos_integer()} |
    {join, lookup_timeout, Conn::connection(), JoinId::?RT:key(), JoinUUId::pos_integer()} |
    {join, get_number_of_samples_timeout, Conn::connection(), JoinUUId::pos_integer()} |
    {join, join_request_timeout, Timeouts::non_neg_integer(), CandId::lb_op:id(), JoinUUId::pos_integer()} |
    {join, timeout, JoinUUId::pos_integer()} |
    % messages at the existing node:
    {join, number_of_samples_request, SourcePid::comm:mypid(), LbPsv::module(), Conn::connection()} |
    {join, get_candidate, Source_PID::comm:mypid(), Key::?RT:key(), LbPsv::module(), Conn::connection()} |
    {join, join_request, NewPred::node:node_type(), CandId::lb_op:id(), MaxTransportEntries::unknown | pos_integer()} |
    {join, LbPsv::module(),
     Msg::lb_psv_simple:custom_message() | lb_psv_split:custom_message() |
          lb_psv_gossip:custom_message(),
     LbPsvState::term()}
    ).

-type phase2() ::
    {phase2,  JoinUUId::pos_integer(), Options::[tuple()], MyKeyVersion::non_neg_integer(),
     Connections::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase2b() ::
    {phase2b, JoinUUId::pos_integer(), Options::[tuple()], MyKeyVersion::non_neg_integer(),
     Connections::[{null | pos_integer(), comm:mypid()},...],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase3() ::
    {phase3,  JoinUUId::pos_integer(), Options::[tuple()], MyKeyVersion::non_neg_integer(),
     Connections::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase4() ::
    {phase4,  JoinUUId::pos_integer(), Options::[tuple()], MyKeyVersion::non_neg_integer(),
     Connections::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase_2_4() :: phase2() | phase2b() | phase3() | phase4().

-type join_state() ::
    {join, {phase1,  JoinUUId::pos_integer(), Options::[tuple()], MyKeyVersion::non_neg_integer(),
            Connections::[], JoinIds::[?RT:key()], Candidates::[]},
     QueuedMessages::msg_queue:msg_queue()} |
    {join, phase_2_4(), QueuedMessages::msg_queue:msg_queue()}.

%% userdevguide-begin dht_node_join:join_as_first
-spec join_as_first(Id::?RT:key(), IdVersion::non_neg_integer(), Options::[tuple()])
        -> dht_node_state:state().
join_as_first(Id, IdVersion, _Options) ->
    log:log(info, "[ Node ~w ] joining as first: (~.0p, ~.0p)",
            [self(), Id, IdVersion]),
    Me = node:new(comm:this(), Id, IdVersion),
    % join complete, State is the first "State"
    finish_join(Me, Me, Me, db_dht:new(db_dht), msg_queue:new(), []).
%% userdevguide-end dht_node_join:join_as_first

%% userdevguide-begin dht_node_join:join_as_other
-spec join_as_other(Id::?RT:key(), IdVersion::non_neg_integer(), Options::[tuple()])
        -> {'$gen_component', [{on_handler, Handler::gen_component:handler()}],
            State::{join, phase2(), msg_queue:msg_queue()}}.
join_as_other(Id, IdVersion, Options) ->
    log:log(info,"[ Node ~w ] joining, trying ID: (~.0p, ~.0p)",
            [self(), Id, IdVersion]),
    JoinUUID = uid:get_pids_uid(),
    gen_component:change_handler(
      {join, {phase1, JoinUUID, Options, IdVersion, [], [Id], []},
       msg_queue:new()},
      fun ?MODULE:process_join_state/2).
%% userdevguide-end dht_node_join:join_as_other

% join protocol
%% @doc Process a DHT node's join messages during the join phase.
-spec process_join_state(dht_node:message(), join_state())
        -> join_state() |
           {'$gen_component', [{on_handler, Handler::gen_component:handler()}], dht_node_state:state()}.
% !first node
% start join (starting via message allows tracing with trace_mpath)
process_join_state({join, start} = _Msg,
                   {join, JoinState, QueuedMessages}) ->
    JoinUUID = element(2, JoinState),
    get_known_nodes(JoinUUID),
    msg_delay:send_local(get_join_timeout() div 1000, self(),
                         {join, timeout, JoinUUID}),
    {join, set_phase(phase2, JoinState), QueuedMessages};
% 2. Find known hosts
% no matter which phase, if there are no contact nodes (yet), try to get some
process_join_state({join, known_hosts_timeout, JoinUUId} = _Msg,
                   {join, JoinState, _QueuedMessages} = State)
  when element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    case get_connections(JoinState) of
        [] -> get_known_nodes(JoinUUId);
        [_|_] -> ok
    end,
    State;

% ignore unrelated known_hosts_timeout:
process_join_state({join, known_hosts_timeout, _JoinId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, _JoinState),
    State;

%% userdevguide-begin dht_node_join:join_other_p2
% in phase 2 add the nodes and do lookups with them / get number of samples
process_join_state({get_dht_nodes_response, Nodes} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 ->
    ?TRACE_JOIN1(_Msg, JoinState),
    JoinOptions = get_join_options(JoinState),
    %% additional nodes required when firstnode jumps and he's the only known host
    DhtNodes = Nodes ++ proplists:get_value(bootstrap_nodes, JoinOptions, []),
    Connections = [{null, Node} || Node <- DhtNodes, Node =/= comm:this()],
    JoinState1 = add_connections(Connections, JoinState, back),
    NewJoinState = phase2_next_step(JoinState1, Connections),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% in all other phases, just add the provided nodes:
process_join_state({get_dht_nodes_response, Nodes} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2b orelse
           element(1, JoinState) =:= phase3 orelse
           element(1, JoinState) =:= phase4 ->
    ?TRACE_JOIN1(_Msg, JoinState),
    Connections = [{null, Node} || Node <- Nodes, Node =/= comm:this()],
    JoinState1 = add_connections(Connections, JoinState, back),
    ?TRACE_JOIN_STATE(JoinState1),
    {join, JoinState1, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p2

% 2b. get the number of nodes/ids to sample

% on timeout:
%  - in phase 2b, remove failed connection, start over
%  - in other phases, ignore this timeout (but remove the connection)
process_join_state({join, get_number_of_samples_timeout, Conn, JoinUUId} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    JoinState1 = remove_connection(Conn, JoinState),
    NewJoinState = case get_phase(JoinState1) of
                       phase2b -> start_over(JoinState1);
                       _       -> JoinState1
                   end,
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

process_join_state({join, get_number_of_samples_timeout, _Conn, _JoinId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, _JoinState),
    State;

%% userdevguide-begin dht_node_join:join_other_p2b
% note: although this message was send in phase2b, also accept message in
% phase2, e.g. messages arriving from previous calls
process_join_state({join, get_number_of_samples, Samples, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b ->
    ?TRACE_JOIN1(_Msg, JoinState),
    % prefer node that send get_number_of_samples as first contact node
    JoinState1 = reset_connection(Conn, JoinState),
    % (re-)issue lookups for all existing IDs and
    % create additional samples, if required
    NewJoinState = lookup_new_ids2(Samples, JoinState1),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% ignore message arriving in other phases:
process_join_state({join, get_number_of_samples, _Samples, Conn} = _Msg,
                   {join, JoinState, QueuedMessages}) ->
    ?TRACE_JOIN1(_Msg, JoinState),
    NewJoinState = reset_connection(Conn, JoinState),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p2b

% 3. lookup all positions
process_join_state({join, lookup_timeout, Conn, Id, JoinUUId} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 andalso
           element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    % do not know whether the contact node is dead or the lookup takes too long
    % -> simple solution: try with an other contact node, remove the current one
    JoinState1 = remove_connection(Conn, JoinState),
    NewJoinState = lookup(JoinState1, [Id]),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% only remove the failed connection in other phases:
process_join_state({join, lookup_timeout, Conn, _Id, JoinUUId} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    NewJoinState = remove_connection(Conn, JoinState),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% ignore unrelated lookup_timeout messages:
process_join_state({join, lookup_timeout, _Conn, _Id, _JoinId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, _JoinState),
    State;

%% userdevguide-begin dht_node_join:join_other_p3
process_join_state({join, get_candidate_response, OrigJoinId, Candidate, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 ->
    ?TRACE_JOIN1(_Msg, JoinState),
    JoinState0 = reset_connection(Conn, JoinState),
    JoinState1 = remove_join_id(OrigJoinId, JoinState0),
    JoinState2 = integrate_candidate(Candidate, JoinState1, front),
    NewJoinState =
        case get_join_ids(JoinState2) of
            [] -> % no more join ids to look up -> join with the best:
                contact_best_candidate(JoinState2);
            [_|_] -> % still some unprocessed join ids -> wait
                JoinState2
        end,
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% In phase 2 or 2b, also add the candidate but do not continue.
% In phase 4, add the candidate to the end of the candidates as they are sorted
% and the join with the first has already started (use this candidate as backup
% if the join fails). Do not start a new join.
process_join_state({join, get_candidate_response, OrigJoinId, Candidate, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b orelse
           element(1, JoinState) =:= phase4 ->
    ?TRACE_JOIN1(_Msg, JoinState),
    JoinState0 = reset_connection(Conn, JoinState),
    JoinState1 = remove_join_id(OrigJoinId, JoinState0),
    JoinState2 = case get_phase(JoinState1) of
                     phase4 -> integrate_candidate(Candidate, JoinState1, back);
                     _      -> integrate_candidate(Candidate, JoinState1, front)
                 end,
    ?TRACE_JOIN_STATE(JoinState2),
    {join, JoinState2, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p3

% 4. joining my neighbor
process_join_state({join, join_request_timeout, Timeouts, CandId, JoinUUId} = _Msg,
                   {join, JoinState, QueuedMessages} = State)
  when element(1, JoinState) =:= phase4 andalso
           element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    case get_candidates(JoinState) of
        [] -> State; % no candidates -> late timeout, ignore
        [BestCand | _] ->
            case lb_op:get(BestCand, id) =:= CandId of
                false -> State; % unrelated/old message
                _ ->
                    NewJoinState =
                        case Timeouts < get_join_request_timeouts() of
                            true ->
                                send_join_request(JoinState, Timeouts + 1);
                            _ ->
                                % no response from responsible node
                                % -> select new candidate, try again
                                log:log(warn, "[ Node ~w ] no response on join "
                                              "request for the chosen ID, "
                                              "trying next candidate", [self()]),
                                try_next_candidate(JoinState)
                        end,
                    ?TRACE_JOIN_STATE(NewJoinState),
                    {join, NewJoinState, QueuedMessages}
            end
    end;

% ignore late or unrelated join_request_timeout message:
process_join_state({join, join_request_timeout, _Timeouts, _CandId, _JoinId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, _JoinState),
    State;

%% userdevguide-begin dht_node_join:join_other_p4
process_join_state({join, join_response, Reason, CandId} = _Msg,
                   {join, JoinState, QueuedMessages} = State)
  when element(1, JoinState) =:= phase4 andalso
           (Reason =:= not_responsible orelse Reason =:= busy) ->
    ?TRACE_JOIN1(_Msg, JoinState),
    % the node we contacted is not responsible for the selected key anymore
    % -> try the next candidate, if the message is related to the current candidate
    case get_candidates(JoinState) of
        [] -> % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                        "starting over", [self()]),
            NewJoinState = start_over(JoinState),
            ?TRACE_JOIN_STATE(NewJoinState),
            {join, NewJoinState, QueuedMessages};
        [Candidate | _Rest] ->
            case lb_op:get(Candidate, id) =:= CandId of
                false -> State; % unrelated/old message
                _ ->
                    if Reason =:= not_responsible ->
                           log:log(info,
                                   "[ Node ~w ] node contacted for join is not "
                                       "responsible for the selected ID (anymore), "
                                       "trying next candidate",
                                   [self()]);
                       Reason =:= busy ->
                           log:log(info,
                                   "[ Node ~w ] node contacted for join is busy, "
                                       "trying next candidate",
                                   [self()])
                    end,
                    NewJoinState = try_next_candidate(JoinState),
                    ?TRACE_JOIN_STATE(NewJoinState),
                    {join, NewJoinState, QueuedMessages}
            end
    end;

% in other phases remove the candidate from the list (if it still exists):
process_join_state({join, join_response, Reason, CandId} = _Msg,
                   {join, JoinState, QueuedMessages})
  when (Reason =:= not_responsible orelse Reason =:= busy) ->
    ?TRACE_JOIN1(_Msg, JoinState),
    {join, remove_candidate(CandId, JoinState), QueuedMessages};

% note: accept (delayed) join_response messages in any phase
process_join_state({join, join_response, Succ, Pred, MoveId, CandId, TargetId, NextOp} = _Msg,
                   {join, JoinState, QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, JoinState),
    % only act on related messages, i.e. messages from the current candidate
    Phase = get_phase(JoinState),
    State1 = case get_candidates(JoinState) of
        [] when Phase =:= phase4 ->
            % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                           "starting over", [self()]),
            reject_join_response(Succ, Pred, MoveId, CandId),
            NewJoinState = start_over(JoinState),
            ?TRACE_JOIN_STATE(NewJoinState),
            {join, NewJoinState, QueuedMessages};
        [] ->
            % in all other phases, ignore the delayed join_response if no
            % candidates exist
            reject_join_response(Succ, Pred, MoveId, CandId),
            State;
        [Candidate | _Rest] ->
            CandidateNode = node_details:get(lb_op:get(Candidate, n1succ_new), node),
            CandidateNodeSame = node:same_process(CandidateNode, Succ),
            case lb_op:get(Candidate, id) =:= CandId of
                false ->
                    % ignore old/unrelated message
                    log:log(warn, "[ Node ~w ] ignoring old or unrelated "
                                  "join_response message", [self()]),
                    reject_join_response(Succ, Pred, MoveId, CandId),
                    State;
                _ when not CandidateNodeSame ->
                    % id is correct but the node is not (should never happen!)
                    log:log(error, "[ Node ~w ] got join_response but the node "
                                  "changed, trying next candidate", [self()]),
                    reject_join_response(Succ, Pred, MoveId, CandId),
                    NewJoinState = try_next_candidate(JoinState),
                    ?TRACE_JOIN_STATE(NewJoinState),
                    {join, NewJoinState, QueuedMessages};
                _ ->
                    MyId = TargetId,
                    MyIdVersion = get_id_version(JoinState),
                    case MyId =:= node:id(Succ) orelse MyId =:= node:id(Pred) of
                        true ->
                            log:log(warn, "[ Node ~w ] chosen ID already exists, "
                                          "trying next candidate", [self()]),
                            reject_join_response(Succ, Pred, MoveId, CandId),
                            % note: can not keep Id, even if skip_psv_lb is set
                            JoinState1 = remove_candidate_front(JoinState),
                            NewJoinState = contact_best_candidate(JoinState1),
                            ?TRACE_JOIN_STATE(NewJoinState),
                            {join, NewJoinState, QueuedMessages};
                        _ ->
                            ?TRACE("[ ~.0p ]~n  joined MyId:~.0p, MyIdVersion:~.0p~n  "
                                       "Succ: ~.0p~n  Pred: ~.0p~n",
                                       [self(), MyId, MyIdVersion, Succ, Pred]),
                            Me = node:new(comm:this(), MyId, MyIdVersion),
                            log:log(info, "[ Node ~w ] joined between ~w and ~w",
                                    [self(), Pred, Succ]),
                            rm_loop:notify_new_succ(node:pidX(Pred), Me),
                            rm_loop:notify_new_pred(node:pidX(Succ), Me),

                            JoinOptions = get_join_options(JoinState),

                            finish_join_and_slide(Me, Pred, Succ, db_dht:new(db_dht),
                                                  QueuedMessages, MoveId, NextOp, JoinOptions)
                    end
            end
    end,
    State1;
%% userdevguide-end dht_node_join:join_other_p4

% a join timeout message re-starts the complete join
process_join_state({join, timeout, JoinUUId} = _Msg, {join, JoinState, QueuedMessages})
  when (element(1, JoinState) =:= phase2 orelse
            element(1, JoinState) =:= phase2b orelse
            element(1, JoinState) =:= phase3 orelse
            element(1, JoinState) =:= phase4) andalso
           element(2, JoinState) =:= JoinUUId ->
    ?TRACE_JOIN1(_Msg, JoinState),
    log:log(warn, "[ Node ~w ] join timeout hit, starting over...", [self()]),
    NewJoinState = start_over(JoinState),
    ?TRACE_JOIN_STATE(NewJoinState),
    {join, NewJoinState, QueuedMessages};

% ignore unrelated join timeout message:
process_join_state({join, timeout, _JoinId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, _JoinState),
    State;

process_join_state({web_debug_info, Requestor} = _Msg,
                   {join, JoinState, QueuedMessages} = State) ->
    ?TRACE_JOIN1(_Msg, JoinState),
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", webhelpers:safe_html_string("~p", [Message])}
                  || Message <- lists:sublist(QueuedMessages, 50)],
    MessageList = case length(QueuedMessages) > 50 of
                      true -> lists:append(MessageListTmp, [{"...", ""}]);
                      _    -> MessageListTmp
                  end,
    Phase = get_phase(JoinState),
    StateInfo =
        case lists:member(Phase, [phase2, phase2b, phase3, phase4]) of
            true ->
                [{"phase",       Phase},
                 {"key_vers",    get_id_version(JoinState)},
                 {"connections", webhelpers:safe_html_string("~p", [get_connections(JoinState)])},
                 {"join_ids",    webhelpers:safe_html_string("~p", [get_join_ids(JoinState)])},
                 {"candidates",  webhelpers:safe_html_string("~p", [get_candidates(JoinState)])}];
            _ -> [{"phase",      Phase}]
        end,
    KeyValueList =
        [{"", ""}, {"joining dht_node process", ""} |
             lists:append(StateInfo,
                          [{"", ""},
                           {"queued messages:", ""} | MessageList])],
    ?TRACE_SEND(Requestor, {web_debug_info_reply, KeyValueList}),
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

process_join_state({?lookup_aux, Key, Hops, Msg} = FullMsg,
                   {join, JoinState, _QueuedMessages} = State) ->
    case get_connections(JoinState) of
        [] ->
            _ = comm:send_local_after(100, self(), FullMsg),
            ok;
        [{_, Pid} | _] ->
            % integrate the list of processes for which the send previously failed:
            Self = comm:reply_as(self(), 2, {join, '_', []}),
            comm:send(Pid, {?lookup_aux, Key, Hops + 1, Msg}, [{shepherd, Self}])
    end,
    State;
process_join_state({join, {send_error, Target, {?lookup_aux, Key, Hops, Msg}, _Reason},
                    FailedPids}, {join, JoinState, _QueuedMessages} = State) ->
    Connections = get_connections(JoinState),
    case lists:dropwhile(fun({_, Pid}) -> lists:member(Pid, FailedPids) end, Connections) of
        [] ->
            _ = comm:send_local_after(100, pid_groups:get_my(routing_table), {?lookup_aux, Key, Hops + 1, Msg}),
            ok;
        [{_, Pid} | _] ->
            % integrate the list of processes for which the send previously failed:
            Self = comm:reply_as(self(), 2, {join, '_', [Target | FailedPids]}),
            comm:send(Pid, {?lookup_aux, Key, Hops + 1, Msg}, [{shepherd, Self}])
    end,
    State;
process_join_state({join, {send_error, Target, {get_dht_nodes, _This}, _Reason},
                    FailedPids}, State) ->
    KnownHosts = config:read(known_hosts),
    NextHost = case lists:dropwhile(fun(Pid) ->
                                            lists:member(Pid, FailedPids)
                                    end, KnownHosts) of
                   [] -> util:randomelem(KnownHosts);
                   [X | _] -> X
               end,
    % integrate the list of processes for which the send previously failed:
    Self = comm:reply_as(self(), 2, {join, '_', [Target | FailedPids]}),
    ?TRACE_SEND(NextHost, {get_dht_nodes, comm:this()}),
    comm:send(NextHost, {get_dht_nodes, comm:this()}, [{shepherd, Self}]),
    State;

% do not queue rm_trigger to prevent its infection with msg_queue:send/1
process_join_state({rm, trigger} = _Msg, State) ->
    ?TRACE_JOIN1(_Msg, element(2, State)),
    rm_loop:send_trigger(),
    State;

% do not queue rm_trigger to prevent its infection with msg_queue:send/1
process_join_state({move, check_for_timeouts} = _Msg, State) ->
    ?TRACE_JOIN1(_Msg, element(2, State)),
    dht_node_move:send_trigger(),
    State;

% Catch all other messages until the join procedure is complete
process_join_state(Msg, {join, JoinState, QueuedMessages}) ->
    ?TRACE_JOIN1(Msg, JoinState),
    %log:log(info("[dhtnode] [~p] postponed delivery of ~p", [self(), Msg]),
    {join, JoinState, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Process requests from a joining node at a existing node:
-spec process_join_msg(join_message(), dht_node_state:state()) -> dht_node_state:state().
process_join_msg({join, start}, State) ->
    State;
process_join_msg({join, number_of_samples_request, SourcePid, LbPsv, Conn} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    call_lb_psv(LbPsv, get_number_of_samples_remote, [SourcePid, Conn]),
    State;

%% userdevguide-begin dht_node_join:get_candidate
process_join_msg({join, get_candidate, Source_PID, Key, LbPsv, Conn} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    call_lb_psv(LbPsv, create_join, [State, Key, Source_PID, Conn]);
%% userdevguide-end dht_node_join:get_candidate

%% userdevguide-begin dht_node_join:join_request1
process_join_msg({join, join_request, NewPred, CandId, MaxTransportEntries} = _Msg, State)
  when (not is_atom(NewPred)) -> % avoid confusion with not_responsible message
    ?TRACE1(_Msg, State),
    TargetId = node:id(NewPred),
    JoinType = {join, 'send'},
    MyNode = dht_node_state:get(State, node),
    Command = dht_node_move:check_setup_slide_not_found(
                State, JoinType, MyNode, NewPred, TargetId),
    case Command of
        {ok, JoinType} ->
            MoveFullId = uid:get_global_uid(),
            State1 = dht_node_move:exec_setup_slide_not_found(
                       Command, State, MoveFullId, NewPred, TargetId, join,
                       MaxTransportEntries, null, nomsg, {none}, false),
            % set up slide, now send join_response:
            MyOldPred = dht_node_state:get(State1, pred),
            % no need to tell the ring maintenance -> the other node will trigger an update
            % also this is better in case the other node dies during the join
            %%     rm_loop:notify_new_pred(comm:this(), NewPred),
            SlideOp = dht_node_state:get(State1, slide_pred),
            Msg = {join, join_response, MyNode, MyOldPred, MoveFullId, CandId,
                   slide_op:get_target_id(SlideOp), slide_op:get_next_op(SlideOp)},
            dht_node_move:send(node:pidX(NewPred), Msg, MoveFullId),
            State1;
        {abort, ongoing_slide, JoinType} ->
            ?TRACE("[ ~.0p ]~n  rejecting join_request from ~.0p due to a running slide~n",
                   [self(), NewPred]),
            ?TRACE_SEND(node:pidX(NewPred), {join, join_response, busy, CandId}),
            comm:send(node:pidX(NewPred), {join, join_response, busy, CandId}),
            State;
        {abort, _Reason, JoinType} -> % all other errors:
            ?TRACE("~p", [Command]),
            ?TRACE_SEND(node:pidX(NewPred),
                        {join, join_response, not_responsible, CandId}),
            comm:send(node:pidX(NewPred),
                      {join, join_response, not_responsible, CandId}),
            State
    end;
%% userdevguide-end dht_node_join:join_request1

% only messages with the first element being "join" are processed here
% -> see dht_node.erl
process_join_msg({get_dht_nodes_response, _Nodes} = _Msg, State) ->
    State;
process_join_msg({join, get_number_of_samples, _Samples, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, get_candidate_response, _OrigJoinId, _Candidate, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, join_response, Succ, Pred, MoveFullId, CandId, _TargetId, _NextOp} = _Msg, State) ->
    reject_join_response(Succ, Pred, MoveFullId, CandId),
    State;
process_join_msg({join, join_response, Reason, _CandId} = _Msg, State)
  when (Reason =:= not_responsible orelse Reason =:= busy) ->
    State;
process_join_msg({join, lookup_timeout, _Conn, _Id, _JoinId} = _Msg, State) ->
    State;
process_join_msg({join, known_hosts_timeout, _JoinId} = _Msg, State) ->
    State;
process_join_msg({join, get_number_of_samples_timeout, _Conn, _JoinId} = _Msg, State) ->
    State;
process_join_msg({join, join_request_timeout, _Timeouts, _CandId, _JoinId} = _Msg, State) ->
    State;
process_join_msg({join, {send_error, _Target, _Msg, _Reason}, _FailedPids}, State) ->
    State;
% messages send by the passive load balancing module
% -> forward to the module
process_join_msg({join, LbPsv, Msg, LbPsvState} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    call_lb_psv(LbPsv, process_join_msg, [Msg, LbPsvState, State]);
process_join_msg({join, timeout, _JoinId} = _Msg, State) ->
    State.

-spec call_lb_psv(LbPsv::term(), Function::atom(), Parameters::[term()]) -> term().
call_lb_psv(LbPsv, Function, Parameters)
  when is_atom(Function) andalso is_list(Parameters) ->
    Module =
    case lists:member(LbPsv, ?VALID_PASSIVE_ALGORITHMS) of
        true -> LbPsv;
        _    -> MyLbPsv = config:read(join_lb_psv),
                log:log(error, "[ Node ~.0p ] unknown passive load balancing "
                               "algorithm requested: ~.0p - using ~.0p instead",
                        [LbPsv, MyLbPsv]),
                MyLbPsv
    end,
    erlang:apply(Module, Function, Parameters).

%% @doc Contacts all nodes set in the known_hosts config parameter and request
%%      a list of dht_node instances in their VMs.
-spec get_known_nodes(JoinUUId::pos_integer()) -> ok.
get_known_nodes(JoinUUId) ->
    KnownHosts = config:read(known_hosts),
    % contact a subset of at most 3 random known VMs
    Self = comm:reply_as(self(), 2, {join, '_', []}),
    _ = [begin
            ?TRACE_SEND(Host, {get_dht_nodes, comm:this()}),
            comm:send(Host, {get_dht_nodes, comm:this()}, [{shepherd, Self}])
         end || Host <- util:random_subset(3, KnownHosts)],
    % also try to get some nodes from the current erlang VM:
    OwnServicePerVm = whereis(service_per_vm),
    ?TRACE_SEND(OwnServicePerVm, {get_dht_nodes, comm:this()}),
    comm:send_local(OwnServicePerVm, {get_dht_nodes, comm:this()}),
    % timeout just in case
    msg_delay:send_local(get_known_hosts_timeout() div 1000, self(),
                         {join, known_hosts_timeout, JoinUUId}).

-spec phase2_next_step(JoinState::phase2(), Connections::[connection()])
        -> phase_2_4().
phase2_next_step(JoinState, Connections) ->
    case skip_psv_lb(JoinState) of
        true -> % skip phase2b (use only the given ID)
            % (re-)issue lookups for all existing IDs and make sure there is at
            % least one id (the chosen Id may have been removed due to a
            % collision, for example)
            lookup_new_ids2(1, JoinState);
        _    -> get_number_of_samples(JoinState, Connections)
    end.

%% @doc Calls get_number_of_samples/1 on the configured passive load balancing
%%      algorithm if there is a contact node in Connections and then adds them
%%      to the list of connections.
-spec get_number_of_samples
        (Phase, Connections::[]) -> Phase when is_subtype(Phase, phase_2_4());
        (phase_2_4(), Connections::[connection(),...]) -> phase2b().
get_number_of_samples(JoinState, []) ->
    % do not act here - wait other responses - a known_hosts_timeout will
    % occur if something is wrong
    JoinState;
get_number_of_samples(JoinState, [Conn | _]) ->
    NewConn = new_connection(Conn),
    LbPsv = get_lb_psv(JoinState),
    LbPsv:get_number_of_samples(NewConn),
    msg_delay:send_local(
      get_number_of_samples_timeout() div 1000, self(),
      {join, get_number_of_samples_timeout, NewConn, get_join_uuid(JoinState)}),
    JoinState1 = update_connection(Conn, NewConn, JoinState),
    set_phase(phase2b, JoinState1).

%% @doc Select Count new (random) IDs and issue lookups.
-spec lookup_new_ids1
        (Count::pos_integer(), JoinState::phase2() | phase2b() | phase3())
            -> phase2() | phase3();
        (Count::pos_integer(), JoinState::phase4()) -> phase4().
lookup_new_ids1(Count, JoinState) ->
    OldIds = get_join_ids(JoinState),
    {NewJoinIds, OnlyNewJoinIds} = create_join_ids(Count, OldIds),
    JoinState1 = set_join_ids(NewJoinIds, JoinState),
    % try to contact Ids not in Candidates even if they have already been contacted
    lookup(JoinState1, OnlyNewJoinIds).

%% @doc Select as many new (random) IDs as needed to create (at least)
%%      TotalCount candidates and issue lookups for all existing as well as the
%%      new IDs.
%%      Note: Existing IDs are not removed.
%%      Note: If no join IDs remain, the next best candidate will be contacted!
-spec lookup_new_ids2(TotalCount::pos_integer(), JoinState::phase2() | phase2b())
        -> phase_2_4().
lookup_new_ids2(TotalCount, JoinState) ->
    % (re-)issue lookups for all existing IDs
    JoinState2 = lookup(JoinState),
    OldIds = get_join_ids(JoinState2),
    CurCount = length(OldIds) + length(get_candidates(JoinState2)),
    NewIdsCount = erlang:max(TotalCount - CurCount, 0),
    case NewIdsCount of
        0 ->
            % if there are no join IDs left, then there will be no candidate response
            % -> contact best candidate
            case get_join_ids(JoinState2) of
                []    -> contact_best_candidate(JoinState2);
                [_|_] -> JoinState2
            end;
        _ -> lookup_new_ids1(NewIdsCount, JoinState2)
    end.

%% @doc Creates Count new (unique) additional IDs.
-spec create_join_ids(Count::pos_integer(), OldIds::[?RT:key()])
        -> {AllKeys::[?RT:key(),...], OnlyNewKeys::[?RT:key(),...]}.
create_join_ids(Count, OldIds) ->
    OldIdsSet = gb_sets:from_list(OldIds),
    NewIdsSet = create_join_ids_helper(Count + gb_sets:size(OldIdsSet), OldIdsSet),
    OnlyNewIdsSet = gb_sets:subtract(NewIdsSet, OldIdsSet),
    {gb_sets:to_list(NewIdsSet), gb_sets:to_list(OnlyNewIdsSet)}.

%% @doc Helper for create_join_ids/2 that creates the new unique IDs.
-spec create_join_ids_helper(TotalCount::pos_integer(), gb_sets:set(?RT:key())) -> gb_sets:set(?RT:key()).
create_join_ids_helper(TotalCount, Ids) ->
    case gb_sets:size(Ids) of
        TotalCount -> Ids;
        _          -> create_join_ids_helper(TotalCount, gb_sets:add(?RT:get_random_node_id(), Ids))
    end.

%% @doc Tries to do a lookup for all join IDs in JoinState by contacting the
%%      first node among the Connections, then go to phase 3. If there is no
%%      node to contact, try to get new contact nodes and continue in phase 2.
%%      A node that has been contacted will be put at the end of the
%%      Connections list.
%%      Note: the returned join state will stay in phase 4 if already in phase 4
-spec lookup(phase2() | phase2b() | phase3()) -> NewState::phase_2_4().
lookup(JoinState) ->
    case get_join_ids(JoinState) of
        [] ->
            ?TRACE("[ Node ~w ] tried to look up nodes with an empty list "
                   "of IDs, trying to contact best candidate...", [self()]),
            contact_best_candidate(JoinState);
        [_|_] = JoinIds -> lookup(JoinState, JoinIds)
    end.

%% @doc Like lookup/1 but only looks up the given JoinIds (if there are any).
%%      Note: the returned join state will stay in phase 4 if already in phase 4.
-spec lookup(phase2() | phase2b() | phase3(), JoinIds::[?RT:key(),...]) -> NewState::phase2() | phase3();
            (phase4(), JoinIds::[?RT:key(),...]) -> NewState::phase4().
lookup(JoinState, JoinIds = [_|_]) ->
    Phase = get_phase(JoinState),
    case get_connections(JoinState) of
        [] ->
            case Phase of
                phase4 ->
                    % can't do lookup here but we are already waiting for a
                    % join_response -> for now, try to get contact nodes
                    % (may be useful for a lookup in a later step)
                    get_known_nodes(get_join_uuid(JoinState)),
                    JoinState;
                phase2 ->
                    % do not immediately re-issue a request for known nodes
                    % -> wait for further replies until the known_hosts_timeout hits
                    log:log(warn, "[ Node ~w ] empty list of contact nodes, "
                                "waiting...", [self()]),
                    JoinState;
                _      ->
                    log:log(warn, "[ Node ~w ] no further nodes to contact, "
                                "trying to get new nodes...", [self()]),
                    get_known_nodes(get_join_uuid(JoinState)),
                    set_phase(phase2, JoinState)
            end;
        [{_, Node} = Conn | _] ->
            MyLbPsv = get_lb_psv(JoinState),
            % we do at least contact one node -> move this connection to the
            % end if it has been used before
            % otherwise new connections will be added later on:
            JoinState1 = remove_connection(Conn, JoinState),
            JoinState2 = case element(1, Conn) of
                             null -> JoinState1;
                             _    -> add_connections([Conn], JoinState1, back)
                         end,
            NewConns =
                [begin
                     NewConn = new_connection(Conn),
                     Msg = {?lookup_aux, Id, 0,
                            {join, get_candidate, comm:this(), Id, MyLbPsv, NewConn}},
                     ?TRACE_SEND(Node, Msg),
                     comm:send(Node, Msg),
                     msg_delay:send_local(
                       get_lookup_timeout() div 1000, self(),
                       {join, lookup_timeout, NewConn, Id, get_join_uuid(JoinState2)}),
                     NewConn
                 end
                 || Id <- JoinIds],
            JoinState3 = add_connections(NewConns, JoinState2, back),
            case Phase of
                phase4 -> JoinState3;
                _      -> set_phase(phase3, JoinState3)
            end
    end.

%% @doc Integrates the Candidate into the join state if is is valid, i.e. a
%%      slide with an ID not equal to the node to join at.
%%      Note: a new ID will be sampled if the candidate is invalid.
-spec integrate_candidate
        (Candidate::lb_op:lb_op(), JoinState::phase2() | phase2b() | phase3(), Position::front | back)
            -> phase2() | phase2b() | phase3();
        (Candidate::lb_op:lb_op(), JoinState::phase4(), Position::front | back)
            -> phase4().
integrate_candidate(Candidate, JoinState, Position) ->
    % the other node could suggest a no_op -> select a new ID
    case lb_op:is_slide(Candidate) of
        false ->
            log:log(warn, "[ Node ~w ] got no_op candidate, "
                        "looking up a new ID...", [self()]),
            lookup_new_ids1(1, JoinState);
        _ ->
            % note: can not use an existing ID! the other node should not have
            % replied with such an ID though...
            NewIdCand = node_details:get(lb_op:get(Candidate, n1_new), new_key),
            SuccIdCand = node:id(node_details:get(lb_op:get(Candidate, n1succ_new), node)),
            case NewIdCand =:= SuccIdCand of
                true -> % the other node should not have send such an operation!
                    log:log(warn, "[ Node ~w ] chosen ID already exists, trying a "
                                "new ID", [self()]),
                    lookup_new_ids1(1, JoinState);
                _ when Position =:= front ->
                    add_candidate_front(Candidate, JoinState);
                _ when Position =:= back ->
                    add_candidate_back(Candidate, JoinState)
            end
    end.

%% userdevguide-begin dht_node_join:contact_best_candidate
%% @doc Contacts the best candidate among all stored candidates and sends a
%%      join_request (Timeouts = 0).
-spec contact_best_candidate(JoinState::phase_2_4())
        -> phase2() | phase2b() | phase4().
contact_best_candidate(JoinState) ->
    JoinState1 = sort_candidates(JoinState),
    send_join_request(JoinState1, 0).
%% userdevguide-end dht_node_join:contact_best_candidate

%% userdevguide-begin dht_node_join:send_join_request
%% @doc Sends a join request to the first candidate. Timeouts is the number of
%%      join_request_timeout messages previously received.
%%      PreCond: the id has been set to the ID to join at and has been updated
%%               in JoinState.
-spec send_join_request(JoinState::phase_2_4(), Timeouts::non_neg_integer())
        -> phase2() | phase2b() | phase4().
send_join_request(JoinState, Timeouts) ->
    case get_candidates(JoinState) of
        [] -> % no candidates -> start over (can happen, e.g. when join candidates are busy):
            start_over(JoinState);
        [BestCand | _] ->
            Id = node_details:get(lb_op:get(BestCand, n1_new), new_key),
            IdVersion = get_id_version(JoinState),
            NewSucc = node_details:get(lb_op:get(BestCand, n1succ_new), node),
            Me = node:new(comm:this(), Id, IdVersion),
            CandId = lb_op:get(BestCand, id),
            MyMTE = case dht_node_move:use_incremental_slides() of
                        true -> dht_node_move:get_max_transport_entries();
                        false -> unknown
                    end,
            Msg = {join, join_request, Me, CandId, MyMTE},
            ?TRACE_SEND(node:pidX(NewSucc), Msg),
            comm:send(node:pidX(NewSucc), Msg),
            msg_delay:send_local(
              get_join_request_timeout() div 1000, self(),
              {join, join_request_timeout, Timeouts, CandId, get_join_uuid(JoinState)}),
            set_phase(phase4, JoinState)
    end.
%% userdevguide-end dht_node_join:send_join_request

%% userdevguide-begin dht_node_join:start_over
%% @doc Goes back to phase 2 or 2b depending on whether contact nodes are
%%      available or not.
-spec start_over(JoinState::phase_2_4()) -> phase2() | phase2b().
start_over(JoinState) ->
    JoinState1 = set_new_join_uuid(JoinState),
    msg_delay:send_local(get_join_timeout() div 1000, self(),
                         {join, timeout, get_join_uuid(JoinState1)}),
    case get_connections(JoinState1) of
        [] ->
            get_known_nodes(get_join_uuid(JoinState1)),
            set_phase(phase2, JoinState1);
        [_|_] = Connections ->
            JoinState2 = set_phase(phase2, JoinState1),
            phase2_next_step(JoinState2, Connections)
    end.
%% userdevguide-end dht_node_join:start_over

%% @doc Removes the candidate currently at the front and tries to contact
%%      the next candidate. Keeps the join ID from the front candidate if
%%      skip_psv_lb is set.
-spec try_next_candidate(JoinState::phase_2_4())
        -> phase2() | phase2b() | phase4().
try_next_candidate(JoinState) ->
    JoinState1 =
        case skip_psv_lb(JoinState) of
            true -> remove_candidate_front_keep_id(JoinState);
            _    -> remove_candidate_front(JoinState)
        end,
    contact_best_candidate(JoinState1).

%% userdevguide-begin dht_node_join:finish_join
%% @doc Finishes the join and sends all queued messages.
-spec finish_join(Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type(), DB::db_dht:db(),
                  QueuedMessages::msg_queue:msg_queue(),
                  JoinOptions::[tuple()])
        -> dht_node_state:state().
finish_join(Me, Pred, Succ, DB, QueuedMessages, JoinOptions) ->
    %% get old rt loop subscribtion table (if available)
    MoveState = proplists:get_value(move_state, JoinOptions, []),
    OldSubscrTable = proplists:get_value(subscr_table, MoveState, null),
    RMState = rm_loop:init(Me, Pred, Succ, OldSubscrTable),
    Neighbors = rm_loop:get_neighbors(RMState),
    % wait for the ring maintenance to initialize and tell us its table ID
    rt_loop:activate(Neighbors),
    if MoveState =:= [] ->
           dc_clustering:activate(),
           gossip:activate(Neighbors);
       true -> ok
    end,
    dht_node_reregister:activate(),
    msg_queue:send(QueuedMessages),
    NewRT_ext = ?RT:empty_ext(Neighbors),
    service_per_vm:register_dht_node(node:pidX(Me)),
    dht_node_state:new(NewRT_ext, RMState, DB).

-spec reject_join_response(Succ::node:node_type(), Pred::node:node_type(),
                           MoveFullId::slide_op:id(), CandId::lb_op:id()) -> ok.
reject_join_response(Succ, _Pred, MoveId, _CandId) ->
    % similar to dht_node_move:abort_slide/9 - keep message in sync!
    Msg = {move, slide_abort, pred, MoveId, ongoing_slide},
    ?TRACE_SEND(node:pidX(Succ), Msg),
    dht_node_move:send_no_slide(node:pidX(Succ), Msg, 0).

%% @doc Finishes the join by setting up a slide operation to get the data from
%%      the other node and sends all queued messages.
-spec finish_join_and_slide(Me::node:node_type(), Pred::node:node_type(),
                            Succ::node:node_type(), DB::db_dht:db(),
                            QueuedMessages::msg_queue:msg_queue(),
                            MoveId::slide_op:id(), NextOp::slide_op:next_op(),
                            JoinOptions::[tuple()])
        -> {'$gen_component', [{on_handler, Handler::gen_component:handler()}],
            State::dht_node_state:state()}.
finish_join_and_slide(Me, Pred, Succ, DB, QueuedMessages, MoveId, NextOp, JoinOptions) ->
    State = finish_join(Me, Pred, Succ, DB, QueuedMessages, JoinOptions),
    {SourcePid, Tag} =
        case lists:keyfind(jump, 1, JoinOptions) of
            {jump, JumpTag, Pid} -> {Pid, JumpTag};
            _ -> {null, join}
        end,
    State1 = dht_node_move:exec_setup_slide_not_found(
               {ok, {join, 'rcv'}}, State, MoveId, Succ, node:id(Me), Tag,
               unknown, SourcePid, nomsg, NextOp, false),
    gen_component:change_handler(State1, fun dht_node:on/2).
%% userdevguide-end dht_node_join:finish_join

% getter:
-spec get_phase(phase_2_4()) -> phase2 | phase2b | phase3 | phase4.
get_phase(JoinState)         -> element(1, JoinState).
-spec get_join_uuid(phase_2_4()) -> pos_integer().
get_join_uuid(JoinState)     -> element(2, JoinState).
-spec get_join_options(phase_2_4()) -> [tuple()].
get_join_options(JoinState) -> element(3, JoinState).
-spec get_id_version(phase_2_4()) -> non_neg_integer().
get_id_version(JoinState)    -> element(4, JoinState).
-spec get_connections(phase_2_4()) -> [connection()].
get_connections(JoinState)   -> element(5, JoinState).
-spec get_join_ids(phase_2_4()) -> [?RT:key()].
get_join_ids(JoinState)      -> element(6, JoinState).
-spec get_candidates(phase_2_4()) -> [lb_op:lb_op()].
get_candidates(JoinState)    -> element(7, JoinState).

% setter:
-spec set_phase(phase2, phase_2_4()) -> phase2();
               (phase2b, phase_2_4()) -> phase2b();
               (phase3, phase_2_4()) -> phase3();
               (phase4, phase_2_4()) -> phase4().
set_phase(Phase, JoinState) -> setelement(1, JoinState, Phase).
-spec set_new_join_uuid(Phase) -> Phase when is_subtype(Phase, phase_2_4()).
set_new_join_uuid(JoinState) -> setelement(2, JoinState, uid:get_pids_uid()).
-spec set_join_ids(JoinIds::[?RT:key()], phase_2_4()) -> phase_2_4().
set_join_ids(JoinIds, JoinState) -> setelement(6, JoinState, JoinIds).
-spec remove_join_id(JoinId::?RT:key(), phase_2_4()) -> phase_2_4().
remove_join_id(JoinIdToRemove, {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections,
     [Id || Id <- JoinIds, Id =/= JoinIdToRemove], Candidates}.
-spec add_candidate_front(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_front(Candidate, {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    % filter previous candidates with the same ID (only use the newest one)
    CandId = lb_op:get(Candidate, id),
    Candidates1 = [C || C <- Candidates, lb_op:get(C, id) =/= CandId],
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, [Candidate | Candidates1]}.
-spec add_candidate_back(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_back(Candidate, {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, lists:append(Candidates, [Candidate])}.
-spec sort_candidates(phase_2_4()) -> phase_2_4().
sort_candidates({Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates} = JoinState) ->
    LbPsv = get_lb_psv(JoinState),
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, LbPsv:sort_candidates(Candidates)}.
-spec remove_candidate(CandId::lb_op:id(), phase_2_4()) -> phase_2_4().
remove_candidate(CandId, {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    NewCandidates = [C || C <- Candidates, lb_op:get(C, id) =/= CandId],
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, NewCandidates}.
-spec remove_candidate_front(phase_2_4()) -> phase_2_4().
remove_candidate_front({Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, []}) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, []};
remove_candidate_front({Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, [_ | Candidates]}) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}.
-spec remove_candidate_front_keep_id(phase_2_4()) -> phase_2_4().
remove_candidate_front_keep_id({Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, []}) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, []};
remove_candidate_front_keep_id({Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, [Front | Candidates]}) ->
    IdFront = node_details:get(lb_op:get(Front, n1_new), new_key),
    {Phase, JoinUUId, Options, CurIdVersion, Connections, [IdFront | JoinIds], Candidates}.
-spec skip_psv_lb(phase_2_4()) -> boolean().
skip_psv_lb({_Phase, _JoinUUId, Options, _CurIdVersion, _Connections, _JoinIds, _Candidates}) ->
    lists:member({skip_psv_lb}, Options).

-spec new_connection(OldConn::{null | pos_integer(), comm:mypid()})
        -> connection().
new_connection({_, Node}) ->
    {uid:get_pids_uid(), Node}.

-spec update_connection(
        OldConn::{null | pos_integer(), comm:mypid()},
        NewConn::{null | pos_integer(), comm:mypid()}, Phase)
            -> Phase when is_subtype(Phase, phase_2_4()).
update_connection({null, _} = OldConn, NewConn, JoinState) ->
    JoinState1 = remove_connection(OldConn, JoinState),
    add_connections([NewConn], JoinState1, back);
update_connection(OldConn, NewConn, JoinState) ->
    JoinState1 = remove_connection(OldConn, JoinState),
    add_connections([OldConn, NewConn], JoinState1, back).

-spec reset_connection(OldConn::{null | pos_integer(), comm:mypid()}, JoinState)
        -> JoinState when is_subtype(JoinState, phase_2_4()).
reset_connection({_, Node} = OldConn, JoinState) ->
    JoinState1 = remove_connection(OldConn, JoinState),
    add_connections([{null, Node}], JoinState1, front).

-spec add_connections([connection()], JoinState, front | back) -> JoinState when is_subtype(JoinState, phase_2_4()).
add_connections([], JoinState, _Pos) -> JoinState;
add_connections([CNHead], {Phase, JoinUUId, Options, CurIdVersion, [CNHead | _] = Connections, JoinIds, Candidates}, _Pos) ->
    {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates};
add_connections(Nodes = [_|_], {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}, back) ->
    {Phase, JoinUUId, Options, CurIdVersion, lists:append(Connections, Nodes), JoinIds, Candidates};
add_connections(Nodes = [_|_], {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}, front) ->
    {Phase, JoinUUId, Options, CurIdVersion, lists:append(Nodes, Connections), JoinIds, Candidates}.

-spec remove_connection(connection(), JoinState) -> JoinState when is_subtype(JoinState, phase_2_4()).
remove_connection(Conn, {Phase, JoinUUId, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    {Phase, JoinUUId, Options, CurIdVersion, [C || C <- Connections, C =/= Conn],
     JoinIds, Candidates}.

%% @doc Checks whether config parameters of the dht_node process during join
%%      exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(join_request_timeout) and
    config:cfg_is_greater_than_equal(join_request_timeout, 1000) and

    config:cfg_is_integer(join_request_timeouts) and
    config:cfg_is_greater_than_equal(join_request_timeouts, 1) and

    config:cfg_is_integer(join_lookup_timeout) and
    config:cfg_is_greater_than_equal(join_lookup_timeout, 1000) and

    config:cfg_is_integer(join_known_hosts_timeout) and
    config:cfg_is_greater_than_equal(join_known_hosts_timeout, 1000) and

    config:cfg_is_integer(join_timeout) and
    config:cfg_is_greater_than_equal(join_timeout, 1000) and

    config:cfg_is_integer(join_get_number_of_samples_timeout) and
    config:cfg_is_greater_than_equal(join_get_number_of_samples_timeout, 1000) and

    config:cfg_is_module(join_lb_psv).

%% @doc Gets the max number of ms to wait for a scalaris node to reply on a
%%      join request (set in the config files).
-spec get_join_request_timeout() -> pos_integer().
get_join_request_timeout() ->
    config:read(join_request_timeout).

%% @doc Gets the max number of join_request_timeouts before a candidate is
%%      removed and the next one is taken (set in the config files).
-spec get_join_request_timeouts() -> pos_integer().
get_join_request_timeouts() ->
    config:read(join_request_timeouts).

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

%% @doc Gets the max number of ms to wait for a join to be completed
%%      (set in the config files).
-spec get_number_of_samples_timeout() -> pos_integer().
get_number_of_samples_timeout() ->
    config:read(join_get_number_of_samples_timeout).

%% @doc Gets the passive load balancing algorithm to use for the given
%%      JoinState (set in the config files). If skip_psv_lb is set,
%%      lb_psv_simple will be used independent from the module set in the
%%      config.
-spec get_lb_psv(JoinState::phase_2_4()) -> module().
get_lb_psv(JoinState) ->
    case skip_psv_lb(JoinState) of
        true -> lb_psv_simple;
        _    -> config:read(join_lb_psv)
    end.
