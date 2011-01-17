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

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State), ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

-export([join_as_first/3, join_as_other/3,
         process_join_state/2, process_join_msg/2, check_config/0]).

-ifdef(with_export_type_support).
-export_type([join_state/0, join_message/0, connection/0]).
-endif.

-include("scalaris.hrl").

-type(join_message() ::
    % messages at the joining node:
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {join, get_number_of_samples, Samples::non_neg_integer(), Conn::connection()} |
    {join, get_candidate_response, OrigJoinId::?RT:key(), Candidate::lb_op:lb_op(), Conn::connection()} |
    {join, join_response, Succ::node:node_type(), Pred::node:node_type(), MoveFullId::slide_op:id(), CandId::lb_op:id()} |
    {join, join_response, not_responsible, CandId::lb_op:id()} |
    {join, lookup_timeout, Conn::connection()} |
    {join, known_hosts_timeout} |
    {join, get_number_of_samples_timeout, Conn::connection()} |
    {join, join_request_timeout, Timeouts::non_neg_integer(), CandId::lb_op:id()} |
    {join, timeout} |
    % messages at the existing node:
    {join, number_of_samples_request, SourcePid::comm:mypid(), LbPsv::module(), Conn::connection()} |
    {join, get_candidate, Source_PID::comm:mypid(), Key::?RT:key(), LbPsv::module(), Conn::connection()} |
    {join, join_request, NewPred::node:node_type(), CandId::lb_op:id()} |
    {join, join_response_timeout, NewPred::node:node_type(), MoveFullId::slide_op:id(), CandId::lb_op:id()} |
    {Msg::lb_psv_simple:custom_message() | lb_psv_split:custom_message() | 
          lb_psv_gossip:custom_message(),
     {join, LbPsv::module(), LbPsvState::term()}}
    ).

-type connection() :: {null | pos_integer(), comm:mypid()}.

-type phase2() ::
    {phase2,  Options::[tuple()], MyKeyVersion::non_neg_integer(),
     ContactNodes::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase2b() ::
    {phase2b, Options::[tuple()], MyKeyVersion::non_neg_integer(),
     ContactNodes::[{null | pos_integer(), comm:mypid()},...],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase3() ::
    {phase3,  Options::[tuple()], MyKeyVersion::non_neg_integer(),
     ContactNodes::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase4() ::
    {phase4,  Options::[tuple()], MyKeyVersion::non_neg_integer(),
     ContactNodes::[{null | pos_integer(), comm:mypid()}],
     JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase_2_4() :: phase2() | phase2b() | phase3() | phase4().

-type join_state() ::
    {join, phase_2_4(), QueuedMessages::msg_queue:msg_queue()}.

%% userdevguide-begin dht_node_join:join_as_first
-spec join_as_first(Id::?RT:key(), IdVersion::non_neg_integer(), Options::[tuple()])
        -> dht_node_state:state().
join_as_first(Id, IdVersion, _Options) ->
    % ugly hack to get a valid ip-address into the comm-layer
    dht_node:trigger_known_nodes(), 
    log:log(info, "[ Node ~w ] joining as first: (~.0p, ~.0p)",
            [self(), Id, IdVersion]),
    Me = node:new(comm:this(), Id, IdVersion),
    % join complete, State is the first "State"
    finish_join(Me, Me, Me, ?DB:new(), msg_queue:new()).
%% userdevguide-end dht_node_join:join_as_first

%% userdevguide-begin dht_node_join:join_as_other
-spec join_as_other(Id::?RT:key(), IdVersion::non_neg_integer(), Options::[tuple()])
        -> {join, phase2(), msg_queue:msg_queue()}.
join_as_other(Id, IdVersion, Options) ->
    log:log(info,"[ Node ~w ] joining, trying ID: (~.0p, ~.0p)",
            [self(), Id, IdVersion]),
    get_known_nodes(),
    msg_delay:send_local(get_join_timeout() div 1000, self(), {join, timeout}),
    {join, {phase2, Options, IdVersion, [], [Id], []}, msg_queue:new()}.
%% userdevguide-end dht_node_join:join_as_other

% join protocol
%% @doc Process a DHT node's join messages during the join phase.
-spec process_join_state(dht_node:message(), join_state()) -> dht_node_state:state().
% !first node
% 2. Find known hosts
% no matter which phase, if there are no contact nodes (yet), try to get some
process_join_state({join, known_hosts_timeout} = _Msg,
                   {join, JoinState, _QueuedMessages} = State) ->
    ?TRACE1(_Msg, {join, JoinState}),
    case get_connections(JoinState) of
        [] -> get_known_nodes();
        [_|_] -> ok
    end,
    State;

%% userdevguide-begin dht_node_join:join_other_p2
% in phase 2 add the nodes and do lookups with them / get number of samples
process_join_state({get_dht_nodes_response, Nodes} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 ->
    ?TRACE1(_Msg, {join, JoinState}),
    Connections = [{null, Node} || Node <- Nodes, Node =/= comm:this()],
    JoinState1 = add_connections(Connections, JoinState, back),
    NewJoinState = phase2_next_step(JoinState1, Connections),
    {join, NewJoinState, QueuedMessages};

% in all other phases, just add the provided nodes:
process_join_state({get_dht_nodes_response, Nodes} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2b orelse
           element(1, JoinState) =:= phase3 orelse
           element(1, JoinState) =:= phase4 ->
    ?TRACE1(_Msg, {join, JoinState}),
    Connections = [{null, Node} || Node <- Nodes, Node =/= comm:this()],
    JoinState1 = add_connections(Connections, JoinState, back),
    {join, JoinState1, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p2

% 2b. get the number of nodes/ids to sample

% on timeout:
%  - in phase 2b, remove failed connection, start over
%  - in other phases, ignore this timeout (but remove the connection)
process_join_state({join, get_number_of_samples_timeout, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2b ->
    ?TRACE1(_Msg, {join, JoinState}),
    JoinState1 = remove_connection(Conn, JoinState),
    case get_phase(JoinState1) of
        phase2b -> {join, start_over(JoinState1), QueuedMessages};
        _       -> {join, JoinState1, QueuedMessages}
    end;

%% userdevguide-begin dht_node_join:join_other_p2b
% note: although this message was send in phase2, also accept message in
% phase2, e.g. messages arriving from previous calls
process_join_state({join, get_number_of_samples, Samples, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b ->
    ?TRACE1(_Msg, {join, JoinState}),
    % prefer node that send get_number_of_samples as first contact node
    JoinState1 = reset_connection(Conn, JoinState),
    % (re-)issue lookups for all existing IDs
    JoinState2 = lookup(JoinState1),
    % then create additional samples, if required
    {join, lookup_new_ids2(Samples, JoinState2), QueuedMessages};

% ignore message arriving in other phases:
process_join_state({join, get_number_of_samples, _Samples, Conn} = _Msg,
                   {join, JoinState, QueuedMessages}) ->
    ?TRACE1(_Msg, {join, JoinState}),
    {join, reset_connection(Conn, JoinState), QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p2b

% 3. lookup all positions
process_join_state({join, lookup_timeout, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 ->
    ?TRACE1(_Msg, {join, JoinState}),
    % do not know whether the contact node is dead or the lookup takes too long
    % -> simple solution: try with an other contact node, remove the current one
    JoinState1 = remove_connection(Conn, JoinState),
    {join, lookup(JoinState1), QueuedMessages};

% only remove the failed connection in other phases:
process_join_state({join, lookup_timeout, Conn} = _Msg,
                   {join, JoinState, QueuedMessages}) ->
    ?TRACE1(_Msg, {join, JoinState}),
    {join, remove_connection(Conn, JoinState), QueuedMessages};

%% userdevguide-begin dht_node_join:join_other_p3
process_join_state({join, get_candidate_response, OrigJoinId, Candidate, Conn} = _Msg,
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 ->
    ?TRACE1(_Msg, {join, JoinState}),
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
    ?TRACE1(_Msg, {join, JoinState}),
    JoinState0 = reset_connection(Conn, JoinState),
    JoinState1 = remove_join_id(OrigJoinId, JoinState0),
    JoinState2 = case get_phase(JoinState1) of
                     phase4 -> integrate_candidate(Candidate, JoinState1, back);
                     _      -> integrate_candidate(Candidate, JoinState1, front)
                 end,
    {join, JoinState2, QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p3

% 4. joining my neighbor
process_join_state({join, join_request_timeout, Timeouts, CandId} = _Msg,
                   {join, JoinState, QueuedMessages} = State)
  when element(1, JoinState) =:= phase4 ->
    ?TRACE1(_Msg, {join, JoinState}),
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
                    {join, NewJoinState, QueuedMessages}
            end
    end;

% ignore message arriving in other phases:
process_join_state({join, join_request_timeout, _Timeouts, _CandId} = _Msg,
                   {join, _JoinState, _QueuedMessages} = State) ->
    ?TRACE1(_Msg, {join, _JoinState}),
    State;

%% userdevguide-begin dht_node_join:join_other_p4
process_join_state({join, join_response, not_responsible, CandId} = _Msg,
                   {join, JoinState, QueuedMessages} = State)
  when element(1, JoinState) =:= phase4 ->
    ?TRACE1(_Msg, {join, JoinState}),
    % the node we contacted is not responsible for the selected key anymore
    % -> try the next candidate, if the message is related to the current candidate
    case get_candidates(JoinState) of
        [] -> % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                        "starting over", [self()]),
            {join, start_over(JoinState), QueuedMessages};
        [Candidate | _Rest] ->
            case lb_op:get(Candidate, id) =:= CandId of
                false -> State; % unrelated/old message
                _ ->
                    log:log(info,
                            "[ Node ~w ] node contacted for join is not responsible "
                            "for the selected ID (anymore), trying next candidate",
                            [self()]),
                    {join, try_next_candidate(JoinState), QueuedMessages}
            end
    end;

% in other phases remove the candidate from the list (if it still exists):
process_join_state({join, join_response, not_responsible, CandId} = _Msg,
                   {join, JoinState, QueuedMessages}) ->
    ?TRACE1(_Msg, {join, JoinState}),
    {join, remove_candidate(CandId, JoinState), QueuedMessages};

% note: accept (delayed) join_response messages in any phase
process_join_state({join, join_response, Succ, Pred, MoveId, CandId} = _Msg,
                   {join, JoinState, QueuedMessages} = State) ->
    ?TRACE1(_Msg, {join, JoinState}),
    % only act on related messages, i.e. messages from the current candidate
    Phase = get_phase(JoinState),
    State1 = case get_candidates(JoinState) of
        [] when Phase =:= phase4 -> % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                           "starting over", [self()]),
            {join, start_over(JoinState), QueuedMessages};
        [] -> State; % in all other phases, ignore the delayed join_response
                     % if no candidates exist
        [Candidate | _Rest] ->
            CandidateNode = node_details:get(lb_op:get(Candidate, n1succ_new), node),
            CandidateNodeSame = node:same_process(CandidateNode, Succ),
            case lb_op:get(Candidate, id) =:= CandId of
                false ->
                    log:log(warn, "[ Node ~w ] ignoring old or unrelated "
                                  "join_response message", [self()]),
                    State; % ignore old/unrelated message
                _ when not CandidateNodeSame ->
                    % id is correct but the node is not (should never happen!)
                    log:log(error, "[ Node ~w ] got join_response but the node "
                                  "changed, trying next candidate", [self()]),
                    {join, try_next_candidate(JoinState), QueuedMessages};
                _ ->
                    MyId = node_details:get(lb_op:get(Candidate, n1_new), new_key),
                    MyIdVersion = get_id_version(JoinState),
                    case MyId =:= node:id(Succ) orelse MyId =:= node:id(Pred) of
                        true ->
                            log:log(warn, "[ Node ~w ] chosen ID already exists, "
                                          "trying next candidate", [self()]),
                            % note: can not keep Id, even if skip_psv_lb is set
                            JoinState1 = remove_candidate_front(JoinState),
                            {join, contact_best_candidate(JoinState1), QueuedMessages};
                        _ ->
                            ?TRACE("[ ~.0p ]~n  joined MyId:~.0p, MyIdVersion:~.0p~n  "
                                       "Succ: ~.0p~n  Pred: ~.0p~n",
                                       [self(), MyId, MyIdVersion, Succ, Pred]),
                            Me = node:new(comm:this(), MyId, MyIdVersion),
                            log:log(info, "[ Node ~w ] joined between ~w and ~w",
                                    [self(), Pred, Succ]),
                            rm_loop:notify_new_succ(node:pidX(Pred), Me),
                            rm_loop:notify_new_pred(node:pidX(Succ), Me),
                            
                            finish_join_and_slide(Me, Pred, Succ, ?DB:new(),
                                                  QueuedMessages, MoveId)
                    end
            end
    end,
    State1;
%% userdevguide-end dht_node_join:join_other_p4

% a join timeout message re-starts the complete join
process_join_state({join, timeout} = _Msg, {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b orelse
           element(1, JoinState) =:= phase3 orelse
           element(1, JoinState) =:= phase4 ->
    ?TRACE1(_Msg, {join, JoinState}),
    log:log(warn, "[ Node ~w ] join timeout hit, starting over...", [self()]),
    {join, start_over(JoinState), QueuedMessages};

process_join_state({web_debug_info, Requestor} = _Msg,
                   {join, JoinState, QueuedMessages} = State) ->
    ?TRACE1(_Msg, {join, JoinState}),
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", lists:flatten(io_lib:format("~p", [Message]))}
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
                 {"connections", lists:flatten(io_lib:format("~p", [get_connections(JoinState)]))},
                 {"join_ids",    lists:flatten(io_lib:format("~p", [get_join_ids(JoinState)]))},
                 {"candidates",  lists:flatten(io_lib:format("~p", [get_candidates(JoinState)]))}];
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
    
% Catch all other messages until the join procedure is complete
process_join_state(Msg, {join, JoinState, QueuedMessages}) ->
    ?TRACE1(Msg, {join, JoinState}),
    %log:log(info("[dhtnode] [~p] postponed delivery of ~p", [self(), Msg]),
    {join, JoinState, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Process requests from a joining node at a existing node:
-spec process_join_msg(join_message(), dht_node_state:state()) -> dht_node_state:state().
process_join_msg({join, number_of_samples_request, SourcePid, LbPsv, Conn} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    LbPsv:get_number_of_samples_remote(SourcePid, Conn),
    State;

%% userdevguide-begin dht_node_join:get_candidate
process_join_msg({join, get_candidate, Source_PID, Key, LbPsv, Conn} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    LbPsv:create_join(State, Key, Source_PID, Conn);
%% userdevguide-end dht_node_join:get_candidate

% messages send by the passive load balancing module
% -> forward to the module
process_join_msg({Msg, {join, LbPsv, LbPsvState}} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    LbPsv:process_join_msg(Msg, LbPsvState, State);

%% userdevguide-begin dht_node_join:join_request1
process_join_msg({join, join_request, NewPred, CandId} = _Msg, State)
  when (not is_atom(NewPred)) -> % avoid confusion with not_responsible message
    ?TRACE1(_Msg, State),
    TargetId = node:id(NewPred),
    % only reply to join request with keys in our range:
    KeyInRange = dht_node_state:is_responsible(node:id(NewPred), State),
    case KeyInRange andalso
             dht_node_move:can_slide_pred(State, TargetId, {join, 'rcv'}) of
        true ->
            % TODO: implement step-wise join
            MoveFullId = util:get_global_uid(),
            SlideOp = slide_op:new_sending_slide_join(
                        MoveFullId, NewPred, join, State),
            SlideOp1 = slide_op:set_phase(SlideOp, wait_for_pred_update_join),
            RMSubscrTag = {move, slide_op:get_id(SlideOp1)},
            rm_loop:subscribe(self(), RMSubscrTag,
                              fun(_OldNeighbors, NewNeighbors) ->
                                      NewPred =:= nodelist:pred(NewNeighbors)
                              end,
                              fun dht_node_move:rm_notify_new_pred/4),
            State1 = dht_node_state:add_db_range(
                       State, slide_op:get_interval(SlideOp1)),
            send_join_response(State1, SlideOp1, NewPred, CandId);
        _ when not KeyInRange ->
            ?TRACE_SEND(node:pidX(NewPred),
                        {join, join_response, not_responsible, CandId}),
            comm:send(node:pidX(NewPred),
                      {join, join_response, not_responsible, CandId}),
            State;
        _ ->
            ?TRACE("[ ~.0p ]~n  ignoring join_request from ~.0p due to a running slide~n",
                   [self(), NewPred]),
            State
    end;
%% userdevguide-end dht_node_join:join_request1

process_join_msg({join, join_response_timeout, NewPred, MoveFullId, CandId} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % almost the same as dht_node_move:safe_operation/5 but we tolerate wrong pred:
    case dht_node_state:get_slide_op(State, MoveFullId) of
        {pred, SlideOp} ->
            ResponseReceived =
                lists:member(slide_op:get_phase(SlideOp),
                             [wait_for_req_data, wait_for_pred_update_join]),
            case (slide_op:get_timeouts(SlideOp) < get_join_response_timeouts()) of
                _ when ResponseReceived -> State;
                true ->
                    NewSlideOp = slide_op:inc_timeouts(SlideOp),
                    send_join_response(State, NewSlideOp, NewPred, CandId);
                _ ->
                    % abort the slide operation set up for the join:
                    % (similar to dht_node_move:abort_slide/*)
                    log:log(warn, "abort_join(op: ~p, reason: timeout)~n",
                            [SlideOp]),
                    _ = slide_op:reset_timer(SlideOp), % reset previous timeouts
                    RMSubscrTag = {move, slide_op:get_id(SlideOp)},
                    rm_loop:unsubscribe(self(), RMSubscrTag),
                    State1 = dht_node_state:rm_db_range(
                               State, slide_op:get_interval(SlideOp)),
                    dht_node_state:set_slide(State1, pred, null)
            end;
        not_found -> State
    end;

% only messages with the first element being "join" are processed here
% -> see dht_node.erl
process_join_msg({get_dht_nodes_response, _Nodes} = _Msg, State) ->
    State;
process_join_msg({join, get_number_of_samples, _Samples, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, get_candidate_response, _OrigJoinId, _Candidate, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, join_response, _Succ, _Pred, _MoveFullId, _CandId} = _Msg, State) ->
    State;
process_join_msg({join, join_response, not_responsible, _CandId} = _Msg, State) ->
    State;
process_join_msg({join, lookup_timeout, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, known_hosts_timeout} = _Msg, State) ->
    State;
process_join_msg({join, get_number_of_samples_timeout, _Conn} = _Msg, State) ->
    State;
process_join_msg({join, join_request_timeout, _Timeouts, _CandId} = _Msg, State) ->
    State;
process_join_msg({join, timeout} = _Msg, State) ->
    State.

%% @doc Contacts all nodes set in the known_hosts config parameter and request
%%      a list of dht_node instances in their VMs.
-spec get_known_nodes() -> ok.
get_known_nodes() ->
    KnownHosts = config:read(known_hosts),
    % contact all known VMs
    _ = [begin
            ?TRACE_SEND(Host, {get_dht_nodes, comm:this()}),
            comm:send(Host, {get_dht_nodes, comm:this()})
         end || Host <- KnownHosts],
    % timeout just in case
    msg_delay:send_local(get_known_hosts_timeout() div 1000, self(),
                         {join, known_hosts_timeout}).

-spec phase2_next_step(JoinState::phase2(), Connections::[connection()])
        -> phase2() | phase2b() | phase3().
phase2_next_step(JoinState, Connections) ->
    case skip_psv_lb(JoinState) of
        true -> % skip phase2b (use only the given ID)
            % (re-)issue lookups for all existing IDs
            JoinState2 = lookup(JoinState),
            % the chosen Id may have been removed due to a collision, for example
            % -> make sure there is at least one id:
            lookup_new_ids2(1, JoinState2);
        _    -> get_number_of_samples(JoinState, Connections)
    end.

%% @doc Calls get_number_of_samples/1 on the configured passive load balancing
%%      algorithm if there is a contact node in ContactNodes and then adds them
%%      to the list of contact nodes.
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
    msg_delay:send_local(get_number_of_samples_timeout() div 1000,
                         self(), {join, get_number_of_samples_timeout, NewConn}),
    JoinState1 = update_connection(Conn, NewConn, JoinState),
    set_phase(phase2b, JoinState1).

%% @doc Select Count new (random) IDs and issue lookups.
-spec lookup_new_ids1
        (Count::non_neg_integer(), JoinState::phase2() | phase2b() | phase3())
            -> phase2() | phase3();
        (Count::non_neg_integer(), JoinState::phase4()) -> phase4().
lookup_new_ids1(Count, JoinState) ->
    OldIds = get_join_ids(JoinState),
    {NewJoinIds, OnlyNewJoinIds} = create_join_ids(Count, OldIds),
    JoinState1 = set_join_ids(NewJoinIds, JoinState),
    % try to contact Ids not in Candidates even if they have already been contacted
    lookup(JoinState1, OnlyNewJoinIds).

%% @doc Select as many new (random) IDs as needed to create (at least)
%%      TotalCount candidates and issue lookups.
%%      Note: existing IDs are not removed.
-spec lookup_new_ids2(TotalCount::pos_integer(), JoinState::phase2() | phase2b())
        -> phase2() | phase3().
lookup_new_ids2(TotalCount, JoinState) ->
    OldIds = get_join_ids(JoinState),
    CurCount = length(OldIds) + length(get_candidates(JoinState)),
    NewIdsCount = erlang:max(TotalCount - CurCount, 0),
    lookup_new_ids1(NewIdsCount, JoinState).

%% @doc Creates Count new (unique) additional IDs.
-spec create_join_ids(Count::non_neg_integer(), OldIds::[?RT:key()]) -> {AllKeys::[?RT:key()], OnlyNewKeys::[?RT:key()]}.
create_join_ids(0, OldIds) -> {OldIds, []};
create_join_ids(Count, OldIds) ->
    OldIdsSet = gb_sets:from_list(OldIds),
    NewIdsSet = create_join_ids_helper(Count, OldIdsSet),
    OnlyNewIdsSet = gb_sets:subtract(NewIdsSet, OldIdsSet),
    {gb_sets:to_list(NewIdsSet), gb_sets:to_list(OnlyNewIdsSet)}.

%% @doc Helper for create_join_ids/2 that creates the new unique IDs.
-spec create_join_ids_helper(Count::pos_integer(), gb_set()) -> gb_set().
create_join_ids_helper(Count, Ids) ->
    case gb_sets:size(Ids) of
        Count -> Ids;
        _     -> create_join_ids_helper(Count, gb_sets:add(?RT:get_random_node_id(), Ids))
    end.

%% @doc Tries to do a lookup for all join IDs in JoinState by contacting the
%%      first node among the ContactNodes, then go to phase 3. If there is no
%%      node to contact, try to get new contact nodes and continue in phase 2.
%%      A node that has been contacted will be put at the end of the
%%      ContactNodes list.
%%      Note: the returned join state will stay in phase 4 if already in phase 4
-spec lookup(phase2() | phase2b() | phase3()) -> NewState::phase2() | phase3().
lookup(JoinState) ->
    lookup(JoinState, get_join_ids(JoinState)).

%% @doc Like lookup/1 but only looks up the given JoinIds (if there are any).
%%      Note: the returned join state will stay in phase 4 if already in phase 4.
-spec lookup(phase2() | phase2b() | phase3(), JoinIds::[?RT:key()]) -> NewState::phase2() | phase3();
            (phase4(), JoinIds::[?RT:key()]) -> NewState::phase4().
lookup(JoinState, []) -> JoinState;
lookup(JoinState, JoinIds = [_|_]) ->
    Phase = get_phase(JoinState),
    case get_connections(JoinState) of
        [] ->
            case Phase of
                phase4 ->
                    % can't do lookup here but we are already waiting for a
                    % join_response -> for now, try to get contact nodes
                    % (may be useful for a lookup in a later step)
                    get_known_nodes(),
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
                    get_known_nodes(),
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
            NewConns = [begin
                            NewConn = new_connection(Conn),
                            Msg = {lookup_aux, Id, 0,
                                   {join, get_candidate, comm:this(), Id, MyLbPsv, NewConn}},
                            ?TRACE_SEND(Node, Msg),
                            comm:send(Node, Msg),
                            msg_delay:send_local(get_lookup_timeout() div 1000, self(),
                                                 {join, lookup_timeout, NewConn}),
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
    contact_best_candidate(JoinState, 0).
%% @doc Contacts the best candidate among all stored candidates and sends a
%%      join_request. Timeouts is the number of join_request_timeout messages
%%      previously received.
-spec contact_best_candidate(JoinState::phase_2_4(), Timeouts::non_neg_integer())
        -> phase2() | phase2b() | phase4().
contact_best_candidate(JoinState, Timeouts) ->
    JoinState1 = sort_candidates(JoinState),
    send_join_request(JoinState1, Timeouts).
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
        [] -> % no candidates -> start over (should not happen):
            start_over(JoinState);
        [BestCand | _] ->
            Id = node_details:get(lb_op:get(BestCand, n1_new), new_key),
            IdVersion = get_id_version(JoinState),
            NewSucc = node_details:get(lb_op:get(BestCand, n1succ_new), node),
            Me = node:new(comm:this(), Id, IdVersion),
            CandId = lb_op:get(BestCand, id),
            ?TRACE_SEND(node:pidX(NewSucc), {join, join_request, Me, CandId}),
            comm:send(node:pidX(NewSucc), {join, join_request, Me, CandId}),
            msg_delay:send_local(get_join_request_timeout() div 1000, self(),
                                 {join, join_request_timeout, Timeouts, CandId}),
            set_phase(phase4, JoinState)
    end.
%% userdevguide-end dht_node_join:send_join_request

%% userdevguide-begin dht_node_join:start_over
%% @doc Goes back to phase 2 or 2b depending on whether contact nodes are
%%      available or not.
-spec start_over(JoinState::phase_2_4()) -> phase2() | phase2b().
start_over(JoinState) ->
    case get_connections(JoinState) of
        [] ->
            get_known_nodes(),
            set_phase(phase2, JoinState);
        [_|_] = Connections ->
            JoinState1 = set_phase(phase2, JoinState),
            phase2_next_step(JoinState1, Connections)
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

%% @doc Sends a join response message to the new predecessor and sets the given
%%      slide operation in the dht_node state (adding a timeout to it as well).
%% userdevguide-begin dht_node_join:join_request2
-spec send_join_response(State::dht_node_state:state(),
                         NewSlideOp::slide_op:slide_op(),
                         NewPred::node:node_type(), CandId::lb_op:id())
        -> dht_node_state:state().
send_join_response(State, SlideOp, NewPred, CandId) ->
    MoveFullId = slide_op:get_id(SlideOp),
    NewSlideOp =
        slide_op:set_timer(SlideOp, get_join_response_timeout(),
                           {join, join_response_timeout, NewPred, MoveFullId, CandId}),
    MyOldPred = dht_node_state:get(State, pred),
    MyNode = dht_node_state:get(State, node),
    ?TRACE_SEND(node:pidX(NewPred),
                {join, join_response, MyNode, MyOldPred, MoveFullId, CandId}),
    comm:send(node:pidX(NewPred),
              {join, join_response, MyNode, MyOldPred, MoveFullId, CandId}),
    % no need to tell the ring maintenance -> the other node will trigger an update
    % also this is better in case the other node dies during the join
%%     rm_loop:notify_new_pred(comm:this(), NewPred),
    dht_node_state:set_slide(State, pred, NewSlideOp).
%% userdevguide-end dht_node_join:join_request2

%% userdevguide-begin dht_node_join:finish_join
-spec finish_join(Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type(), DB::?DB:db(),
                  QueuedMessages::msg_queue:msg_queue())
        -> dht_node_state:state().
finish_join(Me, Pred, Succ, DB, QueuedMessages) ->
    rm_loop:activate(Me, Pred, Succ),
    % wait for the ring maintenance to initialize and tell us its table ID
    NeighbTable = rm_loop:get_neighbors_table(),
    rt_loop:activate(NeighbTable),
    cyclon:activate(),
    vivaldi:activate(),
    dc_clustering:activate(),
    gossip:activate(),
    dht_node_reregister:activate(),
    msg_queue:send(QueuedMessages),
    NewRT_ext = ?RT:empty_ext(rm_loop:get_neighbors(NeighbTable)),
    dht_node_state:new(NewRT_ext, NeighbTable, DB).

-spec finish_join_and_slide(Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type(), DB::?DB:db(),
                  QueuedMessages::msg_queue:msg_queue(), MoveId::slide_op:id())
        -> dht_node_state:state().
finish_join_and_slide(Me, Pred, Succ, DB, QueuedMessages, MoveId) ->
    State = finish_join(Me, Pred, Succ, DB, QueuedMessages),
    SlideOp = slide_op:new_receiving_slide_join(MoveId, Pred, Succ, node:id(Me), join),
    SlideOp1 = slide_op:set_phase(SlideOp, wait_for_node_update),
    State1 = dht_node_state:set_slide(State, succ, SlideOp1),
    State2 = dht_node_state:add_msg_fwd(
               State1, slide_op:get_interval(SlideOp1),
               node:pidX(slide_op:get_node(SlideOp1))),
    RMSubscrTag = {move, slide_op:get_id(SlideOp1)},
    NewMsgQueue = msg_queue:add(QueuedMessages,
                                {move, node_update, RMSubscrTag}),
    msg_queue:send(NewMsgQueue),
    State2.
%% userdevguide-end dht_node_join:finish_join

% getter:
-spec get_phase(phase_2_4()) -> phase2 | phase2b | phase3 | phase4.
get_phase(JoinState)         -> element(1, JoinState).
-spec get_id_version(phase_2_4()) -> non_neg_integer().
get_id_version(JoinState)    -> element(3, JoinState).
-spec get_connections(phase_2_4()) -> [connection()].
get_connections(JoinState) -> element(4, JoinState).
-spec get_join_ids(phase_2_4()) -> [?RT:key()].
get_join_ids(JoinState)      -> element(5, JoinState).
-spec get_candidates(phase_2_4()) -> [lb_op:lb_op()].
get_candidates(JoinState)    -> element(6, JoinState).

% setter:
-spec set_phase(phase2, phase_2_4()) -> phase2();
               (phase2b, phase_2_4()) -> phase2b();
               (phase3, phase_2_4()) -> phase3();
               (phase4, phase_2_4()) -> phase4().
set_phase(Phase, JoinState) -> setelement(1, JoinState, Phase).
-spec set_join_ids(JoinIds::[?RT:key()], phase_2_4()) -> phase_2_4().
set_join_ids(JoinIds, JoinState) -> setelement(5, JoinState, JoinIds).
-spec remove_join_id(JoinId::?RT:key(), phase_2_4()) -> phase_2_4().
remove_join_id(JoinIdToRemove, {Phase, Options, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, Options, CurIdVersion, ContactNodes,
     [Id || Id <- JoinIds, Id =/= JoinIdToRemove], Candidates}.
-spec add_candidate_front(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_front(Candidate, {Phase, Options, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, [Candidate | Candidates]}.
-spec add_candidate_back(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_back(Candidate, {Phase, Options, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, lists:append(Candidates, [Candidate])}.
-spec sort_candidates(phase_2_4()) -> phase_2_4().
sort_candidates({Phase, Options, CurIdVersion, ContactNodes, JoinIds, Candidates} = JoinState) ->
    LbPsv = get_lb_psv(JoinState),
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, LbPsv:sort_candidates(Candidates)}.
-spec remove_candidate(CandId::lb_op:id(), phase_2_4()) -> phase_2_4().
remove_candidate(CandId, {Phase, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    NewCandidates = [C || C <- Candidates, lb_op:get(C, id) =/= CandId],
    {Phase, Options, CurIdVersion, Connections, JoinIds, NewCandidates}.
-spec remove_candidate_front(phase_2_4()) -> phase_2_4().
remove_candidate_front({Phase, Options, CurIdVersion, ContactNodes, JoinIds, []}) ->
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, []};
remove_candidate_front({Phase, Options, CurIdVersion, ContactNodes, JoinIds, [_ | Candidates]}) ->
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, Candidates}.
-spec remove_candidate_front_keep_id(phase_2_4()) -> phase_2_4().
remove_candidate_front_keep_id({Phase, Options, CurIdVersion, ContactNodes, JoinIds, []}) ->
    {Phase, Options, CurIdVersion, ContactNodes, JoinIds, []};
remove_candidate_front_keep_id({Phase, Options, CurIdVersion, ContactNodes, JoinIds, [Front | Candidates]}) ->
    IdFront = node_details:get(lb_op:get(Front, n1_new), new_key),
    {Phase, Options, CurIdVersion, ContactNodes, [IdFront | JoinIds], Candidates}.
-spec skip_psv_lb(phase_2_4()) -> boolean().
skip_psv_lb({_Phase, Options, _CurIdVersion, _ContactNodes, _JoinIds, _Candidates}) ->
    lists:member({skip_psv_lb}, Options).

-spec new_connection(OldConn::{null | pos_integer(), comm:mypid()})
        -> connection().
new_connection({_, Node}) ->
    {util:get_pids_uid(), Node}.

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
add_connections([CNHead], {Phase, Options, CurIdVersion, [CNHead | _] = Connections, JoinIds, Candidates}, _Pos) ->
    {Phase, Options, CurIdVersion, Connections, JoinIds, Candidates};
add_connections(Nodes = [_|_], {Phase, Options, CurIdVersion, Connections, JoinIds, Candidates}, back) ->
    {Phase, Options, CurIdVersion, lists:append(Connections, Nodes), JoinIds, Candidates};
add_connections(Nodes = [_|_], {Phase, Options, CurIdVersion, Connections, JoinIds, Candidates}, front) ->
    {Phase, Options, CurIdVersion, lists:append(Nodes, Connections), JoinIds, Candidates}.

-spec remove_connection(connection(), JoinState) -> JoinState when is_subtype(JoinState, phase_2_4()).
remove_connection(Conn, {Phase, Options, CurIdVersion, Connections, JoinIds, Candidates}) ->
    {Phase, Options, CurIdVersion, [C || C <- Connections, C =/= Conn],
     JoinIds, Candidates}.

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(join_response_timeout) and
    config:is_greater_than_equal(join_response_timeout, 1000) and

    config:is_integer(join_response_timeouts) and
    config:is_greater_than_equal(join_response_timeouts, 1) and

    config:is_integer(join_request_timeout) and
    config:is_greater_than_equal(join_request_timeout, 1000) and

    config:is_integer(join_request_timeouts) and
    config:is_greater_than_equal(join_request_timeouts, 1) and

    config:is_integer(join_lookup_timeout) and
    config:is_greater_than_equal(join_lookup_timeout, 1000) and

    config:is_integer(join_known_hosts_timeout) and
    config:is_greater_than_equal(join_known_hosts_timeout, 1000) and

    config:is_integer(join_timeout) and
    config:is_greater_than_equal(join_timeout, 1000) and

    config:is_integer(join_get_number_of_samples_timeout) and
    config:is_greater_than_equal(join_get_number_of_samples_timeout, 1000) and

    config:is_module(join_lb_psv).

%% @doc Gets the max number of ms to wait for a joining node's reply after
%%      it send a join request (set in the config files).
-spec get_join_response_timeout() -> pos_integer().
get_join_response_timeout() ->
    config:read(join_response_timeout).

%% @doc Gets the max number of join_response_timeouts before the slide is
%%      aborted (set in the config files).
-spec get_join_response_timeouts() -> pos_integer().
get_join_response_timeouts() ->
    config:read(join_response_timeouts).

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
