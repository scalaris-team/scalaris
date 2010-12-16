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
    % messages at the joining node:
    {idholder_get_id_response, Id::?RT:key(), IdVersion::non_neg_integer()} |
    {get_dht_nodes_response, Nodes::[node:node_type()]} |
    {join, get_number_of_samples, Samples::non_neg_integer(), Source::comm:mypid()} |
    {join, get_candidate_response, OrigJoinId::?RT:key(), Candidate::lb_op:lb_op()} |
    {join, join_response, Succ::node:node_type(), Pred::node:node_type(), MoveFullId::slide_op:id()} |
    {join, join_response, not_responsible, Node::node:node_type()} |
    {join, lookup_timeout, Node::comm:mypid()} |
    {join, known_hosts_timeout} |
    {join, get_number_of_samples_timeout} |
    {join, join_request_timeout, Timeouts::non_neg_integer()} |
    {join, timeout} |
    % messages at the existing node:
    {join, get_candidate, Source_PID::comm:mypid(), Key::?RT:key()} |
    {join, join_request, NewPred::node:node_type()} |
    {join, join_response_timeout, NewPred::node:node_type(), MoveFullId::slide_op:id()}
    ).

-type phase0() ::
    {as_first}.
-type phase1() ::
    {phase1}.
-type phase2() ::
    {phase2,  MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[comm:mypid()], JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase2b() ::
    {phase2b, MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[comm:mypid()], JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase3() ::
    {phase3,  MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[comm:mypid()], JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase3b() ::
    {phase3b,  MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[comm:mypid()], JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase4() ::
    {phase4,  MyKey::?RT:key(), MyKeyVersion::non_neg_integer(), ContactNodes::[comm:mypid()], JoinIds::[?RT:key()], Candidates::[lb_op:lb_op()]}.
-type phase_2_4() :: phase2() | phase2b() | phase3() | phase3b() | phase4().

-type join_state() ::
    {join, phase0() | phase1() | phase_2_4(), QueuedMessages::msg_queue:msg_queue()}.

% join protocol
%% @doc Process a DHT node's join messages during the join phase.
-spec process_join_state(dht_node:message(), join_state()) -> dht_node_state:state().
% first node
%% userdevguide-begin dht_node_join:join_first
process_join_state({idholder_get_id_response, Id, IdVersion},
                   {join, {as_first}, QueuedMessages}) ->
    log:log(info, "[ Node ~w ] joining as first: ~p",[self(), Id]),
    Me = node:new(comm:this(), Id, IdVersion),
    % join complete, State is the first "State"
    finish_join(Me, Me, Me, ?DB:new(), QueuedMessages);
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
    {join, {phase2, Id, IdVersion, [], [], []}, QueuedMessages};

% 2. Find known hosts
process_join_state({join, known_hosts_timeout},
                   {join, JoinState, _QueuedMessages} = State)
  when element(1, JoinState) =:= phase2 ->
    %io:format("p2: known hosts timeout~n"),
    get_known_nodes(),
    State;

% ignore message arriving in later phases: 
process_join_state({join, known_hosts_timeout}, State) -> State;

process_join_state({get_dht_nodes_response, Nodes},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b ->
    % note: collect nodes in the other phases, too (messages have been send anyway)
    %io:format("p2: got dht_nodes_response ~p~n", [lists:delete(comm:this(), Nodes)]),
    ContactNodes = [Node || Node <- Nodes, Node =/= comm:this()],
    {join, get_number_of_samples(JoinState, ContactNodes), QueuedMessages};

% in all other phases, just add the provided nodes:
% note: phase1 should never receive this message!
process_join_state({get_dht_nodes_response, Nodes},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 orelse
           element(1, JoinState) =:= phase3b orelse
           element(1, JoinState) =:= phase4 ->
    FurtherNodes = [Node || Node <- Nodes, Node =/= comm:this()],
    {join, add_contact_nodes_back(FurtherNodes, JoinState), QueuedMessages};

% 2b. get the number of nodes/ids to sample
process_join_state({join, get_number_of_samples_timeout},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2b ->
    %io:format("p2b: get number of samples timeout~n"),
    get_known_nodes(),
    {join, setelement(1, JoinState, phase2), QueuedMessages};

% ignore message arriving in later phases: 
process_join_state({join, get_number_of_samples_timeout}, State) ->
    State;

% note: although this message was send in phase2, also accept message in
% phase2, e.g. messages arriving from previous calls
process_join_state({join, get_number_of_samples, Samples, Source},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b ->
    %io:format("p2b: got get_number_of_samples ~p~n", [Samples]),
    % prefer node that send get_number_of_samples as first contact node
    JoinState1 = add_contact_nodes_front([Source], JoinState),
    {join, lookup_new_ids2(Samples, JoinState1), QueuedMessages};

% ignore message arriving in other phases:
process_join_state({join, get_number_of_samples, _Samples, _Source}, State) ->
    State;
%% userdevguide-end dht_node_join:join_other_p12

%% userdevguide-begin dht_node_join:join_other_p3
% 3. lookup all positions
process_join_state({join, lookup_timeout, Node},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 ->
    %io:format("p3: lookup_timeout~n"),
    % do not know whether the contact node is dead or the lookup takes too long
    % -> simple solution: try with an other contact node, remove the current one
    {join, lookup(remove_contact_node(Node, JoinState)), QueuedMessages};

% ignore message arriving in other phases:
process_join_state({join, lookup_timeout, _Node}, State) -> State;

process_join_state({join, get_candidate_response, OrigJoinId, Candidate},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3 ->
    %io:format("p3: lookup success~n"),
    JoinState1 = remove_join_id(OrigJoinId, JoinState),
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
process_join_state({join, get_candidate_response, OrigJoinId, Candidate},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b ->
    JoinState1 = remove_join_id(OrigJoinId, JoinState),
    JoinState2 = integrate_candidate(Candidate, JoinState1, front),
    {join, JoinState2, QueuedMessages};

% In phase 3b or 4, add the candidate to the end of the candidates as they are sorted
% and the join with the first has already started (use this candidate as backup
% if the join fails). Do not start a new join.
process_join_state({join, get_candidate_response, OrigJoinId, Candidate},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3b orelse
           element(1, JoinState) =:= phase4 ->
    JoinState1 = remove_join_id(OrigJoinId, JoinState),
    JoinState2 = integrate_candidate(Candidate, JoinState1, back),
    {join, JoinState2, QueuedMessages};

process_join_state({idholder_get_id_response, Id, IdVersion},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase3b->
    %io:format("p3b: got key~n"),
    JoinState1 = set_id(Id, IdVersion, JoinState),
    {join, send_join_request(JoinState1, 0), QueuedMessages};
%% userdevguide-end dht_node_join:join_other_p3

%% userdevguide-begin dht_node_join:join_other_p4
% 4. joining my neighbor
process_join_state({join, join_request_timeout, Timeouts},
                   {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase4 ->
    NewJoinState = 
        case Timeouts < get_join_request_timeouts() of
            true ->
                contact_best_candidate(JoinState, Timeouts + 1);
            _ ->
                % no response from responsible node -> select new candidate, try again
                log:log(warn, "[ Node ~w ] no response on join request for the "
                            "chosen ID ~w, trying next candidate",
                        [self(), get_id(JoinState)]),
                JoinState1 = remove_candidate_front(JoinState),
                contact_best_candidate(JoinState1)
        end,
    {join, NewJoinState, QueuedMessages};

% ignore message arriving in other phases:
process_join_state({join, join_request_timeout, _Timeouts}, State) -> State;

process_join_state({join, join_response, not_responsible, Node},
                   {join, JoinState, QueuedMessages} = State)
  when element(1, JoinState) =:= phase4 ->
    % the node we contacted is not responsible for our key (anymore)
    % -> try the next candidate, if the message is related to the current candidate
    case get_candidates(JoinState) of
        [] -> % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                        "starting over", [self()]),
            {join, start_over(JoinState), QueuedMessages};
        [Candidate | _Rest] ->
            CandidateNode = node_details:get(lb_op:get(Candidate, n1succ_new), node),
            case node:same_process(CandidateNode, Node) of
                false -> State; % ignore old/unrelated message
                _ ->
                    log:log(info,
                            "[ Node ~w ] node contacted for join is not responsible for "
                            "the selected ID (anymore) ~w, trying next candidate",
                            [self(), get_id(JoinState)]),
                    JoinState1 = remove_candidate_front(JoinState),
                    {join, contact_best_candidate(JoinState1), QueuedMessages}
            end
    end;

% ignore (delayed) message arriving in other phases:
process_join_state({join, join_response, not_responsible, _Node}, State) -> State;

% note: accept (delayed) join_response messages in any phase
process_join_state({join, join_response, Succ, Pred, MoveId},
                   {join, JoinState, QueuedMessages} = State) ->
    %io:format("p4: join_response~n"),
    % only act on related messages, i.e. messages from the current candidate
    Phase = get_phase(JoinState),
    case get_candidates(JoinState) of
        [] when Phase =:= phase4 -> % no candidates -> should not happen in phase4!
            log:log(error, "[ Node ~w ] empty candidate list in join phase 4, "
                        "starting over", [self()]),
            {join, start_over(JoinState), QueuedMessages};
        [] -> State; % in all other phases, ignore the delayed join_response
                     % if no candidates exist
        [Candidate | _Rest] ->
%%             io:format("Candidate: ~.0p~n", [Candidate]),
            CandidateNode = node_details:get(lb_op:get(Candidate, n1succ_new), node),
            case node:same_process(CandidateNode, Succ) of
                false -> State; % ignore old/unrelated message
                _ ->
                    MyId = get_id(JoinState),
                    MyIdVersion = get_id_version(JoinState),
                    case MyId =:= node:id(Succ) orelse MyId =:= node:id(Pred) of
                        true ->
                            log:log(warn, "[ Node ~w ] chosen ID already exists, trying next "
                                        "candidate", [self()]),
                            JoinState1 = remove_candidate_front(JoinState),
                            {join, contact_best_candidate(JoinState1), QueuedMessages};
                        _ ->
                            Me = node:new(comm:this(), MyId, MyIdVersion),
                            log:log(info, "[ Node ~w ] joined between ~w and ~w",
                                    [self(), Pred, Succ]),
                            rm_loop:notify_new_succ(node:pidX(Pred), Me),
                            rm_loop:notify_new_pred(node:pidX(Succ), Me),
                            
                            finish_join_and_slide(Me, Pred, Succ, ?DB:new(),
                                                  QueuedMessages, MoveId)
                    end
            end
    end;
%% userdevguide-end dht_node_join:join_other_p4

% a join timeout message re-starts the complete join
process_join_state({join, timeout}, {join, JoinState, QueuedMessages})
  when element(1, JoinState) =:= phase2 orelse
           element(1, JoinState) =:= phase2b orelse
           element(1, JoinState) =:= phase3 orelse
           element(1, JoinState) =:= phase3b orelse
           element(1, JoinState) =:= phase4 ->
    {join, start_over(JoinState), QueuedMessages};
process_join_state({join, timeout}, {join, JoinState, _QueuedMessages} = State)
  when element(1, JoinState) =:= phase1 ->
    % check that we are in phase1 (no further phase should receive the timeout)
    idholder:get_id(),
    State;

process_join_state({web_debug_info, Requestor}, {join, JoinState, QueuedMessages} = State) ->
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", lists:flatten(io_lib:format("~p", [Message]))}
                  || Message <- lists:sublist(QueuedMessages, 50)],
    MessageList = case length(QueuedMessages) > 50 of
                      true -> lists:append(MessageListTmp, [{"...", ""}]);
                      _    -> MessageListTmp
                  end,
    Phase = get_phase(JoinState),
    StateInfo =
        case lists:member(Phase, [phase2, phase2b, phase3, phase3b, phase4]) of
            true ->
                [{"phase",      Phase},
                 {"key",        get_id(JoinState)},
                 {"key_vers",   get_id_version(JoinState)},
                 {"contacts",   lists:flatten(io_lib:format("~p", [get_contact_nodes(JoinState)]))},
                 {"join_ids",   lists:flatten(io_lib:format("~p", [get_join_ids(JoinState)]))},
                 {"candidates", lists:flatten(io_lib:format("~p", [get_candidates(JoinState)]))}];
            _ -> [{"phase", Phase}]
        end,
    KeyValueList =
        [{"", ""}, {"joining dht_node process", ""} |
             lists:append(StateInfo,
                          [{"", ""},
                           {"queued messages:", ""} | MessageList])],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;
    
% Catch all other messages until the join procedure is complete
process_join_state(Msg, {join, JoinState, QueuedMessages}) ->
    %log:log(info("[dhtnode] [~p] postponed delivery of ~p", [self(), Msg]),
    {join, JoinState, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Process requests from a joining node at a existing node:.
-spec process_join_msg(join_message(), dht_node_state:state()) -> dht_node_state:state().
process_join_msg({join, get_candidate, Source_PID, Key}, State) ->
    Candidate = ?LB_PSV:create_join(State, Key),
    comm:send(Source_PID, {join, get_candidate_response, Key, Candidate}),
    State;
%% userdevguide-begin dht_node_join:join_request1
process_join_msg({join, join_request, NewPred}, State) when (not is_atom(NewPred)) ->
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
            send_join_response(State1, SlideOp1, NewPred);
        _ when not KeyInRange ->
            comm:send(node:pidX(NewPred), {join, join_response, not_responsible,
                                           dht_node_state:get(State, node)}),
            State;
        _ -> State
    end;
process_join_msg({join, join_response_timeout, NewPred, MoveFullId}, State) ->
    % almost the same as dht_node_move:safe_operation/5 but we tolerate wrong pred:
    case dht_node_state:get_slide_op(State, MoveFullId) of
        {pred, SlideOp} ->
            ResponseReceived =
                lists:member(slide_op:get_phase(SlideOp),
                             [wait_for_req_data, wait_for_pred_update_join]),
            case (slide_op:get_timeouts(SlideOp) < 3) of
                _ when ResponseReceived -> State;
                true ->
                    NewSlideOp = slide_op:inc_timeouts(SlideOp),
                    send_join_response(State, NewSlideOp, NewPred);
                _ ->
                    % abort the slide operation set up for the join:
                    % (similar to dht_node_move:abort_slide/*)
                    log:log(warn, "abort_join(op: ~p, reason: timeout)~n",
                            [SlideOp]),
                    slide_op:reset_timer(SlideOp), % reset previous timeouts
                    RMSubscrTag = {move, slide_op:get_id(SlideOp)},
                    rm_loop:unsubscribe(self(), RMSubscrTag),
                    State1 = dht_node_state:rm_db_range(
                               State, slide_op:get_interval(SlideOp)),
                    dht_node_state:set_slide(State1, pred, null)
            end;
        not_found -> State
    end;
%% userdevguide-end dht_node_join:join_request1
% only messages with the first element being "join" are processed here
% -> see dht_node.erl
process_join_msg({idholder_get_id_response, _Id, _IdVersion}, State) -> State;
process_join_msg({get_dht_nodes_response, _Nodes}, State) -> State;
process_join_msg({join, get_number_of_samples, _Samples, _Source}, State) -> State;
process_join_msg({join, get_candidate_response, _OrigJoinId, _Candidate}, State) -> State;
process_join_msg({join, join_response, _Succ, _Pred, _MoveFullId}, State) -> State;
process_join_msg({join, join_response, not_responsible, _Node}, State) -> State;
process_join_msg({join, lookup_timeout, _Node}, State) -> State;
process_join_msg({join, known_hosts_timeout}, State) -> State;
process_join_msg({join, get_number_of_samples_timeout}, State) -> State;
process_join_msg({join, join_request_timeout, _Timeouts}, State) -> State;
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

%% @doc Calls ?LB_PSV:get_number_of_samples/1 if there is a contact node
%%      in ContactNodes2 and then adds them to the list of contact nodes.
-spec get_number_of_samples
        (phase2(), ContactNodes::[node:node_type()]) -> phase2();
        (phase2b(), ContactNodes::[node:node_type()]) -> phase2b().
get_number_of_samples(JoinState, []) ->
    JoinState;
get_number_of_samples(JoinState, ContactNodes2 = [_|_]) ->
    ?LB_PSV:get_number_of_samples(ContactNodes2),
    msg_delay:send_local(get_number_of_samples_timeout() div 1000, self(),
                         {join, get_number_of_samples_timeout}),
    add_contact_nodes_back(ContactNodes2, JoinState).

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
-spec lookup(phase2() | phase2b() | phase3()) -> NewState::phase2() | phase3();
            (phase4()) -> NewState::phase4().
lookup(JoinState) ->
    lookup(JoinState, get_join_ids(JoinState)).

%% @doc Like lookup/1 but only looks up the given JoinIds (if there are any).
%%      Note: the returned join state will stay in phase 4 if already in phase 4.
-spec lookup(phase2() | phase2b() | phase3(), JoinIds::[?RT:key()]) -> NewState::phase2() | phase3();
            (phase4(), JoinIds::[?RT:key()]) -> NewState::phase4().
lookup(JoinState, []) -> JoinState;
lookup(JoinState, JoinIds = [_|_]) ->
    Phase = get_phase(JoinState),
    case get_contact_nodes(JoinState) of
        [] ->
            NewJoinState =
                case Phase of
                    phase4 -> JoinState;
                    _      ->
                        log:log(warn, "[ Node ~w ] no further nodes to contact, "
                               "trying to get new nodes...", [self()]),
                        set_phase(phase2, JoinState)
                end,
            get_known_nodes(),
            NewJoinState;
        [First | _Rest] ->
            _ = [comm:send(First, {lookup_aux, Id, 0, {join, get_candidate, comm:this(), Id}})
                || Id <- JoinIds],
            msg_delay:send_local(get_lookup_timeout() div 1000, self(),
                                 {join, lookup_timeout, First}),
            % move First to the end of the nodes to contact and continue
            JoinState1 = remove_contact_node(First, JoinState),
            JoinState2 = add_contact_nodes_back([First], JoinState1),
            case Phase of
                phase4 -> JoinState2;
                _      -> set_phase(phase3, JoinState2)
            end
    end.

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

%% @doc Select as many new (random) IDs as to create TotalCount candidates and
%%      issue lookups.
-spec lookup_new_ids2
        (TotalCount::pos_integer(), JoinState::phase2() | phase2b() | phase3())
            -> phase2() | phase3();
        (TotalCount::pos_integer(), JoinState::phase4()) -> phase4().
lookup_new_ids2(TotalCount, JoinState) ->
    OldIds = get_join_ids(JoinState),
    CurCount = length(OldIds) + length(get_candidates(JoinState)),
    NewIdsCount = max(TotalCount - CurCount, 0),
    lookup_new_ids1(NewIdsCount, JoinState).

%% @doc Integrates the Candidate into the join state if is is valid, i.e. a
%%      slide with an ID not equal to the node to join at.
%%      Note: a new ID will be sampled if the candidate is invalid.
-spec integrate_candidate
        (Candidate::lb_op:lb_op(), JoinState::phase2() | phase2b() | phase3() | phase3b(), Position::front | back)
            -> phase2() | phase2b() | phase3() | phase3b();
        (Candidate::lb_op:lb_op(), JoinState::phase4(), Position::front | back)
            -> phase4().
integrate_candidate(Candidate, JoinState, Position) ->
    % the other node could suggest a no_op -> select a new ID
    case lb_op:is_slide(Candidate) of
        false -> lookup_new_ids1(1, JoinState);
        _ ->
            % note: can not use an existing ID! the other node should not have
            % replied with such an ID though...
            NewIdCand = node_details:get(lb_op:get(Candidate, n1_new), new_key),
            SuccIdCand = node:id(node_details:get(lb_op:get(Candidate, n1succ_new), node)),
            case NewIdCand =:= SuccIdCand of
                true ->
                    log:log(warn, "[ Node ~w ] chosen ID already exists, trying a "
                                "new ID", [self()]),
                    lookup_new_ids1(1, JoinState);
                _ when Position =:= front ->
                    add_candidate_front(Candidate, JoinState);
                _ when Position =:= back ->
                    add_candidate_back(Candidate, JoinState)
            end
    end.

%% @doc Contacts the best candidate among all stored candidates and sends a
%%      join_request (Timeouts = 0).
-spec contact_best_candidate(JoinState::phase_2_4()) -> phase2() | phase2b() | phase3b() | phase4().
contact_best_candidate(JoinState) ->
    contact_best_candidate(JoinState, 0).
%% @doc Contacts the best candidate among all stored candidates and sends a
%%      join_request. Timeouts is the number of join_request_timeout messages
%%      previously received.
-spec contact_best_candidate(JoinState::phase_2_4(), Timeouts::non_neg_integer()) -> phase2() | phase2b() | phase3b() | phase4().
contact_best_candidate(JoinState, Timeouts) ->
    JoinState1 = sort_candidates(JoinState),
    case get_candidates(JoinState1) of
        [] -> % no candidates -> start over:
            start_over(JoinState1);
        [BestCand | _] ->
            NewId = node_details:get(lb_op:get(BestCand, n1_new), new_key),
            Id = get_id(JoinState1),
            IdVersion = get_id_version(JoinState1),
            case NewId of
                Id -> send_join_request(JoinState1, Timeouts);
                _  -> idholder:set_id(NewId, IdVersion + 1),
                      idholder:get_id(),
                      set_phase(phase3b, JoinState1)
            end
    end.

%% @doc Sends a join request to the first candidate. Timeouts is the number of
%%      join_request_timeout messages previously received.
%%      PreCond: the id has been set to the ID to join at and has been updated
%%               in JoinState.
-spec send_join_request(JoinState::phase_2_4(), Timeouts::non_neg_integer()) -> phase2() | phase2b() | phase4().
send_join_request(JoinState, Timeouts) ->
    case get_candidates(JoinState) of
        [] -> % no candidates -> start over (should not happen):
            start_over(JoinState);
        [BestCand | _] ->
            Id = get_id(JoinState),
            IdVersion = get_id_version(JoinState),
            NewSucc = node_details:get(lb_op:get(BestCand, n1succ_new), node),
            Me = node:new(comm:this(), Id, IdVersion),
            comm:send(node:pidX(NewSucc), {join, join_request, Me}),
            msg_delay:send_local(get_join_request_timeout() div 1000,
                                 self(), {join, join_request_timeout, Timeouts}),
            set_phase(phase4, JoinState)
    end.

%% userdevguide-begin dht_node_join:restart_join
%% @doc Goes back to phase 2 or 2b depending on whether contact nodes are
%%      available or not.
-spec start_over(JoinState::phase_2_4()) -> phase2() | phase2b().
start_over(JoinState) ->
    case get_contact_nodes(JoinState) of
        [] ->
            get_known_nodes(),
            set_phase(phase2, JoinState);
        [_|_] = ContactNodes ->
            ?LB_PSV:get_number_of_samples(ContactNodes),
            set_phase(phase2b, JoinState)
    end.
%% userdevguide-end dht_node_join:restart_join

%% @doc Sends a join response message to the new predecessor and sets the given
%%      slide operation in the dht_node state (adding a timeout to it as well).
%% userdevguide-begin dht_node_join:join_request2
-spec send_join_response(State::dht_node_state:state(),
                         NewSlideOp::slide_op:slide_op(),
                         NewPred::node:node_type())
        -> dht_node_state:state().
send_join_response(State, SlideOp, NewPred) ->
    MoveFullId = slide_op:get_id(SlideOp),
    NewSlideOp = slide_op:set_timer(SlideOp, get_join_response_timeout(),
                                   {join, join_response_timeout, NewPred, MoveFullId}),
    MyOldPred = dht_node_state:get(State, pred),
    MyNode = dht_node_state:get(State, node),
    comm:send(node:pidX(NewPred), {join, join_response, MyNode, MyOldPred, MoveFullId}),
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
    dht_node_state:new(?RT:empty_ext(rm_loop:get_neighbors(NeighbTable)), NeighbTable, DB).

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
-spec get_phase(phase_2_4()) -> phase2 | phase2b | phase3 | phase3b | phase4.
get_phase(JoinState)         -> element(1, JoinState).
-spec get_id(phase_2_4()) -> ?RT:key().
get_id(JoinState)            -> element(2, JoinState).
-spec get_id_version(phase_2_4()) -> non_neg_integer().
get_id_version(JoinState)    -> element(3, JoinState).
-spec get_contact_nodes(phase_2_4()) -> [comm:mypid()].
get_contact_nodes(JoinState) -> element(4, JoinState).
-spec get_join_ids(phase_2_4()) -> [?RT:key()].
get_join_ids(JoinState)      -> element(5, JoinState).
-spec get_candidates(phase_2_4()) -> [lb_op:lb_op()].
get_candidates(JoinState)    -> element(6, JoinState).

% setter:
-spec set_phase(phase2 | phase2b | phase3 | phase3b | phase4, phase_2_4()) -> phase_2_4().
set_phase(Phase, JoinState) -> setelement(1, JoinState, Phase).
-spec add_contact_nodes_back(Nodes::[comm:mypid()], phase_2_4()) -> phase_2_4().
add_contact_nodes_back([], JoinState) -> JoinState;
add_contact_nodes_back(Nodes = [_|_], {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, lists:append(ContactNodes, Nodes), JoinIds, Candidates}.
-spec add_contact_nodes_front(Nodes::[comm:mypid(),...], phase_2_4()) -> phase_2_4().
%% add_contact_nodes_front([], JoinState) ->
%%     JoinState;
add_contact_nodes_front([CNHead], {Phase, CurId, CurIdVersion, [CNHead, _] = ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates};
add_contact_nodes_front(Nodes = [_|_], {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, lists:append(Nodes, ContactNodes), JoinIds, Candidates}.
-spec remove_contact_node(Node::comm:mypid(), phase_2_4()) -> phase_2_4().
remove_contact_node(Node, {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, [N || N <- ContactNodes, N =/= Node],
     JoinIds, Candidates}.
-spec set_join_ids(JoinIds::[?RT:key()], phase_2_4()) -> phase_2_4().
set_join_ids(JoinIds, JoinState) -> setelement(5, JoinState, JoinIds).
-spec remove_join_id(JoinId::?RT:key(), phase_2_4()) -> phase_2_4().
remove_join_id(JoinIdToRemove, {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, ContactNodes,
     [Id || Id <- JoinIds, Id =/= JoinIdToRemove], Candidates}.
-spec add_candidate_front(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_front(Candidate, {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, [Candidate | Candidates]}.
-spec add_candidate_back(Candidate::lb_op:lb_op(), phase_2_4()) -> phase_2_4().
add_candidate_back(Candidate, {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, lists:append(Candidates, [Candidate])}.
-spec sort_candidates(phase_2_4()) -> phase_2_4().
sort_candidates({Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, ?LB_PSV:sort_candidates(Candidates)}.
-spec remove_candidate_front(phase_2_4()) -> phase_2_4().
remove_candidate_front({Phase, CurId, CurIdVersion, ContactNodes, JoinIds, []}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, []};
remove_candidate_front({Phase, CurId, CurIdVersion, ContactNodes, JoinIds, [_ | Candidates]}) ->
    {Phase, CurId, CurIdVersion, ContactNodes, JoinIds, Candidates}.
-spec set_id(Id::?RT:key(), IdVersion::non_neg_integer(), phase_2_4()) -> phase_2_4().
set_id(Id, IdVersion, {Phase, _CurId, _CurIdVersion, ContactNodes, JoinIds, Candidates}) ->
    {Phase, Id, IdVersion, ContactNodes, JoinIds, Candidates}.

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(join_response_timeout) and
    config:is_greater_than_equal(join_response_timeout, 1000) and

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
    config:is_greater_than_equal(join_get_number_of_samples_timeout, 1000).

%% @doc Gets the max number of ms to wait for a joining node's reply after
%%      it send a join request (set in the config files).
-spec get_join_response_timeout() -> pos_integer().
get_join_response_timeout() ->
    config:read(join_response_timeout).

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
