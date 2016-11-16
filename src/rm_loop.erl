%  @copyright 2010-2016 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Shared process for the ring maintenance implementations.
%% @end
%% @version $Id$
-module(rm_loop).
-author('kruber@zib.de').

-include("scalaris.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p~n", [self(), Msg, State])).
%% -define(TRACE_STATE(OldState, NewState, Reason),
%%         case element(1, OldState) =/= element(1, NewState) of
%%             true -> log:pal("[ ~.0p ] ~p~n  new Neighbors: ~.0p~n",
%%                [self(), Reason, nodelist:to_list(get_neighbors(NewState))]);
%%             _    -> ok
%%         end).
%% -define(TRACE_STATE(OldState, NewState, Reason),
%%         case element(1, OldState) =/= element(1, NewState) of
%%             true -> trace_mpath:log_info(self(), {rm_changed, Reason,
%%                                                   nodelist:to_list(get_neighbors(NewState))});
%%             _    -> ok
%%         end).
-define(TRACE_STATE(OldState, NewState, Reason), ok).

-export([send_trigger/0, init_first/0, init/4, cleanup/1, on/2,
         leave/1, update_id/1,
         get_neighbors/1, has_left/1, is_responsible/2,
         notify_new_pred/2, notify_new_succ/2,
         notify_slide_finished/1,
         propose_new_neighbors/1,
         % received at dht_node, (also) handled here:
         fd_notify/4, zombie_node/2,
         % node/neighborhood change subscriptions:
         subscribe/5, unsubscribe/2,
         subscribe_dneighbor_change_filter/3,
         subscribe_dneighbor_change_slide_filter/3,
         % web debug info:
         get_web_debug_info/1,
         % unit tests:
         unittest_create_state/2]).

-export_type([state/0, reason/0]).

-type reason() :: {slide_finished, pred | succ | none} | % a slide finished
                  {graceful_leave, Node::node:node_type()} | % the given node is about to leave
                  {node_crashed, Node::comm:mypid()} | % the given node crashed
                  {add_subscriber} | % a subscriber was added
                  {node_discovery} | % a new/changed node was discovered
                  {update_id_failed} | % a request to update the node's ID failed
                  {unknown}. % any other reason, e.g. changes during slides

-type subscriber_filter_fun() :: fun((OldNeighbors::nodelist:neighborhood(),
                                      NewNeighbors::nodelist:neighborhood(),
                                      Reason::reason()) -> boolean()).
-type subscriber_exec_fun() :: fun((Subscriber::pid() | null, Tag::any(),
                                    OldNeighbors::nodelist:neighborhood(),
                                    NewNeighbors::nodelist:neighborhood(),
                                    Reason::reason()) -> any()).

-opaque state() ::
          {RM_State    :: ?RM:state(),
           HasLeft     :: boolean(),
           % subscribers to node change events, i.e. node ID changes:
           SubscrTable :: ets:tid()}.

% accepted messages of an initialized rm_loop process
-type(message() ::
    {rm, trigger} |
    {rm, trigger_action} |
    {rm, notify_new_pred, NewPred::node:node_type()} |
    {rm, notify_new_succ, NewSucc::node:node_type()} |
    {rm, notify_slide_finished, SlideType::pred | succ} |
    {rm, propose_new_neighbors, NewNodes::[node:node_type(),...]} |
    {rm, node_info, SourcePid::comm:mypid(), Which::[is_leaving | succlist | succ | predlist | pred | node,...]} |
    {rm, leave, Tag::jump | leave} |
    {rm, update_my_id, NewId::?RT:key()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {rm, subscribe, Pid::pid(), Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(), MaxCalls::pos_integer() | inf} |
    {rm, unsubscribe, Pid::pid(), Tag::any()} |
    {rm, get_move_state, Pid::pid()}).

-define(SEND_OPTIONS, [{channel, prio}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns the current neighborhood structure.
-spec get_neighbors(state()) -> nodelist:neighborhood().
get_neighbors({RM_State, _HasLeft, _SubscrTable}) ->
    ?RM:get_neighbors(RM_State).

%% @doc Returns whether the current node has already left the ring
%%      (intermediate state before the node is killed or jumping to another
%%      ID).
-spec has_left(state()) -> boolean().
has_left({_RM_State, HasLeft, _SubscrTable}) -> HasLeft.

%% @doc Convenience method checking whether the current node is responsible
%%      for the given key, i.e. has not left and Key is in range.
%%      Improves performance over two calls in dht_node_state/is_responsible/2.
-spec is_responsible(Key::intervals:key(), state()) -> boolean().
is_responsible(Key, {RM_State, HasLeft, _SubscrTable}) ->
    not HasLeft andalso
        intervals:in(Key, nodelist:node_range(?RM:get_neighbors(RM_State))).


%% @doc Notifies fd-subscribed nodes that the current dht_node is
%%      going to leave. Will inform the dht_node process (message handled in
%%      dht_node_move).
%%      Note: only call this method from inside the dht_node process!
-spec leave(Tag::jump | leave) -> ok.
leave(Tag) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, leave, Tag}).

%% @doc Sends a message to the remote node's dht_node process notifying
%%      it of a new successor.
-spec notify_new_succ(Node::comm:mypid(), NewSucc::node:node_type()) -> ok.
notify_new_succ(Node, NewSucc) ->
    comm:send(Node, {rm, notify_new_succ, NewSucc}, ?SEND_OPTIONS).

%% @doc Sends a message to the remote node's dht_node process notifying
%%      it of a new predecessor.
-spec notify_new_pred(Node::comm:mypid(), NewPred::node:node_type()) -> ok.
notify_new_pred(Node, NewPred) ->
    comm:send(Node, {rm, notify_new_pred, NewPred}, ?SEND_OPTIONS).

%% @doc Sends a message to the local node's dht_node process notifying
%%      it of a finished slide.
-spec notify_slide_finished(SlideType::pred | succ) -> ok.
notify_slide_finished(SlideType) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, notify_slide_finished, SlideType}).

%% @doc Sends a message to the local node's dht_node process notifying
%%      it of a potential new neighbor.
-spec propose_new_neighbors(NewNodes::[node:node_type(),...]) -> ok.
propose_new_neighbors(NewNodes) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, propose_new_neighbors, NewNodes}).

%% @doc Updates a dht node's id and sends the ring maintenance a message about
%%      the change.
%%      Beware: the only allowed node id changes are between the node's
%%      predecessor and successor!
-spec update_id(NewId::?RT:key()) -> ok.
update_id(NewId) ->
    %TODO: do not send message, include directly
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, update_my_id, NewId}).

%% @doc Filter function for subscriptions that returns true if a
%%      direct neighbor, i.e. pred, succ or base node, changed.
-spec subscribe_dneighbor_change_filter(
        OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
        Reason::reason()) -> boolean().
subscribe_dneighbor_change_filter(OldNeighbors, NewNeighbors, _Reason) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors) orelse
        nodelist:succ(OldNeighbors) =/= nodelist:succ(NewNeighbors).

%% @doc Filter function for subscriptions that returns true if a
%%      direct neighbor, i.e. pred, succ or base node, changed or a slide
%%      operation finished.
-spec subscribe_dneighbor_change_slide_filter(
        OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
        Reason::reason()) -> boolean().
subscribe_dneighbor_change_slide_filter(_OldNeighbors, _NewNeighbors, {slide_finished, _}) ->
    true;
subscribe_dneighbor_change_slide_filter(OldNeighbors, NewNeighbors, _Reason) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors) orelse
        nodelist:succ(OldNeighbors) =/= nodelist:succ(NewNeighbors).

%% @doc Registers the given function to be called when the dht_node changes its
%%      id. It will get the given Pid and the new node as its parameters.
-spec subscribe(Pid::pid() | null, Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(),
                MaxCalls::pos_integer() | inf) -> ok.
subscribe(RegPid, Tag, FilterFun, ExecFun, MaxCalls) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, subscribe, RegPid, Tag, FilterFun, ExecFun, MaxCalls}).

%% @doc Un-registers the given process with the given tag from node change
%%      updates.
-spec unsubscribe(Pid::pid() | null, Tag::any()) -> ok.
unsubscribe(RegPid, Tag) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, unsubscribe, RegPid, Tag}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the RM trigger.
-spec send_trigger() -> ok.
send_trigger() ->
    msg_delay:send_trigger(?RM:trigger_interval(), {rm, trigger}).

%% @doc Initializes the rm_loop trigger and whatever the RM-specific code wants.
%%      NOTE: this is called during dht_node:init/1 and thus is not infected
%%            with trace_mpath.
-spec init_first() -> ok.
init_first() ->
    send_trigger(),
    ?RM:init_first().

%% @doc Initializes the rm_loop state.
-spec init(Me::node:node_type(), Pred::node:node_type(),
           Succ::node:node_type(), OldSubscrTable::null | ets:tid()) -> state().
init(Me, Pred, Succ, OldSubscrTable) ->
    % do not wait for the first trigger to arrive here
    % -> execute trigger action immediately
    comm:send_local(self(), {rm, trigger_action}),
    % create the ets table storing the subscriptions
    if OldSubscrTable =/= null -> SubscrTable = OldSubscrTable;
       true -> SubscrTable = ets:new(rm_subscribers, [ordered_set, private])
    end,
    dn_cache:subscribe(),
    RM_State = ?RM:init(Me, Pred, Succ),
    set_failuredetector(?RM:get_neighbors(RM_State)),
    NewState = {RM_State, false, SubscrTable},
    ?TRACE_STATE({null, null, null}, NewState, init),
    NewState.

%% @doc Cleans up before the state is deleted, e.g. removes fd subscriptions
%%      for a rejoin operation.
%%      NOTE: only valid when HasLeft is set in the state!
-spec cleanup(State::state()) -> ok.
cleanup({RM_State, true, _SubscrTable} = _State) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    View = lists:append(nodelist:preds(Neighborhood),
                        nodelist:succs(Neighborhood)),
    Pids = [node:pidX(Node) || Node <- View,
                               not node:same_process(Node, nodelist:node(Neighborhood))],
    fd:update_subscriptions(self(), Pids, []),
    ok.

%% @doc Creates a state() object for a unit test.
%%      Pre: the process must have joined a group. See pid_groups:join_as/2.
-spec unittest_create_state(Neighbors::nodelist:neighborhood(), HasLeft::boolean()) -> state().
unittest_create_state(Neighbors, HasLeft) ->
    SubscrTable = ets:new(rm_subscribers, [ordered_set, protected]),
    {?RM:unittest_create_state(Neighbors), HasLeft, SubscrTable}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message() | ?RM:custom_message(), state()) -> state().
on({rm, trigger}, State) ->
    send_trigger(),
    RMFun = fun(RM_State) -> ?RM:trigger_action(RM_State) end,
    update_state(State, RMFun);

on({rm, trigger_action}, State) ->
    RMFun = fun(RM_State) -> ?RM:trigger_action(RM_State) end,
    update_state(State, RMFun);

on({rm, notify_new_pred, NewPred}, State) ->
    RMFun = fun(RM_State) -> ?RM:new_pred(RM_State, NewPred) end,
    update_state(State, RMFun);

on({rm, notify_new_succ, NewSucc}, State) ->
    RMFun = fun(RM_State) -> ?RM:new_succ(RM_State, NewSucc) end,
    update_state(State, RMFun);

on({rm, notify_slide_finished, SlideType}, State = {RM_State, _HasLeft, SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    call_subscribers(Neighborhood, Neighborhood, {slide_finished, SlideType}, SubscrTable),
    State;

on({rm, propose_new_neighbors, NewNodes}, State) ->
    ?RM:contact_new_nodes(NewNodes),
    State;

on({rm, update_my_id, NewId}, State) ->
    Neighborhood = ?RM:get_neighbors(element(1, State)),
    OldMe = nodelist:node(Neighborhood),
    case node:id(OldMe) of
        NewId -> State;
        _ ->
            NewMe = node:update_id(OldMe, NewId),
            % note: nodelist can't update the base node if the new id is not
            % between pred id and succ id
            try begin
                    RMFun = fun(RM_State) -> ?RM:update_node(RM_State, NewMe) end,
                    update_state(State, RMFun)
                end
            catch
                throw:(Reason = new_node_not_in_pred_succ_interval) ->
                    log:log(error, "[ RM ] can't update dht node ~w with id ~w (pred=~w, succ=~w): ~.0p",
                            [nodelist:node(Neighborhood), NewId,
                             nodelist:pred(Neighborhood),
                             nodelist:succ(Neighborhood),
                             Reason]),
                    update_state(State, fun(RM_State) -> {{update_id_failed}, RM_State} end)
            end
    end;

on({rm, node_info, SourcePid, Which}, {RM_State, HasLeft, _SubscrTable} = State) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    ExtractValuesFun =
        fun(Elem, NodeDetails) ->
                Value =
                    case Elem of
                        is_leaving  -> HasLeft;
                        succlist    -> nodelist:succs(Neighborhood);
                        succ        -> nodelist:succ(Neighborhood);
                        predlist    -> nodelist:preds(Neighborhood);
                        pred        -> nodelist:pred(Neighborhood);
                        node        -> nodelist:node(Neighborhood)
                    end,
                node_details:set(NodeDetails, Elem, Value)
        end,
    NodeDetails = lists:foldl(ExtractValuesFun, node_details:new(), Which),
    comm:send(SourcePid, {rm, node_info_response, NodeDetails}, ?SEND_OPTIONS),
    State;

on({rm, leave, Tag}, {RM_State, _HasLeft, SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    Me = nodelist:node(Neighborhood),
    SupDhtNode = pid_groups:get_my(sup_dht_node),
    fd:report(Tag, sup:sup_get_all_children(SupDhtNode), Me),
    % also update the pred in the successor:
    Pred = nodelist:pred(Neighborhood),
    notify_new_pred(node:pidX(nodelist:succ(Neighborhood)), Pred),
    % msg to dht_node to continue the slide:
    comm:send_local(self(), {move, node_leave}),
    {RM_State, true, SubscrTable};

%% requests the move state of the rm, e.g. before rejoining the ring
on({rm, get_move_state, Pid}, {_RM_State, _HasLeft, SubscrTable} = State) ->
    MoveState = [{subscr_table, SubscrTable}],
    comm:send_local(Pid, {get_move_state_response, MoveState}),
    State;

%% add Pid to the node change subscriber list
on({rm, subscribe, Pid, Tag, FilterFun, ExecFun, MaxCalls}, {RM_State, _HasLeft, SubscrTable} = State) ->
    SubscrTuple = {{Pid, Tag}, FilterFun, ExecFun, MaxCalls},
    ets:insert(SubscrTable, SubscrTuple),
    % check if the condition is already met:
    Neighborhood = ?RM:get_neighbors(RM_State),
    call_subscribers_check(Neighborhood, Neighborhood, {add_subscriber}, SubscrTuple, SubscrTable),
    State;

on({rm, unsubscribe, Pid, Tag}, {_RM_State, _HasLeft, SubscrTable} = State) ->
    ets:delete(SubscrTable, {Pid, Tag}),
    State;

on(Message, {RM_State, HasLeft, SubscrTable} = OldState) ->
    % similar to update_state/2 but handle unknown_event differently
    OldNeighborhood = ?RM:get_neighbors(RM_State),
    case ?RM:handle_custom_message(Message, RM_State) of
        unknown_event ->
            log:log(error, "unknown message: ~.0p~n in Module: ~p and handler ~p~n in State ~.0p",
                    [Message, ?MODULE, on, OldState]),
            OldState;
        {Reason, NewRM_State}   ->
            NewNeighborhood = ?RM:get_neighbors(NewRM_State),
            call_subscribers(OldNeighborhood, NewNeighborhood, Reason, SubscrTable),
            update_failuredetector(OldNeighborhood, NewNeighborhood, null),
            NewState = {NewRM_State, HasLeft, SubscrTable},
            ?TRACE_STATE(RM_State, NewState, Reason),
            NewState
    end.

% failure detector reported dead node
-spec fd_notify(State::state(), Event::fd:event(), DeadPid::comm:mypid(),
                Data::term()) -> state().
fd_notify(State, Event, DeadPid, Data) ->
    RMFun = fun(RM_State) -> ?RM:fd_notify(RM_State, Event, DeadPid, Data) end,
    % note: only a crashed pid is unsubscribed by fd itself!
    NewState = update_state(State, RMFun, ?IIF(Event =:= crash, DeadPid, null)),
    % do rrepair in case of non-graceful leaves only
    case config:read(rrepair_after_crash) andalso
             Event =:= crash of
        true ->
            OldPred = nodelist:pred(?RM:get_neighbors(element(1, State))),
            NewNeighb = ?RM:get_neighbors(element(1, NewState)),
            NewPred = nodelist:pred(NewNeighb),
            case NewPred of
                OldPred -> ok;
                _       ->
                    case intervals:in(node:id(OldPred), nodelist:node_range(NewNeighb)) of
                        true ->
                            CrashInterval = node:mk_interval_between_nodes(NewPred, OldPred),
                            case pid_groups:get_my(rrepair) of
                                failed -> ok;
                                Pid ->
                                    comm:send_local(
                                      Pid,
                                      {request_resolve, {interval_upd_my, CrashInterval}, []})
                            end;
                        false ->
                            ok
                    end
            end;
        false -> ok
    end,
    NewState.

% dead-node-cache reported dead node to be alive again
-spec zombie_node(State::state(), Node::node:node_type()) -> state().
zombie_node(State, Node) ->
    RMFun = fun(RM_State) -> ?RM:zombie_node(RM_State, Node) end,
    update_state(State, RMFun).

-spec get_web_debug_info(State::state()) -> [{string(), string()}].
get_web_debug_info({RM_State, _HasLeft, SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    Preds = [{"preds:", ""} | make_indexed_nodelist(nodelist:preds(Neighborhood))],
    Succs = [{"succs:", ""} | make_indexed_nodelist(nodelist:succs(Neighborhood))],
    PredsSuccs = lists:append(Preds, Succs),
    RM_Info = ?RM:get_web_debug_info(RM_State),
    Subscribers = [begin
                       case Pid of
                           null -> {"null", Tag};
                           _    -> {pid_groups:pid_to_name(Pid), Tag}
                       end
                   end
                   || {{Pid, Tag}, _FilterFun, _ExecFun} <- ets:tab2list(SubscrTable)],
    [{"rm_state:", ""},
     {"algorithm", webhelpers:safe_html_string("~p", [?RM])},
     {"self", webhelpers:safe_html_string("~p", [nodelist:node(Neighborhood)])},
     {"nc_subscr", webhelpers:safe_html_string("~p", [Subscribers])} |
         lists:append(PredsSuccs, RM_Info)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calls RMFun (which may update the Neighborhood), then calls all
%%      subscribers and updates the failure detector if necessary.
-spec update_state(OldState::state(), RMFun::fun((?RM:state()) -> {reason(), ?RM:state()}))
        -> NewState::state().
update_state(OldState, RMFun) ->
    update_state(OldState, RMFun, null).

-spec update_state(OldState::state(), RMFun::fun((?RM:state()) -> {reason(), ?RM:state()}),
                   CrashedPid::comm:mypid() | null) -> NewState::state().
update_state({OldRM_State, HasLeft, SubscrTable} = _OldState, RMFun, CrashedPid) ->
    OldNeighborhood = ?RM:get_neighbors(OldRM_State),
    {Reason, NewRM_State} = RMFun(OldRM_State),
    NewNeighborhood = ?RM:get_neighbors(NewRM_State),
    call_subscribers(OldNeighborhood, NewNeighborhood, Reason, SubscrTable),
    update_failuredetector(OldNeighborhood, NewNeighborhood, CrashedPid),
    NewState = {NewRM_State, HasLeft, SubscrTable},
    ?TRACE_STATE(_OldState, NewState, Reason),
    NewState.

% @doc Subscribe all PIDs in the neighborhood with the failuredetector.
-spec set_failuredetector(Neighborhood::nodelist:neighborhood()) -> ok.
set_failuredetector(Neighborhood) ->
    [_ | View] = nodelist:to_list(Neighborhood),
    NewPids = [node:pidX(Node) || Node <- View],
    fd:subscribe(self(), NewPids).

% @doc Check if change of failuredetector is necessary and subscribe the new
%%     nodes' pids.
-spec update_failuredetector(OldNeighborhood::nodelist:neighborhood(),
                             NewNeighborhood::nodelist:neighborhood(),
                             CrashedPid::comm:mypid() | null) -> ok.
update_failuredetector(OldNeighborhood, NewNeighborhood, CrashedPid) ->
    % Note: nodelist:to_list/1 would provide similar functionality to determine
    % the view but at a higher cost and we need neither unique nor sorted lists.
    OldView = lists:append(nodelist:preds(OldNeighborhood),
                           nodelist:succs(OldNeighborhood)),
    NewView = lists:append(nodelist:preds(NewNeighborhood),
                           nodelist:succs(NewNeighborhood)),
    OldNodePid = node:pidX(nodelist:node(OldNeighborhood)),
    NewNodePid = node:pidX(nodelist:node(NewNeighborhood)),
    OldPids = [node:pidX(Node) || Node <- OldView,
                                  not node:same_process(Node, OldNodePid),
                                  % note: crashed pid already unsubscribed by fd, do not unsubscribe again
                                  not node:same_process(Node, CrashedPid)],
    NewPids = [node:pidX(Node) || Node <- NewView,
                                  not node:same_process(Node, NewNodePid)],
    fd:update_subscriptions(self(), OldPids, NewPids),
    ok.

%% @doc Inform the dht_node of a new neighborhood.
-spec call_subscribers(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), Reason::reason(),
        SubscrTable::ets:tid()) -> ok.
call_subscribers(OldNeighborhood, NewNeighborhood, Reason, SubscrTable) ->
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, Reason, SubscrTable,
                          ets:first(SubscrTable)).

%% @doc Iterates over all susbcribers and calls their subscribed functions.
-spec call_subscribers_iter(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), Reason::reason(), SubscrTable::ets:tid(),
        CurrentKey::{Pid::pid() | null, Tag::any()} | '$end_of_table') -> ok.
call_subscribers_iter(_OldNeighborhood, _NewNeighborhood, _Reason,
                      _SubscrTable, '$end_of_table') ->
    ok;
call_subscribers_iter(OldNeighborhood, NewNeighborhood, Reason, SubscrTable, CurrentKey) ->
    % assume the key exists (it should since we are iterating over the table!)
    [SubscrTuple] = ets:lookup(SubscrTable, CurrentKey),
    call_subscribers_check(OldNeighborhood, NewNeighborhood, Reason, SubscrTuple, SubscrTable),
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, Reason, SubscrTable,
                          ets:next(SubscrTable, CurrentKey)).

%% @doc Checks whether FilterFun for the current subscription tuple is true
%%      and executes ExecFun. Unsubscribes the tuple, if ExecFun has been
%%      called MaxCalls times.
-spec call_subscribers_check(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), Reason::reason(),
        {{Pid::pid() | null, Tag::any()}, FilterFun::subscriber_filter_fun(),
         ExecFun::subscriber_exec_fun(), MaxCalls::pos_integer() | inf},
        SubscrTable::ets:tid()) -> ok.
call_subscribers_check(OldNeighborhood, NewNeighborhood, Reason,
        {{Pid, Tag}, FilterFun, ExecFun, MaxCalls}, SubscrTable) ->
    case FilterFun(OldNeighborhood, NewNeighborhood, Reason) of
        true -> ExecFun(Pid, Tag, OldNeighborhood, NewNeighborhood, Reason),
                case MaxCalls of
                    inf -> ok;
                    1   -> ets:delete(SubscrTable, {Pid, Tag}); % unsubscribe
                    _   -> % subscribe with new max
                           ets:insert(SubscrTable, {{Pid, Tag}, FilterFun, ExecFun, MaxCalls - 1})
                end;
        _    -> ok
    end.

%% @doc Helper for the web_debug_info handler. Converts the given node list to
%%      an indexed node list containing string representations of the nodes.
-spec make_indexed_nodelist(NodeList::[node:node_type()]) -> [{Index::string(), Node::string()}].
make_indexed_nodelist(NodeList) ->
    IndexedList = lists:zip(lists:seq(1, length(NodeList)), NodeList),
    [{webhelpers:safe_html_string("~p", [Index]),
      webhelpers:safe_html_string("~p", [Node])} || {Index, Node} <- IndexedList].
