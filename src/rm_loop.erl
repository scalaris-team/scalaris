%  @copyright 2010-2011 Zuse Institute Berlin

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
-vsn('$Id$ ').

-include("scalaris.hrl").

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p~n", [self(), Msg, State])).
%% -define(TRACE_STATE(OldState, NewState),
%%         case element(1, OldState) =/= element(1, NewState) of
%%             true -> ct:pal("[ ~.0p ]~n  new Neighbors: ~.0p~n",
%%                [self(), nodelist:to_list(get_neighbors(NewState))]);
%%             _    -> ok
%%         end).
-define(TRACE_STATE(OldState, NewState), ok).

-export([init/3, on/2,
         leave/0, update_id/1,
         get_neighbors/1, has_left/1, is_responsible/2,
         notify_new_pred/2, notify_new_succ/2,
         notify_slide_finished/1,
         % received at dht_node, (also) handled here:
         crashed_node/2, zombie_node/2,
         % node/neighborhood change subscriptions:
         subscribe/5, unsubscribe/2,
         subscribe_dneighbor_change_filter/3,
         subscribe_dneighbor_change_slide_filter/3,
         % web debug info:
         get_web_debug_info/1,
         % unit tests:
         unittest_create_state/2]).

-ifdef(with_export_type_support).
-export_type([state/0, slide/0]).
-endif.

-type slide() :: pred | succ | both | none.

-type subscriber_filter_fun() :: fun((OldNeighbors::nodelist:neighborhood(),
                                      NewNeighbors::nodelist:neighborhood(),
                                      IsSlide::slide()) -> boolean()).
-type subscriber_exec_fun() :: fun((Subscriber::pid() | null, Tag::any(),
                                    OldNeighbors::nodelist:neighborhood(),
                                    NewNeighbors::nodelist:neighborhood()) -> any()).

-type state_t() ::
          {RM_State    :: ?RM:state(),
           HasLeft     :: boolean(),
           % subscribers to node change events, i.e. node ID changes:
           SubscrTable :: tid()}.
-opaque state() :: state_t().

% accepted messages of an initialized rm_loop process
-type(message() ::
    {rm, check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {rm, init_check_ring, Token::non_neg_integer()} |
    {rm, notify_new_pred, NewPred::node:node_type()} |
    {rm, notify_new_succ, NewSucc::node:node_type()} |
    {rm, notify_slide_finished, IsSlide::slide()} |
    {rm, leave} |
    {rm, pred_left, OldPred::node:node_type(), PredsPred::node:node_type()} |
    {rm, succ_left, OldSucc::node:node_type(), SuccsSucc::node:node_type()} |
    {rm, update_id, NewId::?RT:key()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {rm, subscribe, Pid::pid(), Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(), MaxCalls::pos_integer() | inf} |
    {rm, unsubscribe, Pid::pid(), Tag::any()}).

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


%% @doc Notifies the successor and predecessor that the current dht_node is
%%      going to leave. Will inform the dht_node process (message handled in
%%      dht_node_move).
%%      Note: only call this method from inside the dht_node process!
-spec leave() -> ok.
leave() ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, leave}).

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
-spec notify_slide_finished(slide()) -> ok.
notify_slide_finished(IsSlide) ->
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, notify_slide_finished, IsSlide}).

%% @doc Updates a dht node's id and sends the ring maintenance a message about
%%      the change.
%%      Beware: the only allowed node id changes are between the node's
%%      predecessor and successor!
-spec update_id(NewId::?RT:key()) -> ok.
update_id(NewId) ->
    %TODO: do not send message, include directly
    Pid = pid_groups:get_my(dht_node),
    comm:send_local(Pid, {rm, update_id, NewId}).

%% @doc Filter function for subscriptions that returns true if a
%%      direct neighbor, i.e. pred, succ or base node, changed.
-spec subscribe_dneighbor_change_filter(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(), IsSlide::slide()) -> boolean().
subscribe_dneighbor_change_filter(OldNeighbors, NewNeighbors, _IsSlide) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors) orelse
        nodelist:succ(OldNeighbors) =/= nodelist:succ(NewNeighbors).

%% @doc Filter function for subscriptions that returns true if a
%%      direct neighbor, i.e. pred, succ or base node, changed or a slide
%%      operation finished.
-spec subscribe_dneighbor_change_slide_filter(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(), IsSlide::slide()) -> boolean().
subscribe_dneighbor_change_slide_filter(OldNeighbors, NewNeighbors, IsSlide) ->
    IsSlide =/= none orelse
        nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors) orelse
        nodelist:succ(OldNeighbors) =/= nodelist:succ(NewNeighbors).

%% @doc Registers the given function to be called when the dht_node changes its
%%      id. It will get the given Pid and the new node as its parameters.
-spec subscribe(Pid::pid() | null, Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(), MaxCalls::pos_integer() | inf) -> ok.
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

%% @doc Initializes the rm_loop state.
-spec init(Me::node:node_type(), Pred::node:node_type(),
           Succ::node:node_type()) -> state().
init(Me, Pred, Succ) ->
    % create the ets table storing the subscriptions
    TableName = pid_groups:my_groupname() ++ ":rm_tman",
    SubscrTable = ets:new(list_to_atom(TableName ++ ":subscribers"),
                          [ordered_set, private]),
    dn_cache:subscribe(),
    RM_State = ?RM:init(Me, Pred, Succ),
    set_failuredetector(?RM:get_neighbors(RM_State)),
    NewState = {RM_State, false, SubscrTable},
    ?TRACE_STATE({null, null, null}, NewState),
    NewState.

%% @doc Creates a state() object for a unit test.
%%      Pre: the process must have joined a group. See pid_groups:join_as/2.
-spec unittest_create_state(Neighbors::nodelist:neighborhood(), HasLeft::boolean()) -> state().
unittest_create_state(Neighbors, HasLeft) ->
    TableName = pid_groups:my_groupname() ++ ":rm_tman",
    SubscrTable = ets:new(list_to_atom(TableName ++ ":subscribers"),
                          [ordered_set, protected]),
    {?RM:unittest_create_state(Neighbors), HasLeft, SubscrTable}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message() | ?RM:custom_message(), state()) -> state().
on({rm, notify_new_pred, NewPred}, State = {RM_State, _HasLeft, _SubscrTable}) ->
    RMFun = fun() -> ?RM:new_pred(RM_State, NewPred) end,
    update_state(State, RMFun);

on({rm, notify_new_succ, NewSucc}, State = {RM_State, _HasLeft, _SubscrTable}) ->
    RMFun = fun() -> ?RM:new_succ(RM_State, NewSucc) end,
    update_state(State, RMFun);

on({rm, notify_slide_finished, IsSlide}, State = {RM_State, _HasLeft, SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    call_subscribers(Neighborhood, Neighborhood, IsSlide, SubscrTable),
    State;

on({rm, pred_left, OldPred, PredsPred}, State = {RM_State, _HasLeft, _SubscrTable}) ->
    RMFun = fun() -> ?RM:remove_pred(RM_State, OldPred, PredsPred) end,
    update_state(State, RMFun);

on({rm, succ_left, OldSucc, SuccsSucc}, State = {RM_State, _HasLeft, _SubscrTable}) ->
    RMFun = fun() -> ?RM:remove_succ(RM_State, OldSucc, SuccsSucc) end,
    update_state(State, RMFun);

on({rm, update_id, NewId}, State = {RM_State, _HasLeft, _SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    OldMe = nodelist:node(Neighborhood),
    case node:id(OldMe) =/= NewId of
        true ->
            NewMe = node:update_id(OldMe, NewId),
            % note: nodelist can't update the base node if the new id is not
            % between pred id and succ id
            try begin
                    RMFun = fun() -> ?RM:update_node(RM_State, NewMe) end,
                    update_state(State, RMFun)
                end
            catch
                throw:Reason ->
                    log:log(error, "[ RM ] can't update dht node ~w with id ~w (pred=~w, succ=~w): ~.0p",
                            [nodelist:node(Neighborhood), NewId,
                             nodelist:pred(Neighborhood),
                             nodelist:succ(Neighborhood),
                             Reason]),
                    State
            end;
        _ -> State
    end;

on({rm, leave}, {RM_State, _HasLeft, SubscrTable}) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send(node:pidX(Succ), {rm, pred_left, Me, Pred}, ?SEND_OPTIONS),
    comm:send(node:pidX(Pred), {rm, succ_left, Me, Succ}, ?SEND_OPTIONS),
    comm:send_local(self(), {move, node_leave}), % msg to dht_node
    ?RM:leave(RM_State),
    {RM_State, true, SubscrTable};

%% add Pid to the node change subscriber list
on({rm, subscribe, Pid, Tag, FilterFun, ExecFun, MaxCalls}, {RM_State, _HasLeft, SubscrTable} = State) ->
    SubscrTuple = {{Pid, Tag}, FilterFun, ExecFun, MaxCalls},
    ets:insert(SubscrTable, SubscrTuple),
    % check if the condition is already met:
    Neighborhood = ?RM:get_neighbors(RM_State),
    call_subscribers_check(Neighborhood, Neighborhood, none, SubscrTuple, SubscrTable),
    State;

on({rm, unsubscribe, Pid, Tag}, {_RM_State, _HasLeft, SubscrTable} = State) ->
    ets:delete(SubscrTable, {Pid, Tag}),
    State;

% triggered by admin:dd_check_ring
on({rm, check_ring, Token, Master}, {RM_State, _HasLeft, _SubscrTable} = State) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    Me = nodelist:node(Neighborhood),
    case {Token, Master} of
        {0, Me}     -> io:format(" [ RM ] CheckRing   OK  ~n");
        {0, _}      -> io:format(" [ RM ] CheckRing  reached TTL in Node ~p, not in ~p~n",
                                 [Me, Master]);
        {Token, Me} -> io:format(" [RM ] Token back with Value: ~p~n",[Token]);
        {Token, _}  ->
            Pred = nodelist:pred(Neighborhood),
            comm:send(node:pidX(Pred), {rm, check_ring, Token - 1, Master}, ?SEND_OPTIONS)
    end,
    State;

% trigger by admin:dd_check_ring
on({rm, init_check_ring, Token}, {RM_State, _HasLeft, _SubscrTable} = State) ->
    Neighborhood = ?RM:get_neighbors(RM_State),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    comm:send(node:pidX(Pred), {rm, check_ring, Token - 1, Me}, ?SEND_OPTIONS),
    State;

on(Message, {RM_State, HasLeft, SubscrTable} = OldState) ->
    % similar to update_state/2 but handle unknown_event differently
    OldNeighborhood = ?RM:get_neighbors(RM_State),
    case ?RM:on(Message, RM_State) of
        unknown_event ->
            log:log(error, "unknown message: ~.0p~n in Module: ~p and handler ~p~n in State ~.0p",
                    [Message, ?MODULE, on, OldState]),
            OldState;
        NewRM_State   ->
            NewNeighborhood = ?RM:get_neighbors(NewRM_State),
            call_subscribers(OldNeighborhood, NewNeighborhood, none, SubscrTable),
            update_failuredetector(OldNeighborhood, NewNeighborhood, null),
            NewState = {NewRM_State, HasLeft, SubscrTable},
            ?TRACE_STATE(_OldState, NewState),
            NewState
    end.

% failure detector reported dead node
-spec crashed_node(State::state(), DeadPid::comm:mypid()) -> state().
crashed_node(State = {RM_State, _HasLeft, _SubscrTable}, DeadPid) ->
    RMFun = fun() -> ?RM:crashed_node(RM_State, DeadPid) end,
    update_state(State, RMFun, DeadPid).

% dead-node-cache reported dead node to be alive again
-spec zombie_node(State::state(), Node::node:node_type()) -> state().
zombie_node(State = {RM_State, _HasLeft, _SubscrTable}, Node) ->
    RMFun = fun() -> ?RM:zombie_node(RM_State, Node) end,
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
-spec update_state(OldState::state_t(), RMFun::fun(() -> ?RM:state()))
        -> NewState::state().
update_state(OldState, RMFun) ->
    update_state(OldState, RMFun, null).

-spec update_state(OldState::state_t(), RMFun::fun(() -> ?RM:state()),
                   CrashedPid::comm:mypid() | null) -> NewState::state().
update_state({OldRM_State, HasLeft, SubscrTable} = _OldState, RMFun, CrashedPid) ->
    OldNeighborhood = ?RM:get_neighbors(OldRM_State),
    NewRM_State = RMFun(),
    NewNeighborhood = ?RM:get_neighbors(NewRM_State),
    call_subscribers(OldNeighborhood, NewNeighborhood, none, SubscrTable),
    update_failuredetector(OldNeighborhood, NewNeighborhood, CrashedPid),
    NewState = {NewRM_State, HasLeft, SubscrTable},
    ?TRACE_STATE(_OldState, NewState),
    NewState.

% @doc Check if change of failuredetector is necessary.
-spec set_failuredetector(Neighborhood::nodelist:neighborhood()) -> ok.
set_failuredetector(Neighborhood) ->
    [_ | View] = nodelist:to_list(Neighborhood),
    NewPids = [node:pidX(Node) || Node <- View],
    fd:subscribe(NewPids).

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
    OldPids = [node:pidX(Node) || Node <- OldView,
                                  not node:same_process(Node, nodelist:node(OldNeighborhood)),
                                  % note: crashed pid already unsubscribed by fd, do not unsubscribe again
                                  not node:same_process(Node, CrashedPid)],
    NewPids = [node:pidX(Node) || Node <- NewView,
                                  not node:same_process(Node, nodelist:node(NewNeighborhood))],
    fd:update_subscriptions(OldPids, NewPids),
    ok.

%% @doc Inform the dht_node of a new neighborhood.
-spec call_subscribers(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), IsSlide::slide(),
        SubscrTable::tid()) -> ok.
call_subscribers(OldNeighborhood, NewNeighborhood, IsSlide, SubscrTable) ->
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, IsSlide, SubscrTable, ets:first(SubscrTable)).

%% @doc Iterates over all susbcribers and calls their subscribed functions.
-spec call_subscribers_iter(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), IsSlide::slide(), SubscrTable::tid(),
        CurrentKey::{Pid::pid() | null, Tag::any()} | '$end_of_table') -> ok.
call_subscribers_iter(_OldNeighborhood, _NewNeighborhood, _IsSlide, _SubscrTable, '$end_of_table') ->
    ok;
call_subscribers_iter(OldNeighborhood, NewNeighborhood, IsSlide, SubscrTable, CurrentKey) ->
    % assume the key exists (it should since we are iterating over the table!)
    [SubscrTuple] = ets:lookup(SubscrTable, CurrentKey),
    call_subscribers_check(OldNeighborhood, NewNeighborhood, IsSlide, SubscrTuple, SubscrTable),
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, IsSlide, SubscrTable,
                          ets:next(SubscrTable, CurrentKey)).

%% @doc Checks whether FilterFun for the current subscription tuple is true
%%      and executes ExecFun. Unsubscribes the tuple, if ExecFun has been
%%      called MaxCalls times.
-spec call_subscribers_check(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), IsSlide::slide(),
        {{Pid::pid() | null, Tag::any()}, FilterFun::subscriber_filter_fun(),
         ExecFun::subscriber_exec_fun(), MaxCalls::pos_integer() | inf},
        SubscrTable::tid()) -> ok.
call_subscribers_check(OldNeighborhood, NewNeighborhood, IsSlide,
        {{Pid, Tag}, FilterFun, ExecFun, MaxCalls}, SubscrTable) ->
    case FilterFun(OldNeighborhood, NewNeighborhood, IsSlide) of
        true -> ExecFun(Pid, Tag, OldNeighborhood, NewNeighborhood),
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
