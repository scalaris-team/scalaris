%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-behavior(gen_component).

-export([start_link/1]).
-export([init/1, on_startup/2, on/2,
         activate/3, leave/0, update_id/1,
         get_neighbors/1, has_left/1, get_neighbors_table/0,
         notify_new_pred/2, notify_new_succ/2,
         % node/neighborhood change subscriptions:
         subscribe/5, unsubscribe/2,
         send_changes_to_subscriber/4,
         subscribe_default_filter/2, subscribe_node_change_filter/2,
         subscribe_dneighbor_change_filter/2]).
% for rm_* modules only:
-export([update_neighbors/2]).

-type subscriber_filter_fun() :: fun((OldNeighbors::nodelist:neighborhood(),
                                      NewNeighbors::nodelist:neighborhood()) -> boolean()).
-type subscriber_exec_fun() :: fun((Subscriber::pid() | null, Tag::any(),
                                    OldNeighbors::nodelist:neighborhood(),
                                    NewNeighbors::nodelist:neighborhood()) -> any()).

-type state_init() ::
          {NeighbTable :: tid(),
           RM_State    :: ?RM:state(),
           % subscribers to node change events, i.e. node ID changes:
           SubscrTable :: tid()}.
-type(state_uninit() :: {uninit, NeighbTable::tid(), SubscrTable::tid(),
                         QueuedMessages::msg_queue:msg_queue()}).
%% -type(state() :: state_init() | state_uninit()).

% accepted messages of an initialized rm_loop process
-type(message() ::
    {get_neighb_tid, SourcePid::comm:erl_local_pid()} |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::comm:mypid()} |
    {check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {init_check_ring, Token::non_neg_integer()} |
    {notify_new_pred, NewPred::node:node_type()} |
    {notify_new_succ, NewSucc::node:node_type()} |
%%     {leave} | % provided seperately in the on-handler spec
    {pred_left, OldPred::node:node_type(), PredsPred::node:node_type()} |
    {succ_left, OldSucc::node:node_type(), SuccsSucc::node:node_type()} |
    {update_id, NewId::?RT:key()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {subscribe, Pid::pid(), Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(), MaxCalls::pos_integer() | inf} |
    {unsubscribe, Pid::pid(), Tag::any()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends an initialization message to the node's rm_tman process.
-spec activate(Me::node:node_type(), Pred::node:node_type(),
               Succ::node:node_type()) -> ok.
activate(Me, Pred, Succ) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {init_rm, Me, Pred, Succ}).

%% @doc Returns the current neighborhood structure.
-spec get_neighbors(Table::tid()) -> nodelist:neighborhood().
get_neighbors(Table) -> ets:lookup_element(Table, neighbors, 2).

%% @doc Returns whether the current node has already left the ring
%%      (intermediate state before the node is killed or jumping to another
%%      ID).
-spec has_left(Table::tid()) -> boolean().
has_left(Table) -> ets:lookup_element(Table, has_left, 2).

%% @doc Gets the tid of the table rm_tman uses to store its neighbors.
%%      Beware: this is a synchronous call to the rm_tman process!
-spec get_neighbors_table() -> tid().
get_neighbors_table() ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {get_neighb_tid, self()}),
    receive {get_neighb_tid_response, Tid} -> Tid
    end.

%% @doc Notifies the successor and predecessor that the current dht_node is
%%      going to leave. Will reset the ring_maintenance state to uninit
%%      and inform the dht_node process (message handled in dht_node_move).
%%      Note: only call this method from inside the dht_node process!
-spec leave() -> ok.
leave() ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {leave}).

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new successor.
-spec notify_new_succ(Node::comm:mypid(), NewSucc::node:node_type()) -> ok.
notify_new_succ(Node, NewSucc) ->
    comm:send_to_group_member(Node, ring_maintenance,
                              {notify_new_succ, NewSucc}).

%% @doc Sends a message to the remote node's ring_maintenance process notifying
%%      it of a new predecessor.
-spec notify_new_pred(Node::comm:mypid(), NewPred::node:node_type()) -> ok.
notify_new_pred(Node, NewPred) ->
    comm:send_to_group_member(Node, ring_maintenance,
                              {notify_new_pred, NewPred}).

%% @doc Updates a dht node's id and sends the ring maintenance a message about
%%      the change.
%%      Beware: the only allowed node id changes are between the node's
%%      predecessor and successor!
-spec update_id(NewId::?RT:key()) -> ok.
update_id(NewId) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {update_id, NewId}).

%% @doc Default function to send changes to a subscriber when the neighborhood
%%      changes.
-spec send_changes_to_subscriber(Pid::pid(), Tag::any(),
        OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> ok.
send_changes_to_subscriber(Pid, Tag, OldNeighbors, NewNeighbors) ->
    comm:send_local(Pid, {rm_changed, Tag, OldNeighbors, NewNeighbors}).

%% @doc Default filter function for subscriptions that returns true if the
%%      old neighborhood differs from the new neighborhood.
-spec subscribe_default_filter(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> boolean().
subscribe_default_filter(OldNeighbors, NewNeighbors) ->
    OldNeighbors =/= NewNeighbors.

%% @doc Filter function for subscriptions that returns true if the
%%      old neighborhood's base node differs from the new node.
%%      Note: a node can only ever get newer, e.g. change its ID and increase
%%      its ID version.
-spec subscribe_node_change_filter(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> boolean().
subscribe_node_change_filter(OldNeighbors, NewNeighbors) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors).

%% @doc Filter function for subscriptions that returns true if the
%%      pred, succ or base node changed.
-spec subscribe_dneighbor_change_filter(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> boolean().
subscribe_dneighbor_change_filter(OldNeighbors, NewNeighbors) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors) orelse
        nodelist:succ(OldNeighbors) =/= nodelist:succ(NewNeighbors).

%% @doc Registers the given function to be called when the dht_node changes its
%%      id. It will get the given Pid and the new node as its parameters.
-spec subscribe(Pid::pid() | null, Tag::any(), subscriber_filter_fun(), subscriber_exec_fun(), MaxCalls::pos_integer() | inf) -> ok.
subscribe(RegPid, Tag, FilterFun, ExecFun, MaxCalls) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {subscribe, RegPid, Tag, FilterFun, ExecFun, MaxCalls}).

%% @doc Un-registers the given process with the given tag from node change
%%      updates.
-spec unsubscribe(Pid::pid() | null, Tag::any()) -> ok.
unsubscribe(RegPid, Tag) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {unsubscribe, RegPid, Tag}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the ring maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, ok, [{pid_groups_join_as, DHTNodeGroup,
                                            ring_maintenance}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(any()) -> {'$gen_component', [{on_handler, Handler::on_startup}],
                      State::state_uninit()}.
init(_) ->
    TableName = string:concat(pid_groups:my_groupname(), ":rm_tman"),
    NeighbTable = ets:new(list_to_atom(string:concat(TableName, ":neighbors")), [ordered_set, protected]),
    SubscrTable = ets:new(list_to_atom(string:concat(TableName, ":subscribers")), [ordered_set, protected]),
    gen_component:change_handler({uninit, NeighbTable, SubscrTable, msg_queue:new()}, on_startup).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on/2 when a
%%      'init_rm' message is received).
-spec on_startup(message(), state_uninit()) -> state_uninit();
                ({init_rm, Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type()}, state_uninit()) -> {'$gen_component', [{on_handler, Handler::on}], State::state_init()}.
on_startup({init_rm, Me, Pred, Succ}, {uninit, NeighbTable, SubscrTable, QueuedMessages}) ->
    % create the ets table storing the neighborhood
    ets:insert(NeighbTable, {has_left, false}),
    dn_cache:subscribe(),
    % initialize the rm_* module - assume it sets a neighborhood using update_neighbors/2!
    RM_State = ?RM:init(NeighbTable, Me, Pred, Succ),
    set_failuredetector(get_neighbors(NeighbTable)),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler({NeighbTable, RM_State, SubscrTable}, on);

on_startup(Msg, {uninit, NeighbTable, SubscrTable, QueuedMessages}) ->
    {uninit, NeighbTable, SubscrTable, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message() | ?RM:custom_message(), state_init()) -> state_init();
        ({leave}, state_init())
            -> {'$gen_component', [{on_handler, Handler::on_startup}],
                State::state_uninit()}.
on({get_neighb_tid, SourcePid}, {NeighbTable, _RM_State, _SubscrTable} = State) ->
    comm:send_local(SourcePid, {get_neighb_tid_response, NeighbTable}),
    State;

on({notify_new_pred, NewPred}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:new_pred(RM_State, NeighbTable, NewPred) end,
    update_state(NeighbTable, SubscrTable, RMFun);

on({notify_new_succ, NewSucc}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:new_succ(RM_State, NeighbTable, NewSucc) end,
    update_state(NeighbTable, SubscrTable, RMFun);

on({pred_left, OldPred, PredsPred}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:remove_pred(RM_State, NeighbTable, OldPred, PredsPred) end,
    update_state(NeighbTable, SubscrTable, RMFun);

on({succ_left, OldSucc, SuccsSucc}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:remove_succ(RM_State, NeighbTable, OldSucc, SuccsSucc) end,
    update_state(NeighbTable, SubscrTable, RMFun);

on({update_id, NewId}, {NeighbTable, RM_State, SubscrTable} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    OldMe = nodelist:node(Neighborhood),
    case node:id(OldMe) =/= NewId of
        true ->
            NewMe = node:update_id(OldMe, NewId),
            % note: nodelist can't update the base node if the new id is not
            % between pred id and succ id
            try begin
                    RMFun = fun() ->
                                    update_neighbors(NeighbTable,
                                                     nodelist:update_node(Neighborhood, NewMe)),
                                    ?RM:updated_node(RM_State, NeighbTable, OldMe, NewMe)
                            end,
                    update_state(NeighbTable, SubscrTable, RMFun)
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

on({leave}, {NeighbTable, RM_State, SubscrTable}) ->
    Neighborhood = get_neighbors(NeighbTable),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send_to_group_member(node:pidX(Succ), ring_maintenance, {pred_left, Me, Pred}),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance, {succ_left, Me, Succ}),
    comm:send_local(pid_groups:get_my(dht_node), {move, node_leave}),
    ?RM:leave(RM_State, NeighbTable),
    ets:insert(NeighbTable, {has_left, true}),
    gen_component:change_handler({uninit, NeighbTable, SubscrTable, msg_queue:new()}, on_startup);

% failure detector reported dead node
on({crash, DeadPid}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:crashed_node(RM_State, NeighbTable, DeadPid) end,
    update_state(NeighbTable, SubscrTable, RMFun);

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, {NeighbTable, RM_State, SubscrTable}) ->
    RMFun = fun() -> ?RM:zombie_node(RM_State, NeighbTable, Node) end,
    update_state(NeighbTable, SubscrTable, RMFun);

%% add Pid to the node change subscriber list
on({subscribe, Pid, Tag, FilterFun, ExecFun, MaxCalls}, {NeighbTable, _RM_State, SubscrTable} = State) ->
    SubscrTuple = {{Pid, Tag}, FilterFun, ExecFun, MaxCalls},
    ets:insert(SubscrTable, SubscrTuple),
    % check if the condition is already met:
    Neighborhood = get_neighbors(NeighbTable),
    call_subscribers_check(Neighborhood, Neighborhood, SubscrTuple, SubscrTable),
    State;

on({unsubscribe, Pid, Tag}, {_NeighbTable, _RM_State, SubscrTable} = State) ->
    ets:delete(SubscrTable, {Pid, Tag}),
    State;

% triggered by admin:dd_check_ring
on({check_ring, Token, Master}, {NeighbTable, _RM_State, _SubscrTable} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    Me = nodelist:node(Neighborhood),
    case {Token, Master} of
        {0, Me}     -> io:format(" [ RM ] CheckRing   OK  ~n");
        {0, _}      -> io:format(" [ RM ] CheckRing  reached TTL in Node ~p, not in ~p~n",
                                 [Me, Master]);
        {Token, Me} -> io:format(" [RM ] Token back with Value: ~p~n",[Token]);
        {Token, _}  ->
            Pred = nodelist:pred(Neighborhood),
            comm:send_to_group_member(node:pidX(Pred), ring_maintenance,
                                      {check_ring, Token - 1, Master})
    end,
    State;

% trigger by admin:dd_check_ring
on({init_check_ring, Token}, {NeighbTable, _RM_State, _SubscrTable} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance,
                              {check_ring, Token - 1, Me}),
    State;

on({web_debug_info, Requestor}, {NeighbTable, RM_State, SubscrTable} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    Preds = [{"preds:", ""} | make_indexed_nodelist(nodelist:preds(Neighborhood))],
    Succs = [{"succs:", ""} | make_indexed_nodelist(nodelist:succs(Neighborhood))],
    PredsSuccs = lists:append(Preds, Succs),
    RM_Info = ?RM:get_web_debug_info(RM_State, NeighbTable),
    Subscribers = [begin
                       case Pid of
                           null -> {"null", Tag};
                           _    -> {webhelpers:pid_to_name(Pid), Tag}
                       end
                   end
                   || {{Pid, Tag}, _FilterFun, _ExecFun} <- ets:tab2list(SubscrTable)],
    comm:send_local(Requestor,
                    {web_debug_info_reply,
                     [{"algorithm", lists:flatten(io_lib:format("~p", [?RM]))},
                      {"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                      {"nc_subscr", lists:flatten(io_lib:format("~p", [Subscribers]))} |
                      lists:append(PredsSuccs, RM_Info)]}),
    State;

on(Message, {NeighbTable, RM_State, SubscrTable}) ->
    % similar to update_state/2 but handle unknown_event differently
    OldNeighborhood = get_neighbors(NeighbTable),
    case ?RM:on(Message, RM_State, NeighbTable) of
        unknown_event -> unknown_event;
        NewRM_State   ->
            NewNeighborhood = get_neighbors(NeighbTable),
            call_subscribers(OldNeighborhood, NewNeighborhood, SubscrTable),
            update_failuredetector(OldNeighborhood, NewNeighborhood),
            {NeighbTable, NewRM_State, SubscrTable}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec update_state(NeighbTable::tid(), SubscrTable::tid(),
                   RMFun::fun(() -> ?RM:state())) -> state_init().
update_state(NeighbTable, SubscrTable, RMFun) ->
    OldNeighborhood = get_neighbors(NeighbTable),
    NewRM_State = RMFun(),
    NewNeighborhood = get_neighbors(NeighbTable),
    call_subscribers(OldNeighborhood, NewNeighborhood, SubscrTable),
    update_failuredetector(OldNeighborhood, NewNeighborhood),
    {NeighbTable, NewRM_State, SubscrTable}.

%% @doc Updates the stored neighborhood object. Only use inside the rm_loop
%%      process, i.e. in the rm_* modules called from here. Fails otherwise.
-spec update_neighbors(Table::tid(), nodelist:neighborhood()) -> ok.
update_neighbors(Table, Neighborhood) ->
    ets:insert(Table, {neighbors, Neighborhood}),
    ok.

% @doc Check if change of failuredetector is necessary.
-spec set_failuredetector(Neighborhood::nodelist:neighborhood()) -> ok.
set_failuredetector(Neighborhood) ->
    [_ | View] = nodelist:to_list(Neighborhood),
    NewPids = [node:pidX(Node) || Node <- View],
    fd:subscribe(NewPids).

% @doc Check if change of failuredetector is necessary and subscribe the new
%%     nodes' pids.
-spec update_failuredetector(OldNeighborhood::nodelist:neighborhood(),
                             NewNeighborhood::nodelist:neighborhood()) -> ok.
update_failuredetector(OldNeighborhood, NewNeighborhood) ->
    % Note: nodelist:to_list/1 would provide similar functionality to determine
    % the view but at a higher cost and we need neither unique nor sorted lists.
    OldView = lists:append(nodelist:preds(OldNeighborhood),
                           nodelist:succs(OldNeighborhood)),
    NewView = lists:append(nodelist:preds(NewNeighborhood),
                           nodelist:succs(NewNeighborhood)),
    OldPids = [node:pidX(Node) || Node <- OldView,
                                  not node:same_process(Node, nodelist:node(OldNeighborhood))],
    NewPids = [node:pidX(Node) || Node <- NewView,
                                  not node:same_process(Node, nodelist:node(NewNeighborhood))],
    fd:update_subscriptions(OldPids, NewPids),
    ok.

%% @doc Inform the dht_node of a new neighborhood.
-spec call_subscribers(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), SubscrTable::tid()) -> ok.
call_subscribers(OldNeighborhood, NewNeighborhood, SubscrTable) ->
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, SubscrTable, ets:first(SubscrTable)).

%% @doc Iterates over all susbcribers and calls their subscribed functions.
-spec call_subscribers_iter(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(), SubscrTable::tid(),
        CurrentKey::{Pid::pid() | null, Tag::any()} | '$end_of_table') -> ok.
call_subscribers_iter(_OldNeighborhood, _NewNeighborhood, _SubscrTable, '$end_of_table') ->
    ok;
call_subscribers_iter(OldNeighborhood, NewNeighborhood, SubscrTable, CurrentKey) ->
    % assume the key exists (it should since we are iterating over the table!)
    [SubscrTuple] = ets:lookup(SubscrTable, CurrentKey),
    call_subscribers_check(OldNeighborhood, NewNeighborhood, SubscrTuple, SubscrTable),
    call_subscribers_iter(OldNeighborhood, NewNeighborhood, SubscrTable,
                          ets:next(SubscrTable, CurrentKey)).

%% @doc Checks whether FilterFun for the current subscription tuple is true
%%      and executes ExecFun. Unsubscribes the tuple, if ExecFun has been
%%      called MaxCalls times.
-spec call_subscribers_check(OldNeighbors::nodelist:neighborhood(),
        NewNeighbors::nodelist:neighborhood(),
        {{Pid::pid() | null, Tag::any()}, FilterFun::subscriber_filter_fun(),
         ExecFun::subscriber_exec_fun(), MaxCalls::pos_integer() | inf},
        SubscrTable::tid()) -> ok.
call_subscribers_check(OldNeighborhood, NewNeighborhood,
        {{Pid, Tag}, FilterFun, ExecFun, MaxCalls}, SubscrTable) ->
    case FilterFun(OldNeighborhood, NewNeighborhood) of
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
    [{lists:flatten(io_lib:format("~p", [Index])),
      lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- IndexedList].
