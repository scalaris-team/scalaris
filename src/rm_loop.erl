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
%% @doc    TODO: Add description to rm_loop
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
         get_neighbors/1, get_neighbors_table/0,
         notify_new_pred/2, notify_new_succ/2,
         % node change subscriptions:
         register_for_node_change/1, register_for_node_change/2,
         unregister_from_node_change/1, unregister_from_node_change/2,
         unregister_all_from_node_change/1]).
% for rm_* modules only:
-export([update_neighbors/2]).

-type subscriber_fun() :: fun((Subscriber::comm:erl_local_pid(),
                               NewNode::node:node_type()) -> any()).
-type subscriber_list() :: [{Subscriber::comm:erl_local_pid(),
                             ExecFun::subscriber_fun()}].
-type state_init() ::
          {NeighbTable   :: tid(),
           RM_State      :: ?RM:state(),
           % subscribers to node change events, i.e. node ID changes:
           NCSubscribers :: subscriber_list()}.
-type(state_uninit() :: {uninit, QueuedMessages :: msg_queue:msg_queue()}).
%% -type(state() :: state_init() | state_uninit()).

% accepted messages of an initialized rm_tman process
-type(message() ::
    {get_neighb_tid, SourcePid::comm:erl_local_pid()} |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::comm:mypid()} |
    {check_ring, Token::non_neg_integer(), Master::node:node_type()} |
    {init_check_ring, Token::non_neg_integer()} |
    {notify_new_pred, NewPred::node:node_type()} |
    {notify_new_succ, NewSucc::node:node_type()} |
%%     {leave, SourcePid::comm:erl_local_pid()} |
    {pred_left, OldPred::node:node_type(), PredsPred::node:node_type()} |
    {succ_left, OldSucc::node:node_type(), SuccsSucc::node:node_type()} |
    {update_id, NewId::?RT:key()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends an initialization message to the node's rm_tman process.
-spec activate(Me::node:node_type(), Pred::node:node_type(),
               Succ::node:node_type()) -> ok.
activate(Me, Pred, Succ) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {init_rm, Me, Pred, Succ}).

-spec get_neighbors(Table::tid()) -> nodelist:neighborhood().
get_neighbors(Table) -> ets:lookup_element(Table, neighbors, 2).

%% @doc Gets the tid of the table rm_tman uses to store its neighbors.
%%      Beware: this is a synchronous call to the rm_tman process!
-spec get_neighbors_table() -> tid().
get_neighbors_table() ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {get_neighb_tid, self()}),
    receive {get_neighb_tid_response, Tid} -> Tid
    end.

%% @doc Notifies the successor and predecessor that the current dht_node is
%%      going to leave / left. Will reset the ring_maintenance state to uninit
%%      and respond with a {leave_response} message.
%%      Note: only call this method from inside the dht_node process!
-spec leave() -> ok.
leave() ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {leave, self()}).

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

%% @doc Default fun for node change updates.
-spec send_node_change(Pid::comm:erl_local_pid(), NewNode::node:node_type()) -> ok.
send_node_change(Pid, NewNode) ->
    comm:send_local(Pid, {node_update, NewNode}).

%% @doc Registers the given process to receive {node_update, Node} messages if
%%      the dht_node changes its id.
-spec register_for_node_change(Pid::comm:erl_local_pid()) -> ok.
register_for_node_change(RegPid) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {reg_for_nc, RegPid, fun send_node_change/2}).

%% @doc Registers the given function to be called when the dht_node changes its
%%      id. It will get the given Pid and the new node as its parameters.
-spec register_for_node_change(Pid::comm:erl_local_pid(), subscriber_fun()) -> ok.
register_for_node_change(RegPid, FunToExecute) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {reg_for_nc, RegPid, FunToExecute}).

%% @doc Un-registers the given process from node change updates using the
%%      default send_node_change/2 handler sending {node_update, Node} messages.
-spec unregister_from_node_change(Pid::comm:erl_local_pid()) -> ok.
unregister_from_node_change(RegPid) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {unreg_from_nc, RegPid, fun send_node_change/2}).

%% @doc Un-registers the given process from all node change updates using
%%      any(!) message handler.
-spec unregister_all_from_node_change(Pid::comm:erl_local_pid()) -> ok.
unregister_all_from_node_change(RegPid) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {unreg_from_nc, RegPid}).

%% @doc Un-registers the given process from node change updates using
%%      the given message handler.
%%      Note that locally created funs may not be eligible for this as a newly
%%      created fun may not compare equal to the previously created one.
-spec unregister_from_node_change(Pid::comm:erl_local_pid(), subscriber_fun()) -> ok.
unregister_from_node_change(RegPid, FunToExecute) ->
    Pid = pid_groups:get_my(ring_maintenance),
    comm:send_local(Pid, {unreg_from_nc, RegPid, FunToExecute}).

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
init(_) -> gen_component:change_handler({uninit, msg_queue:new()}, on_startup).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on/2 when a
%%      'init_rm' message is received).
-spec on_startup(message(), state_uninit()) -> state_uninit();
                ({init_rm, Me::node:node_type(), Pred::node:node_type(),
                  Succ::node:node_type()}, state_uninit()) -> {'$gen_component', [{on_handler, Handler::on}], State::state_init()}.
on_startup({init_rm, Me, Pred, Succ}, {uninit, QueuedMessages}) ->
    % create the ets table storing the neighborhood
    TableName = list_to_atom(string:concat(pid_groups:my_groupname(), ":rm_tman")),
    NeighbTable = ets:new(TableName, [ordered_set, protected]),
    dn_cache:subscribe(),
    % initialize the rm_* module - assume it sets a neighborhood using update_neighbors/2!
    RM_State = ?RM:init(NeighbTable, Me, Pred, Succ),
    set_failuredetector(get_neighbors(NeighbTable)),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler({NeighbTable, RM_State, []}, on);

on_startup(Msg, {uninit, QueuedMessages}) ->
    {uninit, msg_queue:add(QueuedMessages, Msg)}.

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message() | ?RM:custom_message(), state_init()) -> state_init();
        ({leave, SourcePid::comm:erl_local_pid()}, state_init())
            -> {'$gen_component', [{on_handler, Handler::on_startup}],
                State::state_uninit()}.
on({get_neighb_tid, SourcePid}, {NeighbTable, _RM_State, _NCSubscribers} = State) ->
    comm:send_local(SourcePid, {get_neighb_tid_response, NeighbTable}),
    State;

on({notify_new_pred, NewPred}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:new_pred(RM_State, NeighbTable, NewPred) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

on({notify_new_succ, NewSucc}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:new_succ(RM_State, NeighbTable, NewSucc) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

on({pred_left, OldPred, PredsPred}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:remove_pred(RM_State, NeighbTable, OldPred, PredsPred) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

on({succ_left, OldSucc, SuccsSucc}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:remove_succ(RM_State, NeighbTable, OldSucc, SuccsSucc) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

on({update_id, NewId}, {NeighbTable, RM_State, NCSubscribers} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    OldMe = nodelist:node(Neighborhood),
    NewMe = node:update_id(OldMe, NewId),
    case OldMe =:= NewMe of
        true ->
            % note: nodelist can't update the base node if the new id is not
            % between pred id and succ id
            try begin
                    update_neighbors(NeighbTable,
                                     nodelist:update_node(Neighborhood, NewMe)),
                    RMFun = fun() -> ?RM:updated_node(RM_State, NeighbTable, OldMe, NewMe) end,
                    update_state(NeighbTable, NCSubscribers, RMFun)
                end
            catch
                throw:_ ->
                    log:log(error, "[ RM ] can't update dht node ~w with id ~w (pred=~w, succ=~w)",
                            [nodelist:node(Neighborhood), NewId,
                             nodelist:pred(Neighborhood),
                             nodelist:succ(Neighborhood)]),
                    State
            end;
        _ -> State
    end;

on({leave, SourcePid}, {NeighbTable, RM_State, _NCSubscribers}) ->
    Neighborhood = get_neighbors(NeighbTable),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send_to_group_member(node:pidX(Succ), ring_maintenance, {pred_left, Me, Pred}),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance, {succ_left, Me, Succ}),
    comm:send_local(SourcePid, {leave_response}),
    ?RM:leave(RM_State, NeighbTable),
    gen_component:change_handler({uninit, msg_queue:new()}, on_startup);

% failure detector reported dead node
on({crash, DeadPid}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:crashed_node(RM_State, NeighbTable, DeadPid) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, {NeighbTable, RM_State, NCSubscribers}) ->
    RMFun = fun() -> ?RM:zombie_node(RM_State, NeighbTable, Node) end,
    update_state(NeighbTable, NCSubscribers, RMFun);

%% add Pid to the node change subscriber list
on({reg_for_nc, Pid, FunToExecute}, {NeighbTable, RM_State, OldNCSubscr}) ->
    NewNCSubscribers = [{Pid, FunToExecute} | OldNCSubscr],
    {NeighbTable, RM_State, NewNCSubscribers};

on({unreg_from_nc, Pid, FunToExecute}, {NeighbTable, RM_State, OldNCSubscr}) ->
    SubscrTuple = {Pid, FunToExecute},
    NewNCSubscribers = [X || X <- OldNCSubscr, X =/= SubscrTuple],
    {NeighbTable, RM_State, NewNCSubscribers};

on({unreg_from_nc, Pid}, {NeighbTable, RM_State, OldNCSubscr}) ->
    NewNCSubscribers = [X || X = {Subscr, _Fun} <- OldNCSubscr, Subscr =/= Pid],
    {NeighbTable, RM_State, NewNCSubscribers};

% triggered by admin:dd_check_ring
on({check_ring, Token, Master}, {NeighbTable, _RM_State, _NCSubscribers} = State) ->
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
on({init_check_ring, Token}, {NeighbTable, _RM_State, _NCSubscribers} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    Me = nodelist:node(Neighborhood),
    Pred = nodelist:pred(Neighborhood),
    comm:send_to_group_member(node:pidX(Pred), ring_maintenance,
                              {check_ring, Token - 1, Me}),
    State;

on({web_debug_info, Requestor}, {NeighbTable, RM_State, NCSubscribers} = State) ->
    Neighborhood = get_neighbors(NeighbTable),
    Preds = [{"preds:", ""} | make_indexed_nodelist(nodelist:preds(Neighborhood))],
    Succs = [{"succs:", ""} | make_indexed_nodelist(nodelist:succs(Neighborhood))],
    PredsSuccs = lists:append(Preds, Succs),
    RM_Info = ?RM:get_web_debug_info(RM_State, NeighbTable),
    comm:send_local(Requestor,
                    {web_debug_info_reply,
                     [{"algorithm", lists:flatten(io_lib:format("~p", [?RM]))},
                      {"self", lists:flatten(io_lib:format("~p", [nodelist:node(Neighborhood)]))},
                      {"nc_subscr", lists:flatten(io_lib:format("~p", [[{webhelpers:pid_to_name(Pid), Fun} || {Pid, Fun} <- NCSubscribers]]))} |
                      lists:append(PredsSuccs, RM_Info)]}),
    State;

on(Message, {NeighbTable, RM_State, NCSubscribers}) ->
    % similar to update_state/2 but handle unknown_event differently
    OldNeighborhood = get_neighbors(NeighbTable),
    case ?RM:on(Message, RM_State, NeighbTable) of
        unknown_event -> unknown_event;
        NewRM_State   ->
            NewNeighborhood = get_neighbors(NeighbTable),
            update_dht_node(OldNeighborhood, NewNeighborhood, NCSubscribers),
            update_failuredetector(OldNeighborhood, NewNeighborhood),
            {NeighbTable, NewRM_State, NCSubscribers}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec update_state(NeighbTable::tid(), NCSubscribers::subscriber_list(),
                   RMFun::fun(() -> ?RM:state())) -> state_init().
update_state(NeighbTable, NCSubscribers, RMFun) ->
    OldNeighborhood = get_neighbors(NeighbTable),
    NewRM_State = RMFun(),
    NewNeighborhood = get_neighbors(NeighbTable),
    update_dht_node(OldNeighborhood, NewNeighborhood, NCSubscribers),
    update_failuredetector(OldNeighborhood, NewNeighborhood),
    {NeighbTable, NewRM_State, NCSubscribers}.

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
    OldPids = [node:pidX(Node) || Node <- OldView],
    NewPids = [node:pidX(Node) || Node <- NewView],
    fd:update_subscriptions(OldPids, NewPids).

%% @doc Inform the dht_node of a new neighborhood.
-spec update_dht_node(OldNeighborhood::nodelist:neighborhood(),
                      NewNeighborhood::nodelist:neighborhood(),
                      NCSubscribers::subscriber_list()) -> ok.
update_dht_node(OldNeighborhood, NewNeighborhood, NCSubscribers) ->
    case OldNeighborhood =/= NewNeighborhood of
        true ->
            OldNode = nodelist:node(OldNeighborhood),
            NewNode = nodelist:node(NewNeighborhood),
            case node:is_newer(NewNode, OldNode) of
                true ->
                    [Fun(Pid, NewNode) || {Pid, Fun} <- NCSubscribers],
                    idholder:set_id(node:id(NewNode), node:id_version(NewNode));
                _ -> ok
            end,
            comm:send_local(pid_groups:get_my(dht_node),
                            {rm_update_neighbors, OldNeighborhood});
        _ -> ok
    end.

%% @doc Helper for the web_debug_info handler. Converts the given node list to
%%      an indexed node list containing string representations of the nodes.
-spec make_indexed_nodelist(NodeList::[node:node_type()]) -> [{Index::string(), Node::string()}].
make_indexed_nodelist(NodeList) ->
    IndexedList = lists:zip(lists:seq(1, length(NodeList)), NodeList),
    [{lists:flatten(io_lib:format("~p", [Index])),
      lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- IndexedList].
