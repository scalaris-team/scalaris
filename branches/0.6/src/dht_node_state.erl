% @copyright 2007-2013 Zuse Institute Berlin

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
%% @doc State of a dht_node.
%% @version $Id$
-module(dht_node_state).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).

-export([new/3,
         get/2,
         dump/1,
         set_rt/2, set_rm/2, set_db/2, set_lease_list/2,
         details/1, details/2]).
%% node responsibility:
-export([has_left/1,
        is_responsible/2,
        is_db_responsible/2]).
%% transactions:
-export([set_tx_tp_db/2]).
%% node moves:
-export([get_slide/2, set_slide/3,
         slide_get_data_start_record/2, slide_add_data/2,
         slide_take_delta_stop_record/2, slide_add_delta/2,
         slide_stop_record/3,
         get_split_key/5,
         add_db_range/3, rm_db_range/2]).
%% bulk owner:
-export([add_bulkowner_reply_msg/5,
        take_bulkowner_reply_msgs/1,
        get_bulkowner_reply_timer/1,
        set_bulkowner_reply_timer/2]).
%% prbr DBs and states:
-export([get_prbr_state/2]).
-export([set_prbr_state/3]).
%% snapshots
-export([set_snapshot_state/2]).

-ifdef(with_export_type_support).
-export_type([state/0, name/0, db_selector/0, slide_data/0, slide_delta/0]).
-endif.

-type db_selector() :: kv |
                       txid_1 | txid_2 | txid_3 | txid_4 |
                       leases_1 | leases_2 | leases_3 | leases_4.
-type name() :: rt | rt_size | neighbors | succlist | succ | succ_id
              | succ_pid | predlist | pred | pred_id | pred_pid | node
              | node_id | my_range | db_range | succ_range | join_time
              | db | tx_tp_db | proposer | load | slide_pred | slide_succ
              | msg_fwd | rm_state | monitor_proc | prbr_state.

-type slide_snap() :: {snapshot_state:snapshot_state(), db_dht:db_as_list()} | {false}.

-type slide_data() :: {MovingData::db_dht:db_as_list(),
                       slide_snap()}.
-type slide_delta() :: {ChangedData::db_dht:db_as_list(), DeletedKeys::[?RT:key()]}.

%% userdevguide-begin dht_node_state:state
-record(state, {rt         = ?required(state, rt)        :: ?RT:external_rt(),
                rm_state   = ?required(state, rm_state)  :: rm_loop:state(),
                join_time  = ?required(state, join_time) :: erlang_timestamp(),
                db         = ?required(state, db)        :: db_dht:db(),
                tx_tp_db   = ?required(state, tx_tp_db)  :: any(),
                proposer   = ?required(state, proposer)  :: pid(),
                % slide with pred (must not overlap with 'slide with succ'!):
                slide_pred              = null :: slide_op:slide_op() | null,
                % slide with succ (must not overlap with 'slide with pred'!):
                slide_succ              = null :: slide_op:slide_op() | null,
                % additional range to respond to during a move:
                db_range   = []   :: [{intervals:interval(), slide_op:id()}],
                bulkowner_reply_timer   = null :: null | reference(),
                bulkowner_reply_ids     = []   :: [uid:global_uid()],
                monitor_proc            = ?required(state, monitor_proc) :: pid(),
                prbr_kv_db = ?required(state, prbr_state) :: prbr:state(),
                txid_db1 = ?required(state, prbr_state) :: prbr:state(),
                txid_db2 = ?required(state, prbr_state) :: prbr:state(),
                txid_db3 = ?required(state, prbr_state) :: prbr:state(),
                txid_db4 = ?required(state, prbr_state) :: prbr:state(),
                lease_db1 = ?required(state, prbr_state) :: prbr:state(),
                lease_db2 = ?required(state, prbr_state) :: prbr:state(),
                lease_db3 = ?required(state, prbr_state) :: prbr:state(),
                lease_db4 = ?required(state, prbr_state) :: prbr:state(),
                lease_list = ?required(state, lease_list) :: l_on_cseq:lease_list_state(),
                snapshot_state   = null :: snapshot_state:snapshot_state() | null
               }).
-opaque state() :: #state{}.
%% userdevguide-end dht_node_state:state

-spec new(?RT:external_rt(), RMState::rm_loop:state(), db_dht:db()) -> state().
new(RT, RMState, DB) ->
    #state{rt = RT,
           rm_state = RMState,
           join_time = now(),
           db = DB,
           tx_tp_db = tx_tp:init(),
           proposer = pid_groups:get_my(paxos_proposer),
           monitor_proc = pid_groups:get_my(dht_node_monitor),
           prbr_kv_db = prbr:init(prbr_kv_db),
           txid_db1 = prbr:init(txid_db1),
           txid_db2 = prbr:init(txid_db2),
           txid_db3 = prbr:init(txid_db3),
           txid_db4 = prbr:init(txid_db4),
           lease_db1 = prbr:init(lease_db1),
           lease_db2 = prbr:init(lease_db2),
           lease_db3 = prbr:init(lease_db3),
           lease_db4 = prbr:init(lease_db4),
           lease_list = l_on_cseq:empty_lease_list(),
		   snapshot_state = snapshot_state:new()
          }.

%% @doc Gets the given property from the dht_node state.
%%      Allowed keys include:
%%      <ul>
%%        <li>rt = routing table,</li>
%%        <li>rt_size = size of the routing table (provided for convenience),</li>
%%        <li>succlist = successor list,</li>
%%        <li>succ = successor (provided for convenience),</li>
%%        <li>succ_id = ID of the successor (provided for convenience),</li>
%%        <li>succ_pid = PID of the successor (provided for convenience),</li>
%%        <li>predlist = predecessor list,</li>
%%        <li>pred = predecessor (provided for convenience),</li>
%%        <li>pred_id = ID of the predecessor (provided for convenience),</li>
%%        <li>pred_pid = PID of the predecessor (provided for convenience),</li>
%%        <li>node = the own node,</li>
%%        <li>node_id = the ID of the own node (provided for convenience),</li>
%%        <li>my_range = the range of the own node,</li>
%%        <li>succ_range = the range of the successor,</li>
%%        <li>join_time = the time the node was created, i.e. joined the system,</li>
%%        <li>db = DB storing the items,</li>
%%        <li>tx_tp_db = transaction participant DB,</li>
%%        <li>proposer = paxos proposer PID,</li>
%%        <li>load = the load of the own node (provided for convenience).</li>
%%        <li>slide_pred = information about the node's current slide operation with its predecessor.</li>
%%        <li>slide_succ = information about the node's current slide operation with its successor.</li>
%%        <li>snapshot_state = snapshot algorithm state information</li>
%%      </ul>
%%      Beware of race conditions sing the neighborhood may have changed at
%%      the next call.
-spec get(state(), rt) -> ?RT:external_rt();
         (state(), rt_size) -> non_neg_integer();
         (state(), neighbors) -> nodelist:neighborhood();
         (state(), succlist) -> nodelist:non_empty_snodelist();
         (state(), succ) -> node:node_type();
         (state(), succ_id) -> ?RT:key();
         (state(), succ_pid) -> comm:mypid();
         (state(), predlist) -> nodelist:non_empty_snodelist();
         (state(), pred) -> node:node_type();
         (state(), pred_id) -> ?RT:key();
         (state(), pred_pid) -> comm:mypid();
         (state(), node) -> node:node_type();
         (state(), node_id) -> ?RT:key();
         (state(), my_range) -> intervals:interval();
         (state(), db_range) -> [{intervals:interval(), slide_op:id()}];
         (state(), succ_range) -> intervals:interval();
         (state(), join_time) -> erlang_timestamp();
         (state(), db) -> db_dht:db();
         (state(), tx_tp_db) -> any();
         (state(), proposer) -> pid();
         (state(), load) -> integer();
         (state(), slide_pred) -> slide_op:slide_op() | null;
         (state(), slide_succ) -> slide_op:slide_op() | null;
         (state(), snapshot_state) -> snapshot_state:snapshot_state() | null;
         (state(), msg_fwd) -> [{intervals:interval(), comm:mypid()}];
         (state(), rm_state) -> rm_loop:state();
         (state(), monitor_proc) -> pid();
         (state(), prbr_kv_db) -> prbr:state();
         (state(), txid_db1) -> prbr:state();
         (state(), txid_db2) -> prbr:state();
         (state(), txid_db3) -> prbr:state();
         (state(), txid_db4) -> prbr:state();
         (state(), lease_db1) -> prbr:state();
         (state(), lease_db2) -> prbr:state();
         (state(), lease_db3) -> prbr:state();
         (state(), lease_db4) -> prbr:state();
         (state(), lease_list) -> l_on_cseq:lease_list_state().
get(#state{rt=RT, rm_state=RMState, join_time=JoinTime,
           db=DB, tx_tp_db=TxTpDb, proposer=Proposer,
           slide_pred=SlidePred, slide_succ=SlideSucc,
           db_range=DBRange, monitor_proc=MonitorProc, prbr_kv_db=PRBRState,
           txid_db1=TxIdDB1, txid_db2=TxIdDB2, txid_db3=TxIdDB3, txid_db4=TxIdDB4,
           lease_db1=LeaseDB1, lease_db2=LeaseDB2, lease_db3=LeaseDB3, lease_db4=LeaseDB4, lease_list=LeaseList,
           snapshot_state=SnapState}, Key) ->
    case Key of
        rt           -> RT;
        rt_size      -> ?RT:get_size(RT);
        neighbors    -> rm_loop:get_neighbors(RMState);
        my_range     -> Neighbors = rm_loop:get_neighbors(RMState),
                        nodelist:node_range(Neighbors);
        db_range     -> DBRange;
        succ_range   -> Neighbors = rm_loop:get_neighbors(RMState),
                        nodelist:succ_range(Neighbors);
        msg_fwd      -> MsgFwdPred = slide_op:get_msg_fwd(SlidePred),
                        MsgFwdSucc = slide_op:get_msg_fwd(SlideSucc),
                        if MsgFwdPred =:= [] -> MsgFwdSucc;
                           MsgFwdSucc =:= [] -> MsgFwdPred;
                           true -> lists:append(MsgFwdPred, MsgFwdSucc)
                        end;
        db           -> DB;
        tx_tp_db     -> TxTpDb;
        proposer     -> Proposer;
        slide_pred   -> SlidePred;
        slide_succ   -> SlideSucc;
        rm_state     -> RMState;
        monitor_proc -> MonitorProc;
        snapshot_state -> SnapState;
        succlist     -> nodelist:succs(rm_loop:get_neighbors(RMState));
        succ         -> nodelist:succ(rm_loop:get_neighbors(RMState));
        succ_id      -> node:id(nodelist:succ(rm_loop:get_neighbors(RMState)));
        succ_pid     -> node:pidX(nodelist:succ(rm_loop:get_neighbors(RMState)));
        predlist     -> nodelist:preds(rm_loop:get_neighbors(RMState));
        pred         -> nodelist:pred(rm_loop:get_neighbors(RMState));
        pred_id      -> node:id(nodelist:pred(rm_loop:get_neighbors(RMState)));
        pred_pid     -> node:pidX(nodelist:pred(rm_loop:get_neighbors(RMState)));
        node         -> nodelist:node(rm_loop:get_neighbors(RMState));
        node_id      -> nodelist:nodeid(rm_loop:get_neighbors(RMState));
        join_time    -> JoinTime;
        load         -> db_dht:get_load(DB);
        prbr_kv_db   -> PRBRState;
        txid_db1     -> TxIdDB1;
        txid_db2     -> TxIdDB2;
        txid_db3     -> TxIdDB3;
        txid_db4     -> TxIdDB4;
        lease_db1    -> LeaseDB1;
        lease_db2    -> LeaseDB2;
        lease_db3    -> LeaseDB3;
        lease_db4    -> LeaseDB4;
        lease_list   -> LeaseList
    end.

-spec get_prbr_state(state(), db_selector()) -> prbr:state().
get_prbr_state(State, WhichDB) ->
    case WhichDB of
        kv -> get(State, prbr_kv_db);
        txid_1 -> get(State, txid_db1);
        txid_2 -> get(State, txid_db2);
        txid_3 -> get(State, txid_db3);
        txid_4 -> get(State, txid_db4);
        leases_1 -> get(State, lease_db1);
        leases_2 -> get(State, lease_db2);
        leases_3 -> get(State, lease_db3);
        leases_4 -> get(State, lease_db4)
    end.

-spec set_prbr_state(state(), db_selector(), prbr:state()) -> state().
set_prbr_state(State, WhichDB, Value) ->
    case WhichDB of
        kv -> State#state{prbr_kv_db = Value};
        %% tx_id ->    State#state{tx_id = Value};
        txid_1 -> State#state{txid_db1 = Value};
        txid_2 -> State#state{txid_db2 = Value};
        txid_3 -> State#state{txid_db3 = Value};
        txid_4 -> State#state{txid_db4 = Value};
        leases_1 -> State#state{lease_db1 = Value};
        leases_2 -> State#state{lease_db2 = Value};
        leases_3 -> State#state{lease_db3 = Value};
        leases_4 -> State#state{lease_db4 = Value}
    end.

-spec set_lease_list(state(), l_on_cseq:lease_list_state()) -> state().
set_lease_list(State, LeaseList) ->
    State#state{lease_list = LeaseList}.

%% @doc Checks whether the current node has already left the ring, i.e. the has
%%      already changed his ID in order to leave or jump.
-spec has_left(State::state()) -> boolean().
has_left(#state{rm_state=RMState}) ->
    rm_loop:has_left(RMState).

%% @doc Checks whether the given key is in the node's range, i.e. the node is
%%      responsible for this key.
%%      Beware of race conditions sing the neighborhood may have changed at
%%      the next call.
-spec is_responsible(Key::intervals:key(), State::state()) -> boolean().
is_responsible(Key, #state{rm_state=RMState}) ->
    rm_loop:is_responsible(Key, RMState).

%% @doc Checks whether the node is responsible for the given key either by its
%%      current range or for a range the node is temporarily responsible for
%%      during a slide operation, i.e. we temporarily read/modify data a
%%      neighbor is responsible for but hasn't yet received the data from us.
%%      Beware of race conditions sing the neighborhood may have changed at
%%      the next call.
-spec is_db_responsible(Key::intervals:key(), State::state()) -> boolean().
is_db_responsible(Key, State = #state{db_range = DBRange}) ->
    is_responsible(Key, State) orelse
        lists:any(fun({Interval, _Id}) ->
                          intervals:in(Key, Interval)
                  end, DBRange).

%% @doc Tries to find a slide operation with the given MoveFullId and returns
%%      it including its type (pred or succ) if successful and its pred/succ
%%      info is correct. Otherwise returns {fail, wrong_pred} if the
%%      predecessor info is wrong (slide with pred) and {fail, wrong_succ} if
%%      the successor info is wrong (slide with succ). If not found,
%%      {fail, not_found} is returned.
-spec get_slide(State::state(), MoveFullId::slide_op:id()) ->
        {Type::pred | succ, SlideOp::slide_op:slide_op()} |
        not_found.
get_slide(#state{slide_pred=SlidePred, slide_succ=SlideSucc}, MoveFullId) ->
    case SlidePred =/= null andalso slide_op:get_id(SlidePred) =:= MoveFullId of
        true -> {pred, SlidePred};
        _ ->
            case SlideSucc =/= null andalso slide_op:get_id(SlideSucc) =:= MoveFullId of
                true -> {succ, SlideSucc};
                _ -> not_found
            end
    end.

-spec set_tx_tp_db(State::state(), NewTxTpDb::any()) -> state().
set_tx_tp_db(State, DB) -> State#state{tx_tp_db = DB}.

-spec set_db(State::state(), NewDB::db_dht:db()) -> state().
set_db(State, DB) -> State#state{db = DB}.

-spec set_rt(State::state(), NewRT::?RT:external_rt()) -> state().
set_rt(State, RT) -> State#state{rt = RT}.

-spec set_rm(State::state(), NewRMState::rm_loop:state()) -> state().
set_rm(State, RMState) -> State#state{rm_state = RMState}.

-spec set_slide(state(), pred | succ, slide_op:slide_op() | null) -> state().
set_slide(State, pred, SlidePred) -> State#state{slide_pred=SlidePred};
set_slide(State, succ, SlideSucc) -> State#state{slide_succ=SlideSucc}.

-spec set_snapshot_state(State::state(),NewInfo::snapshot_state:snapshot_state()) -> state().
set_snapshot_state(State,NewInfo) -> State#state{snapshot_state=NewInfo}.

-spec add_db_range(State::state(), Interval::intervals:interval(),
                   SlideId::slide_op:id()) -> state().
add_db_range(State = #state{db_range=DBRange}, Interval, SlideId) ->
    false = intervals:is_all(Interval),
    ?TRACE("[ ~.0p ] add_db_range: ~.0p~n", [self(), Interval]),
    State#state{db_range = [{Interval, SlideId} | DBRange]}.

-spec rm_db_range(State::state(), SlideId::slide_op:id()) -> state().
rm_db_range(State = #state{db_range=DBRange}, SlideId) ->
    ?TRACE("[ ~.0p ] rm_db_range: ~.0p~n", [self(), [I || {I, Id} <- DBRange, Id =:= SlideId]]),
    State#state{db_range = [X || X = {_, Id} <- DBRange, Id =/= SlideId]}.

-spec add_bulkowner_reply_msg(State::state(), Id::uid:global_uid(), Target::comm:mypid(),
                              Msg::comm:message(), Parents::[comm:mypid()]) -> state().
add_bulkowner_reply_msg(State = #state{bulkowner_reply_ids = IDs}, Id, Target, Msg, Parents) ->
    PrevMsgs = case erlang:get({'$bulkowner_reply_msg', Id}) of
                   undefined    -> [];
                   {_, _, X, _} -> X
               end,
    % parent, target information should be the same - use the latest
    _ = erlang:put({'$bulkowner_reply_msg', Id}, {Id, Target, [Msg | PrevMsgs], Parents}),
    State#state{bulkowner_reply_ids = [Id | IDs]}.

-spec take_bulkowner_reply_msgs(State::state())
        -> {state(), [{Id::uid:global_uid(), Target::comm:mypid(),
                       Msgs::[comm:message()], Parents::[comm:mypid()]}]}.
take_bulkowner_reply_msgs(State = #state{bulkowner_reply_ids = IDs}) ->
    {State#state{bulkowner_reply_ids = []},
     [erlang:erase({'$bulkowner_reply_msg', Id}) || Id <- IDs]}.

-spec get_bulkowner_reply_timer(State::state()) -> null | reference().
get_bulkowner_reply_timer(#state{bulkowner_reply_timer = null}) ->
    null;
get_bulkowner_reply_timer(#state{bulkowner_reply_timer = Timer}) ->
    case erlang:read_timer(Timer) of
        false -> null;
        _     -> Timer
    end.

-spec set_bulkowner_reply_timer(State::state(), Timer::null | reference()) -> state().
set_bulkowner_reply_timer(State, Timer) ->
    State#state{bulkowner_reply_timer = Timer}.

%%% util
-spec dump(state()) -> ok.
dump(State) ->
    io:format("dump <~s,~w> <~s,~w> <~s,~w>~n",
              [get(State, node_id), self(),
               get(State, pred_id), get(State, pred_pid),
               get(State, succ_id), get(State, succ_pid)]),
    ok.

%% @doc Gets the requested details about the current node.
-spec details(state(), [node_details:node_details_name()]) -> node_details:node_details().
details(State, Which) ->
    ExtractValues =
        fun(Elem, NodeDetails) ->
                case Elem of
                    hostname    -> node_details:set(NodeDetails, hostname, net_adm:localhost());
                    message_log -> node_details:set(NodeDetails, message_log, ok);
                    memory      -> node_details:set(NodeDetails, memory, erlang:memory(total));
                    is_leaving  -> node_details:set(NodeDetails, Elem, rm_loop:has_left(get(State, rm_state)));
                    Tag         -> node_details:set(NodeDetails, Tag, get(State, Tag))
                end
        end,
    lists:foldl(ExtractValues, node_details:new(), Which).

%% @doc Gets the following details about the current node:
%%      predecessor and successor lists, the node itself, its load, hostname,
%%      routing table size, memory usage.
-spec details(state()) -> node_details:node_details().
details(State) ->
    Neighbors = get(State, neighbors),
    PredList = nodelist:preds(Neighbors),
    SuccList = nodelist:succs(Neighbors),
    Node = nodelist:node(Neighbors),
    Load = get(State, load),
    Hostname = net_adm:localhost(),
    RTSize = get(State, rt_size),
    node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, erlang:memory(total)).

%% @doc Gets all entries to transfer (slide) in the given range and starts delta
%%      recording on the DB for changes in this interval.
-spec slide_get_data_start_record(state(), MovingInterval::intervals:interval())
        -> {state(), slide_data()}.
slide_get_data_start_record(State, MovingInterval) ->
    OldDB = get(State, db),
    MovingData = db_dht:get_entries(OldDB, MovingInterval),
    MovingSnapData = case db_dht:snapshot_is_running(OldDB) of
        true ->
            {get(State,snapshot_state), db_dht:get_snapshot_data(OldDB, MovingInterval)};
        false ->
            {false}
    end,
    NewDB = db_dht:record_changes(OldDB, MovingInterval),
    ?TRACE("~p:slide_get_data_start_record: ~p~nMovingData: ~n~p~nMovingSnapData: ~n~p~nfor
           interval ~p~n~p~n~p",
           [?MODULE, comm:this(), MovingData, MovingSnapData, MovingInterval, OldDB, NewDB]),
    {set_db(State, NewDB), {MovingData, MovingSnapData}}.

%% @doc Adds data from slide_get_data_start_record/2 to the local DB.
-spec slide_add_data(state(),slide_data()) -> state().
slide_add_data(State, {Data, SnapData}) ->
    NewDB = db_dht:add_data(get(State, db), Data),
    ?TRACE("~p:slide_add_data: ~p~nMovingData:~n~p~nMovingSnapData: ~n~p~n~p",
           [?MODULE, comm:this(), Data, SnapData, NewDB]),
    case SnapData of
        {SnapState, SnapEntries} ->
            NewState = set_db(State,
                              db_dht:add_snapshot_data(db_dht:init_snapshot(NewDB),
                                                    SnapEntries)),
            set_snapshot_state(NewState, SnapState);
        {false} ->
            set_db(State, NewDB)
    end.

%% @doc Gets all DB changes in the given interval, stops recording delta infos
%%      and removes the entries in this range from the DB.
-spec slide_take_delta_stop_record(state(), MovingInterval::intervals:interval())
        -> {state(), slide_delta()}.
slide_take_delta_stop_record(State, MovingInterval) ->
    OldDB = get(State, db),
    ChangedData = db_dht:get_changes(OldDB, MovingInterval),
    NewState = slide_stop_record(State, MovingInterval, true),
    ?TRACE("~p:slide_take_delta_stop_record: ~p~nChangedData: ~n~p~n~p",
           [?MODULE, comm:this(), ChangedData, get(NewState, db)]),
    {NewState, ChangedData}.

%% @doc Adds delta infos from slide_take_delta_stop_record/2 to the local DB.
-spec slide_add_delta(state(), slide_delta()) -> state().
slide_add_delta(State, {ChangedData, DeletedKeys}) ->
    NewDB1 = db_dht:add_data(get(State, db), ChangedData),
    NewDB2 = db_dht:delete_entries(NewDB1, intervals:from_elements(DeletedKeys)),
    ?TRACE("~p:slide_add_delta: ~p~nChangedData: ~n~p~n~p",
           [?MODULE, comm:this(), {ChangedData, DeletedKeys}, NewDB2]),
    set_db(State, NewDB2).

%% @doc Stops recording changes in the given interval.
%%      Optionally, the data in this range can be deleted.
-spec slide_stop_record(state(), MovingInterval::intervals:interval(),
                        RemoveDataInInterval::boolean()) -> state().
slide_stop_record(State, MovingInterval, Remove) ->
    NewDB1 = db_dht:stop_record_changes(get(State, db), MovingInterval),
    NewDB = if Remove -> db_dht:delete_entries(NewDB1, MovingInterval);
               true   -> NewDB1
            end,
    set_db(State, NewDB).

%% @doc Returns a key so that there are no more than TargetLoad entries
%%      between Begin and this key in the DBs.
-spec get_split_key(state(), Begin::?RT:key(), End::?RT:key(), TargetLoad::pos_integer(), forward | backward)
        -> {?RT:key(), TakenLoad::pos_integer()}.
get_split_key(State, Begin, End, TargetLoad, Direction) ->
    db_dht:get_split_key(get(State, db), Begin, End, TargetLoad, Direction).
