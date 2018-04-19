%  @copyright 2007-2018 Zuse Institute Berlin

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
%% @doc    dht_node main file
%% @end
%% @version $Id$
-module(dht_node).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("client_types.hrl").
-include("lookup.hrl").

-behaviour(gen_component).

-export([start_link/2, on/2, init/1]).

-export([is_first/1, is_alive/1, is_alive_no_slide/1, is_alive_fully_joined/1]).

-export_type([message/0]).

-include("gen_component.hrl").

-type(database_message() ::
      {?get_key, Source_PID::comm:mypid(), SourceId::any(), HashedKey::?RT:key()} |
      {get_entries, Source_PID::comm:mypid(), Interval::intervals:interval()} |
      {get_entries, Source_PID::comm:mypid(), FilterFun::fun((db_entry:entry()) -> boolean()),
            ValFun::fun((db_entry:entry()) -> any())} |
      {get_chunk, Source_PID::comm:mypid(), Interval::intervals:interval(), MaxChunkSize::pos_integer() | all} |
      {get_chunk, Source_PID::comm:mypid(), Interval::intervals:interval(), FilterFun::fun((db_entry:entry()) -> boolean()),
            ValFun::fun((db_entry:entry()) -> any()), MaxChunkSize::pos_integer() | all} |
      {update_key_entries, Source_PID::comm:mypid(), [{HashedKey::?RT:key(), NewValue::db_dht:value(), NewVersion::client_version()}]} |
%%      % DB subscriptions:
%%      {db_set_subscription, SubscrTuple::db_dht:subscr_t()} |
%%      {db_get_subscription, Tag::any(), SourcePid::comm:erl_local_pid()} |
%%      {db_remove_subscription, Tag::any()} |
      % direct DB manipulation:
      {get_key_entry, Source_PID::comm:mypid(), HashedKey::?RT:key()} |
      {set_key_entry, Source_PID::comm:mypid(), Entry::db_entry:entry()} |
      {delete_key, Source_PID::comm:mypid(), ClientsId::{delete_client_id, uid:global_uid()}, HashedKey::?RT:key()} |
      {add_data, Source_PID::comm:mypid(), db_dht:db_as_list()} |
      {drop_data, Data::db_dht:db_as_list(), Sender::comm:mypid()}).

-type(lookup_message() ::
      {?lookup_aux, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()} |
      {?lookup_fin, Key::?RT:key(), Data::dht_node_lookup:data(), Msg::comm:message()}).

-type(snapshot_message() ::
      {do_snapshot, SnapNumber::non_neg_integer(), Leader::comm:mypid()} |
      {local_snapshot_is_done}).

-type(rt_message() ::
      {rt_update, RoutingTable::?RT:external_rt()}).

-type(misc_message() ::
      {get_yaws_info, Pid::comm:mypid()} |
      {get_state, Pid::comm:mypid(), Which::dht_node_state:name()} |
      {get_node_details, Pid::comm:mypid()} |
      {get_node_details, Pid::comm:mypid(), Which::[node_details:node_details_name()]} |
      {get_pid_group, Pid::comm:mypid()} |
      {dump} |
      {web_debug_info, Requestor::comm:erl_local_pid()} |
      {get_dht_nodes_response, KnownHosts::[comm:mypid()]} |
      {unittest_get_bounds_and_data, SourcePid::comm:mypid(), full | kv}).

% accepted messages of dht_node processes
-type message() ::
    bulkowner:bulkowner_msg() |
    database_message() |
    lookup_message() |
    dht_node_join:join_message() |
    rt_message() |
    dht_node_move:move_message() |
    misc_message() |
    snapshot_message() |
    {zombie, Node::node:node_type()} |
    {fd_notify, fd:event(), DeadPid::comm:mypid(), Reason::fd:reason()} |
    {leave, SourcePid::comm:erl_local_pid() | null} |
    {rejoin, IdVersion::non_neg_integer(), JoinOptions::[tuple()],
      {get_move_state_response, MoveState::[tuple()]}}.

%% @doc message handler
-spec on(message(), dht_node_state:state()) -> dht_node_state:state() | kill.
%% Join messages (see dht_node_join.erl)
%% userdevguide-begin dht_node:join_message
on(Msg, State) when join =:= element(1, Msg) ->
    lb_stats:set_ignore_db_requests(true),
    NewState = dht_node_join:process_join_msg(Msg, State),
    lb_stats:set_ignore_db_requests(false),
    NewState;
%% userdevguide-end dht_node:join_message

% Move messages (see dht_node_move.erl)
on(Msg, State) when move =:= element(1, Msg) ->
    lb_stats:set_ignore_db_requests(true),
    NewState = dht_node_move:process_move_msg(Msg, State),
    lb_stats:set_ignore_db_requests(false),
    NewState;

% Lease management messages (see l_on_cseq.erl)
on(Msg, State) when l_on_cseq =:= element(1, Msg) ->
    l_on_cseq:on(Msg, State);

% DHT node extensions (see dht_node_extensions.erl)
on(Msg, State) when extensions =:= element(1, Msg) ->
    dht_node_extensions:on(Msg, State);

% RM messages (see rm_loop.erl)
on(Msg, State) when element(1, Msg) =:= rm ->
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:on(Msg, RMState),
    dht_node_state:set_rm(State, RMState1);

on({leave, SourcePid}, State) ->
    dht_node_move:make_slide_leave(State, SourcePid);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rt_update, RoutingTable}, State) ->
    dht_node_state:set_rt(State, RoutingTable);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Transactions (see transactions/*.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_rtm, Source_PID, Key, Process}, State) ->
    case pid_groups:get_my(Process) of
        failed ->
            R = config:read(replication_factor),
            case Process of
                {tx_rtm,X} when X =< R ->
                    %% these rtms are concurrently started by the supervisor
                    %% we just have to wait a bit...
                    comm:send_local(self(),
                                    {get_rtm, Source_PID, Key, Process});
                _ ->
                    log:log(warn, "[ ~.0p ] requested non-existing rtm ~.0p~n",
                            [comm:this(), Process])
            end;
        Pid ->
            GPid = comm:make_global(Pid),
            GPidAcc = comm:make_global(tx_tm_rtm:get_my(Process, acceptor)),
            comm:send(Source_PID, {get_rtm_reply, Key, GPid, GPidAcc})
    end,
    State;

%% messages handled as a transaction participant (TP)
on({?init_TP, {_Tid, _RTMs, _Accs, _TM, _RTLogEntry, _ItemId, _PaxId, SnapNo} = Params}, State) ->
    % check if new snapshot
    SnapState = dht_node_state:get(State,snapshot_state),
    LocalSnapNumber = snapshot_state:get_number(SnapState),
    case SnapNo > LocalSnapNumber of
        true ->
            comm:send(comm:this(), {do_snapshot, SnapNo, none});
        false ->
            ok
    end,
    tx_tp:on_init_TP(Params, State);
on({?tp_do_commit_abort, Id, Result, SnapNumber}, State) ->
    tx_tp:on_do_commit_abort(Id, Result, SnapNumber, State);
on({?tp_do_commit_abort_fwd, TM, TMItemId, RTLogEntry, Result, OwnProposal, SnapNumber}, State) ->
    tx_tp:on_do_commit_abort_fwd(TM, TMItemId, RTLogEntry, Result, OwnProposal, SnapNumber, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Lookup (see api_dht_raw.erl and dht_node_lookup.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({?lookup_aux, Key, Hops, Msg}=FullMsg, State) ->
    %% Forward msg to the routing_table (rt_loop).
    %% If possible, messages should be sent directly to routing_table (rt_loop).
    %% Only forward messages for which we aren't responsible (leases).
    %% log:pal("lookup_aux_leases in dht_node"),
    case config:read(leases) of
        true ->
            %% Check lease and translate to lookup_fin or forward to rt_loop
            %% accordingly.
            dht_node_lookup:lookup_aux_leases(State, Key, Hops, Msg);
        _ ->
            %% simply forward the message to routing_table (rt_loop)
            comm:send_local(pid_groups:get_my(routing_table), FullMsg)
    end,
    State;

on({lookup_decision, Key, Hops, Msg}, State) ->
    %% message from rt_loop requesting a decision about a aux-fin transformation
    %% (chord only)
    dht_node_lookup:lookup_decision_chord(State, Key, Hops, Msg),
    State;

on({?lookup_fin, Key, Data, Msg}, State) ->
    dht_node_lookup:lookup_fin(State, Key, Data, Msg);

on({send_error, Target, {?lookup_aux, _, _, _} = Message, _Reason}, State) ->
    dht_node_lookup:lookup_aux_failed(State, Target, Message);

on({send_error, Target, {?send_to_group_member, routing_table,
                         {?lookup_aux, _Key, _Hops, _Msg} = Message}, _Reason}, State) ->
    dht_node_lookup:lookup_aux_failed(State, Target, Message);

%on({send_failed, {send_error, Target, {?lookup_aux, _, _, _}} = Message, _Reason}, _Pids}}, State) ->
%    dht_node_lookup:lookup_aux_failed(State, Target, Message);
on({send_error, Target, {?lookup_fin, _, _, _} = Message, _Reason}, State) ->
    dht_node_lookup:lookup_fin_failed(State, Target, Message);

%% messages handled as a prbr
on(X, State) when is_tuple(X) andalso element(1, X) =:= prbr ->
    %% as prbr has several use cases (may operate on different DBs) in
    %% the dht_node, the addressed use case is given in the third
    %% element by convention.
    DBKind = element(3, X),
    PRBRState = dht_node_state:get(State, DBKind),
    NewRBRState = prbr:on(X, PRBRState),
    dht_node_state:set_prbr_state(State, DBKind, NewRBRState);

on(X, State) when is_tuple(X) andalso element(1, X) =:= crdt_acceptor ->
    CrdtState = dht_node_state:get(State, crdt_db),
    NewCrdtState = crdt_acceptor:on(X, CrdtState),
    dht_node_state:set_prbr_state(State, crdt_db, NewCrdtState);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Database
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({?get_key, Source_PID, SourceId, HashedKey}, State) ->
    Msg = {?get_key_with_id_reply, SourceId, HashedKey,
           db_dht:read(dht_node_state:get(State, db), HashedKey)},
    comm:send(Source_PID, Msg),
    State;

on({?read_op, Source_PID, SourceId, HashedKey, Op}, State) ->
    DB = dht_node_state:get(State, db),
    {ok, Value, Version} = db_dht:read(DB, HashedKey),
    {Ok_Fail, Val_Reason, Vers} = rdht_tx_read:extract_from_value(Value, Version, Op),
    SnapInfo = dht_node_state:get(State, snapshot_state),
    SnapNumber = snapshot_state:get_number(SnapInfo),
    Msg = {?read_op_with_id_reply, SourceId, SnapNumber, Ok_Fail, Val_Reason, Vers},
    comm:send(Source_PID, Msg),
    State;

on({get_entries, Source_PID, Interval}, State) ->
    Entries = db_dht:get_entries(dht_node_state:get(State, db), Interval),
    comm:send_local(Source_PID, {get_entries_response, Entries}),
    State;

on({get_entries, Source_PID, FilterFun, ValFun}, State) ->
    Entries = db_dht:get_entries(dht_node_state:get(State, db), FilterFun, ValFun),
    comm:send_local(Source_PID, {get_entries_response, Entries}),
    State;

on({get_data, Source_PID}, State) ->
    Data = db_dht:get_data(dht_node_state:get(State, db)),
    comm:send_local(Source_PID, {get_data_response, Data}),
    State;

on({get_data, Source_PID, FilterFun, ValueFun}, State) ->
    Data = db_dht:get_data(dht_node_state:get(State, db), FilterFun, ValueFun),
    comm:send_local(Source_PID, {get_data_response, Data}),
    State;

on({get_chunk, Source_PID, Interval, MaxChunkSize}, State) ->
    Chunk = db_dht:get_chunk(dht_node_state:get(State, db),
                             dht_node_state:get(State, pred_id),
                          Interval, MaxChunkSize),
    comm:send_local(Source_PID, {get_chunk_response, Chunk}),
    State;

on({get_chunk, Source_PID, Interval, FilterFun, ValueFun, MaxChunkSize}, State) ->
    Chunk = db_dht:get_chunk(dht_node_state:get(State, db),
                             dht_node_state:get(State, pred_id),
                          Interval, FilterFun, ValueFun, MaxChunkSize),
    comm:send_local(Source_PID, {get_chunk_response, Chunk}),
    State;

on({update_key_entries, Source_PID, KvvList}, State) ->
    DB = dht_node_state:get(State, db),
    {NewDB, NewEntryList} = update_key_entries(KvvList, DB, State, []),
    % send caller update_key_entries_ack with list of {Entry, Exists (Yes/No), Updated (Yes/No)}
    comm:send(Source_PID, {update_key_entries_ack, NewEntryList}),
    dht_node_state:set_db(State, NewDB);

%%on({db_set_subscription, SubscrTuple}, State) ->
%%    DB2 = db_dht:set_subscription(dht_node_state:get(State, db), SubscrTuple),
%%    dht_node_state:set_db(State, DB2);
%%
%%on({db_get_subscription, Tag, SourcePid}, State) ->
%%    Subscr = db_dht:get_subscription(dht_node_state:get(State, db), Tag),
%%    comm:send_local(SourcePid, {db_get_subscription_response, Tag, Subscr}),
%%    State;
%%
%%on({db_remove_subscription, Tag}, State) ->
%%    DB2 = db_dht:remove_subscription(dht_node_state:get(State, db), Tag),
%%    dht_node_state:set_db(State, DB2);

on({delete_key, Source_PID, ClientsId, HashedKey}, State) ->
    {DB2, Result} = db_dht:delete(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {delete_key_response, ClientsId, HashedKey, Result}),
    dht_node_state:set_db(State, DB2);

%% for unit testing only: allow direct DB manipulation
on({get_key_entry, Source_PID, HashedKey}, State) ->
    Entry = db_dht:get_entry(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {get_key_entry_reply, Entry}),
    State;

on({set_key_entry, Source_PID, Entry}, State) ->
    NewDB = db_dht:set_entry(dht_node_state:get(State, db), Entry),
    comm:send(Source_PID, {set_key_entry_reply, Entry}),
    dht_node_state:set_db(State, NewDB);

on({add_data, Source_PID, Data}, State) ->
    NewDB = db_dht:add_data(dht_node_state:get(State, db), Data),
    comm:send(Source_PID, {add_data_reply}),
    dht_node_state:set_db(State, NewDB);

on({delete_keys, Source_PID, HashedKeys}, State) ->
    DB2 = db_dht:delete_entries(dht_node_state:get(State, db), intervals:from_elements(HashedKeys)),
    comm:send(Source_PID, {delete_keys_reply}),
    dht_node_state:set_db(State, DB2);

on({drop_data, Data, Sender}, State) ->
    comm:send(Sender, {drop_data_ack}),
    DB = db_dht:add_data(dht_node_state:get(State, db), Data),
    dht_node_state:set_db(State, DB);

on({get_split_key, DB, Begin, End, TargetLoad, Direction, Sender}, State) ->
    comm:send_local(Sender, {get_split_key_response,
                             db_dht:get_split_key(DB, Begin, End,
                                                  TargetLoad, Direction)}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Bulk owner messages (see bulkowner.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on(Msg, State) when bulkowner =:= element(1, Msg) ->
    bulkowner:on(Msg, State);

on({send_error, _FailedTarget, FailedMsg, _Reason} = Msg, State)
  when bulkowner =:= element(1, FailedMsg) ->
    bulkowner:on(Msg, State);

on({bulk_distribute, _Id, _Range, InnerMsg, _Parents} = Msg, State)
  when mr =:= element(1, InnerMsg) ->
    mr:on(Msg, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% map reduce related messages (see mr.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on(Msg, State) when mr =:= element(1, Msg) ->
    mr:on(Msg, State);

on(Msg, State) when mr_master =:= element(1, Msg) ->
    try
        mr_master:on(Msg, State)
    catch
        error:function_clause ->
            log:log(warn, "Received Message to non-existing master...ignoring!"),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% active load balancing messages (see lb_active_*.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on(Msg, State) when lb_active =:= element(1, Msg) ->
    lb_active:handle_dht_msg(Msg, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% handling of failed sends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_yaws_info, Pid}, State) ->
    comm:send(Pid, {get_yaws_info_response, comm:get_ip(comm:this()), config:read(yaws_port), pid_groups:my_groupname()}),
    State;
on({get_state, Pid, Which}, State) when is_list(Which) ->
    comm:send(Pid, {get_state_response,
                    [{X, dht_node_state:get(State, X)} || X <- Which]}),
    State;
on({get_state, Pid, Which}, State) when is_atom(Which) ->
    comm:send(Pid, {get_state_response, dht_node_state:get(State, Which)}),
    State;
on({set_state, Pid, F}, State) when is_function(F) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    NewState = F(State),
    comm:send(Pid, {set_state_response, NewState}),
    NewState;
on({get_node_details, Pid}, State) ->
    comm:send(Pid, {get_node_details_response, dht_node_state:details(State)}),
    State;
on({get_node_details, Pid, Which}, State) ->
    comm:send(Pid, {get_node_details_response, dht_node_state:details(State, Which)}),
    State;
on({get_pid_group, Pid}, State) ->
    comm:send(Pid, {get_pid_group_response, pid_groups:my_groupname()}),
    State;

on({dump}, State) ->
    dht_node_state:dump(State),
    State;

on({web_debug_info, Requestor}, State) ->
    RMState = dht_node_state:get(State, rm_state),
    Load = dht_node_state:get(State, load),
    % get a list of up to 50 KV pairs to display:
    DataListTmp = [{"",
                    webhelpers:safe_html_string("~p", [DBEntry])}
                  || DBEntry <- element(2, db_dht:get_chunk(dht_node_state:get(State, db),
                                                         dht_node_state:get(State, node_id),
                                                         intervals:all(), 50))],
    DataList = case Load > 50 of
                   true  -> lists:append(DataListTmp, [{"...", ""}]);
                   false -> DataListTmp
               end,
    KVList1 =
        [{"rt_algorithm", webhelpers:safe_html_string("~p", [?RT])},
         {"rt_size", dht_node_state:get(State, rt_size)},
         {"my_range", webhelpers:safe_html_string("~p", [intervals:get_bounds(dht_node_state:get(State, my_range))])},
         {"db_range", webhelpers:safe_html_string("~p", [dht_node_state:get(State, db_range)])},
         {"load", webhelpers:safe_html_string("~p", [Load])},
         {"join_time", webhelpers:safe_html_string("~p UTC", [calendar:now_to_universal_time(dht_node_state:get(State, join_time))])},
%%          {"db", webhelpers:safe_html_string("~p", [dht_node_state:get(State, db)])},
%%          {"proposer", webhelpers:safe_html_string("~p", [pid_groups:get_my(paxos_proposer)])},
         {"tx_tp_db", webhelpers:safe_html_string("~p", [dht_node_state:get(State, tx_tp_db)])},
         {"slide_pred", webhelpers:safe_html_string("~p", [dht_node_state:get(State, slide_pred)])},
         {"slide_succ", webhelpers:safe_html_string("~p", [dht_node_state:get(State, slide_succ)])},
         {"msg_fwd", webhelpers:safe_html_string("~p", [dht_node_state:get(State, msg_fwd)])}
        ],
    KVList2 = lists:append(KVList1, [{"", ""} | rm_loop:get_web_debug_info(RMState)]),
    KVList3 = lists:append(KVList2, [{"", ""} , {"data (db_entry):", ""} | DataList]),
    comm:send_local(Requestor, {web_debug_info_reply, KVList3}),
    State;

on({unittest_get_bounds_and_data, SourcePid, Type}, State) ->
    MyRange = dht_node_state:get(State, my_range),
    MyBounds = intervals:get_bounds(MyRange),
    DB = dht_node_state:get(State, db),
    Data =
        case Type of
            kv ->
                element(
                  2,
                  db_dht:get_chunk(
                    DB, ?MINUS_INFINITY, intervals:all(),
                    fun(_) -> true end,
                    fun(E) -> {db_entry:get_key(E), db_entry:get_version(E)} end,
                    all));
            full ->
                db_dht:get_data(DB)
        end,
    Pred = dht_node_state:get(State, pred),
    Succ = dht_node_state:get(State, succ),
    comm:send(SourcePid, {unittest_get_bounds_and_data_response, MyBounds, Data, Pred, Succ}),
    State;

on({unittest_consistent_send, Pid, _X} = Msg, State) ->
    ?ASSERT(util:is_unittest()),
    comm:send_local(Pid, Msg),
    State;

on({get_dht_nodes_response, _KnownHosts}, State) ->
    % will ignore these messages after join
    State;

on({fd_notify, Event, DeadPid, Data}, State) ->
    % TODO: forward to further integrated modules, e.g. join?
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:fd_notify(RMState, Event, DeadPid, Data),
    dht_node_state:set_rm(State, RMState1);

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, State) ->
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:zombie_node(RMState, Node),
    % TODO: call other modules, e.g. join, move
    dht_node_state:set_rm(State, RMState1);

on({do_snapshot, SnapNumber, Leader}, State) ->
    snapshot:on_do_snapshot(SnapNumber, Leader, State);

on({local_snapshot_is_done}, State) ->
    snapshot:on_local_snapshot_is_done(State);

on({ping, Pid, Msg}, State) ->
    comm:send(Pid, Msg),
    State;

on({rejoin, Id, Options, {get_move_state_response, MoveState}}, State) ->
    % clean up RM, e.g. fd subscriptions:
    rm_loop:cleanup(dht_node_state:get(State, rm_state)),
    %% start new join
    comm:send_local(self(), {join, start}),
    JoinOptions = [{move_state, MoveState} | Options],
    IdVersion = node:id_version(dht_node_state:get(State, node)),
    dht_node_state:delete_for_rejoin(State), % clean up state!
    dht_node_join:join_as_other(Id, IdVersion+1, JoinOptions).

%% userdevguide-begin dht_node:start
%% @doc joins this node in the ring and calls the main loop
-spec init(Options::[tuple()])
        -> dht_node_state:state() |
           {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::dht_node_join:join_state()}.
init(Options) ->
    {my_sup_dht_node_id, MySupDhtNode} = lists:keyfind(my_sup_dht_node_id, 1, Options),
    erlang:put(my_sup_dht_node_id, MySupDhtNode),
    % start trigger here to prevent infection when tracing e.g. node joins
    % (otherwise the trigger would be started at the end of the join and thus
    % be infected forever)
    % NOTE: any trigger started here, needs an exception for queuing messages
    %       in dht_node_join to prevent infection with msg_queue:send/1!
    rm_loop:init_first(),
    dht_node_move:send_trigger(),

    Recover = config:read(start_type) =:= recover,
    dht_node_extensions:init(Options),
    case {is_first(Options), config:read(leases), Recover, is_add_nodes(Options)} of
        {_   , true, true, false} ->
            % we are recovering
            dht_node_join_recover:join(Options);
        {true, true, false, _} ->
            msg_delay:send_trigger(1, {l_on_cseq, renew_leases}),
            Id = l_on_cseq:id(intervals:all()),
            TmpState = dht_node_join:join_as_first(Id, 0, Options),
            %% we have to inject the first lease by hand, as otherwise
            %% no routing will work.
            l_on_cseq:add_first_lease_to_db(Id, TmpState);
        {false, true, _, true} ->
            msg_delay:send_trigger(1, {l_on_cseq, renew_leases}),
            % get my ID (if set, otherwise chose a random ID):
            Id = case lists:keyfind({dht_node, id}, 1, Options) of
                     {{dht_node, id}, IdX} -> IdX;
                     _ -> ?RT:get_random_node_id()
                 end,
            dht_node_join:join_as_other(Id, 0, Options);
        {IsFirst, _, _, _} ->
            % get my ID (if set, otherwise chose a random ID):
            Id = case lists:keyfind({dht_node, id}, 1, Options) of
                     {{dht_node, id}, IdX} -> IdX;
                     _ -> ?RT:get_random_node_id()
                 end,
            case {IsFirst, modr:is_enabled()} of
                {true, _} -> dht_node_join:join_as_first(Id, 0, Options);
                {false, false} -> dht_node_join:join_as_other(Id, 0, Options);
                {false, true} -> %% disable passive lb during join operation
                    dht_node_join:join_as_other(Id, 0, [{skip_psv_lb} | Options])
            end
    end.
%% userdevguide-end dht_node:start

%% userdevguide-begin dht_node:start_link
%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node},
                              {wait_for_init},
                              {spawn_opts, [{fullsweep_after, 0},
                                            {min_heap_size, 131071}]}]).
%% userdevguide-end dht_node:start_link

%% @doc Checks whether this VM is marked as first, e.g. in a unit test, and
%%      this is the first node in this VM.
-spec is_first([tuple()]) -> boolean().
is_first(Options) ->
    lists:member({first}, Options) andalso admin_first:is_first_vm().

-spec is_add_nodes([tuple()]) -> boolean().
is_add_nodes(Options) ->
    lists:member({add_node}, Options).

-spec is_alive(State::dht_node_join:join_state() | dht_node_state:state() | term()) -> boolean().
is_alive(State) ->
    erlang:is_tuple(State) andalso element(1, State) =:= state.

-spec is_alive_no_slide(State::dht_node_join:join_state() | dht_node_state:state() | term()) -> boolean().
is_alive_no_slide(State) ->
    try
        SlidePred = dht_node_state:get(State, slide_pred), % note: this also tests dht_node_state:state()
        SlideSucc = dht_node_state:get(State, slide_succ),
        SlidePred =:= null andalso SlideSucc =:= null
    catch _:_ -> false
    end.

-spec is_alive_fully_joined(State::dht_node_join:join_state() | dht_node_state:state() | term()) -> boolean().
is_alive_fully_joined(State) ->
    try
        SlidePred = dht_node_state:get(State, slide_pred), % note: this also tests dht_node_state:state()
        (SlidePred =:= null orelse not slide_op:is_join(SlidePred, 'rcv'))
    catch _:_ -> false
    end.

-spec update_key_entries(Entries::[{?RT:key(), db_dht:value(), client_version()}],
                         DB, dht_node_state:state(), NewEntries) -> {DB, NewEntries}
    when is_subtype(DB, db_dht:db()),
         is_subtype(NewEntries, [{db_entry:entry(), Exists::boolean(), Done::boolean()}]).
update_key_entries([], DB, _State, NewEntries) ->
    {DB, lists:reverse(NewEntries)};
update_key_entries([{Key, NewValue, NewVersion} | Entries], DB, State, NewEntries) ->
    IsResponsible = dht_node_state:is_db_responsible(Key, State),
    Entry = db_dht:get_entry(DB, Key),
    Exists = not db_entry:is_null(Entry),
    EntryVersion = db_entry:get_version(Entry),
    WL = db_entry:get_writelock(Entry),
    DoUpdate = Exists
                   andalso EntryVersion =/= -1
                   andalso EntryVersion < NewVersion
                   andalso (WL =:= false orelse WL < NewVersion)
                   andalso IsResponsible,
    DoRegen = (not Exists) andalso IsResponsible,
%%     log:pal("update_key_entries:~nold: ~p~nnew: ~p~nDoUpdate: ~w, DoRegen: ~w",
%%             [{db_entry:get_key(Entry), db_entry:get_version(Entry)},
%%              {Key, NewVersion}, DoUpdate, DoRegen]),
    if
        DoUpdate ->
            UpdEntry = db_entry:set_value(Entry, NewValue, NewVersion),
            NewEntry = if WL < NewVersion -> db_entry:reset_locks(UpdEntry);
                          true -> UpdEntry
                       end,
            NewDB = db_dht:update_entry(DB, NewEntry),
            ok;
        DoRegen ->
            NewEntry = db_entry:new(Key, NewValue, NewVersion),
            NewDB = db_dht:set_entry(DB, NewEntry),
            ok;
        true ->
            NewDB = DB,
            NewEntry = Entry,
            ok
    end,
    update_key_entries(Entries, NewDB, State,
                       [{NewEntry, Exists, DoUpdate orelse DoRegen} | NewEntries]).
