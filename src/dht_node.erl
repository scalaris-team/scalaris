%  @copyright 2007-2012 Zuse Institute Berlin

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
-behaviour(gen_component).

-export([start_link/2, on/2, init/1]).

-export([is_first/1, is_alive/1, is_alive_no_slide/1, is_alive_fully_joined/1]).

-ifdef(with_export_type_support).
-export_type([message/0]).
-endif.

-type(database_message() ::
      {?get_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {?get_key, Source_PID::comm:mypid(), SourceId::any(), HashedKey::?RT:key()} |
      {get_entries, Source_PID::comm:mypid(), Interval::intervals:interval()} |
      {get_entries, Source_PID::comm:mypid(), FilterFun::fun((db_entry:entry()) -> boolean()),
            ValFun::fun((db_entry:entry()) -> any())} |
      {get_chunk, Source_PID::comm:mypid(), Interval::intervals:interval(), MaxChunkSize::pos_integer() | all} |
      {get_chunk, Source_PID::comm:mypid(), Interval::intervals:interval(), FilterFun::fun((db_entry:entry()) -> boolean()),
            ValFun::fun((db_entry:entry()) -> any()), MaxChunkSize::pos_integer() | all} |
      {update_key_entry, Source_PID::comm:mypid(), HashedKey::?RT:key(), NewValue::?DB:value(), NewVersion::?DB:version()} |
      % DB subscriptions:
      {db_set_subscription, SubscrTuple::?DB:subscr_t()} |
      {db_get_subscription, Tag::any(), SourcePid::comm:erl_local_pid()} |
      {db_remove_subscription, Tag::any()} |
      % direct DB manipulation:
      {get_key_entry, Source_PID::comm:mypid(), HashedKey::?RT:key()} |
      {set_key_entry, Source_PID::comm:mypid(), Entry::db_entry:entry()} |
      {delete_key, Source_PID::comm:mypid(), ClientsId::{delete_client_id, uid:global_uid()}, HashedKey::?RT:key()} |
      {add_data, Source_PID::comm:mypid(), ?DB:db_as_list()} |      
      {drop_data, Data::?DB:db_as_list(), Sender::comm:mypid()}).

-type(lookup_message() ::
      {?lookup_aux, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()} |
      {?lookup_fin, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()}).

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
      {unittest_get_bounds_and_data, SourcePid::comm:mypid()}).

% accepted messages of dht_node processes
-type message() ::
    bulkowner:bulkowner_msg() |
    database_message() |
    lookup_message() |
    dht_node_join:join_message() |
    rt_message() |
    dht_node_move:move_message() |
    misc_message() |
    {zombie, Node::node:node_type()} |
    {crash, DeadPid::comm:mypid()} |
    {crash, DeadPid::comm:mypid(), Cookie::tuple()} |
    {leave, SourcePid::comm:erl_local_pid() | null}.

%% @doc message handler
-spec on(message(), dht_node_state:state()) -> dht_node_state:state() | kill.
%% Join messages (see dht_node_join.erl)
%% userdevguide-begin dht_node:join_message
on(Msg, State) when join =:= element(1, Msg) ->
    dht_node_join:process_join_msg(Msg, State);
%% userdevguide-end dht_node:join_message

% Move messages (see dht_node_move.erl)
on(Msg, State) when move =:= element(1, Msg) ->
    dht_node_move:process_move_msg(Msg, State);

% RM messages (see rm_loop.erl)
on(Msg, State) when element(1, Msg) =:= rm ->
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:on(Msg, RMState),
    dht_node_state:set_rm(State, RMState1);
on(Msg, State) when element(1, Msg) =:= rm_trigger ->
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
    MyGroup = pid_groups:my_groupname(),
    Pid = pid_groups:get_my(Process),
    SupTx = pid_groups:get_my(sup_dht_node_core_tx),
    NewPid =
        if Pid =:= failed andalso SupTx =:= failed -> failed;
           Pid =:= failed andalso
           Process =/= tx_rtm0 andalso
           Process =/= tx_rtm1 andalso
           Process =/= tx_rtm2 andalso
           Process =/= tx_rtm3 ->
               %% start, if necessary
               RTM_desc = util:sup_worker_desc(
                            Process, tx_tm_rtm, start_link,
                            [MyGroup, Process]),
               case supervisor:start_child(SupTx, RTM_desc) of
                   {ok, TmpPid} -> TmpPid;
                   {ok, TmpPid, _} -> TmpPid;
                   {error, {already_started, TmpPid}} -> TmpPid;
                   {error, Reason} ->
                       log:log(warn, "[ ~.0p ] tx_tm_rtm start_child failed: ~.0p~n",
                               [comm:this(), Reason]),
                       msg_delay:send_local(1, self(), {get_rtm, Source_PID, Key, Process}),
                       failed
               end;
           true -> Pid
        end,
    case NewPid of
        failed ->
            %% we are in the startup phase, processes will come up in a moment
            if Process =:= tx_rtm0 orelse
               Process =:= tx_rtm1 orelse
               Process =:= tx_rtm2 orelse
               Process =:= tx_rtm3 ->
                    comm:send_local(self(),
                                    {get_rtm, Source_PID, Key, Process});
               true -> ok
            end,
            State;
        _ ->
            GPid = comm:make_global(NewPid),
            GPidAcc = comm:make_global(tx_tm_rtm:get_my(Process, acceptor)),
            comm:send(Source_PID, {get_rtm_reply, Key, GPid, GPidAcc}),
            State
    end;

%% messages handled as a transaction participant (TP)
on({?init_TP, Params}, State) ->
    tx_tp:on_init_TP(Params, State);
on({?tp_do_commit_abort, Id, Result}, State) ->
    tx_tp:on_do_commit_abort(Id, Result, State);
on({?tp_do_commit_abort_fwd, TM, TMItemId, RTLogEntry, Result, OwnProposal}, State) ->
    tx_tp:on_do_commit_abort_fwd(TM, TMItemId, RTLogEntry, Result, OwnProposal, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Lookup (see api_dht_raw.erl and dht_node_look up.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({?lookup_aux, Key, Hops, Msg}, State) ->
    dht_node_lookup:lookup_aux(State, Key, Hops, Msg),
    State;

on({?lookup_fin, Key, Hops, Msg}, State) ->
    dht_node_lookup:lookup_fin(State, Key, Hops, Msg);

on({send_error, Target, {?lookup_aux, _, _, _} = Message, _Reason}, State) ->
    dht_node_lookup:lookup_aux_failed(State, Target, Message);
%on({send_failed, {send_error, Target, {?lookup_aux, _, _, _}} = Message, _Reason}, _Pids}}, State) ->
%    dht_node_lookup:lookup_aux_failed(State, Target, Message);
on({send_error, Target, {?lookup_fin, _, _, _} = Message, _Reason}, State) ->
    dht_node_lookup:lookup_fin_failed(State, Target, Message);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Database
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({?get_key, Source_PID, HashedKey}, State) ->
    comm:send(Source_PID,
              {get_key_response, HashedKey,
               ?DB:read(dht_node_state:get(State, db), HashedKey)}),
    State;

on({?get_key, Source_PID, SourceId, HashedKey}, State) ->
    Msg = {?get_key_with_id_reply, SourceId, HashedKey,
           ?DB:read(dht_node_state:get(State, db), HashedKey)},
    comm:send(Source_PID, Msg),
    State;

on({get_entries, Source_PID, Interval}, State) ->
    Entries = ?DB:get_entries(dht_node_state:get(State, db), Interval),
    comm:send_local(Source_PID, {get_entries_response, Entries}),
    State;

on({get_entries, Source_PID, FilterFun, ValFun}, State) ->
    Entries = ?DB:get_entries(dht_node_state:get(State, db), FilterFun, ValFun),
    comm:send_local(Source_PID, {get_entries_response, Entries}),
    State;

on({get_chunk, Source_PID, Interval, MaxChunkSize}, State) ->
    Chunk = ?DB:get_chunk(dht_node_state:get(State, db), Interval, MaxChunkSize),
    comm:send_local(Source_PID, {get_chunk_response, Chunk}),
    State;

on({get_chunk, Source_PID, Interval, FilterFun, ValueFun, MaxChunkSize}, State) ->
    Chunk = ?DB:get_chunk(dht_node_state:get(State, db), Interval, FilterFun, ValueFun, MaxChunkSize),
    comm:send_local(Source_PID, {get_chunk_response, Chunk}),
    State;

% send caller update_key_entry_ack with Entry (if exists) or Key, Exists (Yes/No), Updated (Yes/No)
on({update_key_entry, Source_PID, Key, NewValue, NewVersion}, State) ->
    {Exists, Entry} = ?DB:get_entry2(dht_node_state:get(State, db), Key),
    EntryVersion = db_entry:get_version(Entry),
    DoUpdate = Exists
        andalso EntryVersion =/= -1
        andalso EntryVersion < NewVersion
        andalso not db_entry:get_writelock(Entry)
        andalso dht_node_state:is_responsible(Key, State),
    DoRegen = not Exists
        andalso dht_node_state:is_responsible(Key, State),
    {NewState, NewEntry} =
        if
            DoUpdate ->
                UpdEntry = db_entry:set_version(db_entry:set_value(Entry, NewValue), NewVersion),
                NewDB = ?DB:update_entry(dht_node_state:get(State, db), UpdEntry),
                {dht_node_state:set_db(State, NewDB), UpdEntry};
            DoRegen ->
                RegenEntry = db_entry:new(Key, NewValue, NewVersion),
                NewDB = ?DB:set_entry(dht_node_state:get(State, db), RegenEntry),
                {dht_node_state:set_db(State, NewDB), RegenEntry};
            true -> {State, Entry}
        end,
    comm:send(Source_PID, {update_key_entry_ack, NewEntry, Exists, DoUpdate orelse DoRegen}),
    NewState;

on({db_set_subscription, SubscrTuple}, State) ->
    DB2 = ?DB:set_subscription(dht_node_state:get(State, db), SubscrTuple),
    dht_node_state:set_db(State, DB2);

on({db_get_subscription, Tag, SourcePid}, State) ->
    Subscr = ?DB:get_subscription(dht_node_state:get(State, db), Tag),
    comm:send_local(SourcePid, {db_get_subscription_response, Tag, Subscr}),
    State;

on({db_remove_subscription, Tag}, State) ->
    DB2 = ?DB:remove_subscription(dht_node_state:get(State, db), Tag),
    dht_node_state:set_db(State, DB2);

%% for unit testing only: allow direct DB manipulation
on({get_key_entry, Source_PID, HashedKey}, State) ->
    Entry = ?DB:get_entry(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {get_key_entry_reply, Entry}),
    State;

on({set_key_entry, Source_PID, Entry}, State) ->
    NewDB = ?DB:set_entry(dht_node_state:get(State, db), Entry),
    comm:send(Source_PID, {set_key_entry_reply, Entry}),
    dht_node_state:set_db(State, NewDB);

on({add_data, Source_PID, Data}, State) ->
    NewDB = ?DB:add_data(dht_node_state:get(State, db), Data),
    comm:send(Source_PID, {add_data_reply}),
    dht_node_state:set_db(State, NewDB);

on({delete_key, Source_PID, ClientsId, HashedKey}, State) ->
    {DB2, Result} = ?DB:delete(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {delete_key_response, ClientsId, HashedKey, Result}),
    dht_node_state:set_db(State, DB2);

on({drop_data, Data, Sender}, State) ->
    comm:send(Sender, {drop_data_ack}),
    DB = ?DB:add_data(dht_node_state:get(State, db), Data),
    dht_node_state:set_db(State, DB);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Bulk owner messages (see bulkowner.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on(Msg, State) when bulkowner =:= element(1, Msg)->
    bulkowner:on(Msg, State);

on({send_error, _FailedTarget, FailedMsg, _Reason} = Msg, State)
  when bulkowner =:= element(1, FailedMsg) ->
    bulkowner:on(Msg, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% handling of failed sends
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_yaws_info, Pid}, State) ->
    comm:send(Pid, {get_yaws_info_response, comm:get_ip(comm:this()), config:read(yaws_port), pid_groups:my_groupname()}),
    State;
on({get_state, Pid, Which}, State) ->
    comm:send(Pid, {get_state_response, dht_node_state:get(State, Which)}),
    State;
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
                  || DBEntry <- element(2, ?DB:get_chunk(dht_node_state:get(State, db), intervals:all(), 50))],
    DataList = case Load > 50 of
                   true  -> lists:append(DataListTmp, [{"...", ""}]);
                   false -> DataListTmp
               end,
    KVList1 =
        [{"rt_algorithm", webhelpers:safe_html_string("~p", [?RT])},
         {"rt_size", ?RT:get_size(dht_node_state:get(State, rt))},
         {"my_range", webhelpers:safe_html_string("~p", [intervals:get_bounds(dht_node_state:get(State, my_range))])},
         {"db_range", webhelpers:safe_html_string("~p", [dht_node_state:get(State, db_range)])},
         {"load", webhelpers:safe_html_string("~p", [Load])},
         {"join_time", webhelpers:safe_html_string("~p UTC", [calendar:now_to_universal_time(dht_node_state:get(State, join_time))])},
%%          {"db", webhelpers:safe_html_string("~p", [dht_node_state:get(State, db)])},
%%          {"proposer", webhelpers:safe_html_string("~p", [dht_node_state:get(State, proposer)])},
         {"tx_tp_db", webhelpers:safe_html_string("~p", [dht_node_state:get(State, tx_tp_db)])},
         {"slide_pred", webhelpers:safe_html_string("~p", [dht_node_state:get(State, slide_pred)])},
         {"slide_succ", webhelpers:safe_html_string("~p", [dht_node_state:get(State, slide_succ)])},
         {"msg_fwd", webhelpers:safe_html_string("~p", [dht_node_state:get(State, msg_fwd)])}
        ],
    KVList2 = lists:append(KVList1, [{"", ""} | rm_loop:get_web_debug_info(RMState)]),
    KVList3 = lists:append(KVList2, [{"", ""} , {"data (db_entry):", ""} | DataList]),
    comm:send_local(Requestor, {web_debug_info_reply, KVList3}),
    State;

on({unittest_get_bounds_and_data, SourcePid}, State) ->
    MyRange = dht_node_state:get(State, my_range),
    MyBounds = intervals:get_bounds(MyRange),
    Data = ?DB:get_data(dht_node_state:get(State, db)),
    Pred = dht_node_state:get(State, pred),
    Succ = dht_node_state:get(State, succ),
    comm:send(SourcePid, {unittest_get_bounds_and_data_response, MyBounds, Data, Pred, Succ}),
    State;

on({get_dht_nodes_response, _KnownHosts}, State) ->
    % will ignore these messages after join
    State;

% failure detector, dead node cache
on({crash, DeadPid, Cookie}, State) when is_tuple(Cookie) andalso element(1, Cookie) =:= move->
    dht_node_move:crashed_node(State, DeadPid, Cookie);
on({crash, DeadPid}, State) ->
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:crashed_node(RMState, DeadPid),
    % TODO: integrate crash handler for join
    dht_node_state:set_rm(State, RMState1);

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, State) ->
    RMState = dht_node_state:get(State, rm_state),
    RMState1 = rm_loop:zombie_node(RMState, Node),
    % TODO: call other modules, e.g. join, move
    dht_node_state:set_rm(State, RMState1).


%% userdevguide-begin dht_node:start
%% @doc joins this node in the ring and calls the main loop
-spec init(Options::[tuple()])
        -> dht_node_state:state() |
           {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::dht_node_join:join_state()}.
init(Options) ->
    {my_sup_dht_node_id, MySupDhtNode} = lists:keyfind(my_sup_dht_node_id, 1, Options),
    erlang:put(my_sup_dht_node_id, MySupDhtNode),
    % get my ID (if set, otherwise chose a random ID):
    Id = case lists:keyfind({dht_node, id}, 1, Options) of
             {{dht_node, id}, IdX} -> IdX;
             _ -> ?RT:get_random_node_id()
         end,
    case is_first(Options) of
        true -> dht_node_join:join_as_first(Id, 0, Options);
        _    -> dht_node_join:join_as_other(Id, 0, Options)
    end.
%% userdevguide-end dht_node:start

%% userdevguide-begin dht_node:start_link
%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node}, wait_for_init]).
%% userdevguide-end dht_node:start_link

%% @doc Checks whether this VM is marked as first, e.g. in a unit test, and
%%      this is the first node in this VM.
-spec is_first([tuple()]) -> boolean().
is_first(Options) ->
    lists:member({first}, Options) andalso admin_first:is_first_vm().

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
