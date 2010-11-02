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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    dht_node main file
%% @end
%% @version $Id$
-module(dht_node).
-author('schuett@zib.de').
-vsn('$Id$').

-include("transstore/trecords.hrl").
-include("scalaris.hrl").

-behaviour(gen_component).

-export([start_link/2, on/2, init/1,
         trigger_known_nodes/0]).

-export([is_first/1, is_alive/1]).

-ifdef(with_export_type_support).
-export_type([message/0]).
-endif.

% state of the dht_node loop
-type(state() :: dht_node_join:join_state() | dht_node_state:state() | kill).

-type(bulkowner_message() ::
      {bulk_owner, I::intervals:interval(), Msg::comm:message()} |
      {start_bulk_owner, I::intervals:interval(), Msg::comm:message()} |
      {bulkowner_deliver, Range::intervals:interval(), {bulk_read_entry, Issuer::comm:mypid()}}).

-type(database_message() ::
      {get_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {get_key, Source_PID::comm:mypid(), SourceId::any(), HashedKey::?RT:key()} |
      {delete_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {delete_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {drop_data, Data::list(db_entry:entry()), Sender::comm:mypid()}).

-type(lookup_message() ::
      {lookup_aux, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()} |
      {lookup_fin, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()}).

-type(rt_message() ::
      {rt_update, RoutingTable::?RT:external_rt()}).

% accepted messages of dht_node processes
-type(message() ::
      bulkowner_message() |
      database_message() |
      lookup_message() |
      dht_node_join:join_message() |
      rt_message() |
      dht_node_move:move_message()).

%% @doc message handler
-spec on(message(), state()) -> state().

%% Join messages (see dht_node_join.erl)
%% userdevguide-begin dht_node:join_message
on(Msg, State) when element(1, State) =:= join ->
    dht_node_join:process_join_state(Msg, State);
on(Msg, State) when element(1, Msg) =:= join ->
    dht_node_join:process_join_msg(Msg, State);
%% userdevguide-end dht_node:join_message

% Move messages (see dht_node_move.erl)
on(Msg, State) when element(1, Msg) =:= move orelse
     (is_tuple(Msg) andalso erlang:element(1, Msg) =:= move)->
    dht_node_move:process_move_msg(Msg, State);

%% Kill Messages
on({kill}, _State) ->
    kill;

on({leave}, State) ->
    dht_node_move:make_slide_leave(State);

on({churn}, _State) ->
    idholder:reinit(),
    kill;

on({halt}, _State) ->
    util:sleep_for_ever();

on({die}, _State) ->
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rt_update, RoutingTable}, State) ->
    dht_node_state:set_rt(State, RoutingTable);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Transactions (see transstore/*.erl, transactions/*.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({read, SourcePID, Key}, State) ->
    transaction:quorum_read(Key, SourcePID),
    State;

on({delete, SourcePID, Key}, State) ->
    transaction:delete(SourcePID, Key),
    State;

on({parallel_reads, SourcePID, Keys, TLog}, State) ->
    transaction:parallel_quorum_reads(Keys, TLog, SourcePID),
    State;

%%  initiate a read phase
on({do_transaction, TransFun, SuccessFun, FailureFun, Owner}, State) ->
    transaction:do_transaction(State, TransFun, SuccessFun, FailureFun, Owner),
    State;

%% do a transaction without a read phase
on({do_transaction_wo_rp, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner}, State) ->
    transaction:do_transaction_wo_readphase(State, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner),
    State;

%% answer - lookup for transaction participant
on({lookup_tp, Message}, State) ->
    {Leader} = Message#tp_message.message,
    comm:send(Leader, {tp, Message#tp_message.item_key, Message#tp_message.orig_key, comm:this()}),
    State;

%% answer - lookup for replicated transaction manager
on({init_rtm, Message}, State) ->
    transaction:initRTM(State, Message);

%% a validation request for a node acting as a transaction participant
on({validate, TransID, Item}, State) ->
    tparticipant:tp_validate(State, TransID, Item);

%% this message contains the final decision for a certain transaction
on({decision, Message}, State) ->
    {_, TransID, Decision} = Message#tp_message.message,
    if
        Decision =:= commit ->
            tparticipant:tp_commit(State, TransID);
        true ->
            tparticipant:tp_abort(State, TransID)
    end;

%% remove tm->tid mapping after transaction manager stopped
on({remove_tm_tid_mapping, TransID, _TMPid}, State) ->
    {translog, TID_TM_Mapping, Decided, Undecided} = dht_node_state:get(State, trans_log),
    NewTID_TM_Mapping = dict:erase(TransID, TID_TM_Mapping),
    dht_node_state:set_trans_log(State, {translog, NewTID_TM_Mapping, Decided, Undecided});

on({get_process_in_group, Source_PID, Key, Process}, State) ->
    Pid = pid_groups:get_my(Process),
    GPid = comm:make_global(Pid),
    comm:send(Source_PID, {get_process_in_group_reply, Key, GPid}),
    State;

on({get_rtm, Source_PID, Key, Process}, State) ->
    MyGroup = pid_groups:my_groupname(),
    Pid = pid_groups:get_my(Process),
    SupTx = pid_groups:get_my(sup_dht_node_core_tx),
    NewPid = case Pid of
                 failed ->
                     %% start, if necessary
                     RTM_desc = util:sup_worker_desc(
                                  Process, tx_tm_rtm, start_link,
                                  [MyGroup, Process]),
                     case supervisor:start_child(SupTx, RTM_desc) of
                         {ok, TmpPid} -> TmpPid;
                         {ok, TmpPid, _} -> TmpPid;
                         {error, _} ->
                             msg_delay:send_local(1, self(), {get_rtm, Source_PID, Key, Process}),
                             failed
                     end;
                 _ -> Pid
             end,
    case NewPid of
        failed -> State;
        _ ->
            GPid = comm:make_global(NewPid),
            comm:send(Source_PID, {get_rtm_reply, Key, GPid}),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Lookup (see lookup.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({lookup_aux, Key, Hops, Msg}, State) ->
    dht_node_lookup:lookup_aux(State, Key, Hops, Msg),
    State;

on(CompleteMsg = {lookup_fin, Key, Hops, Msg}, State) ->
    MsgFwd = dht_node_state:get(State, msg_fwd),
    FwdList = [P || {I, P} <- MsgFwd, intervals:in(Key, I)],
    case FwdList of
        []    ->
            case dht_node_state:is_db_responsible(Key, State) of
                true -> comm:send_local(self(), Msg);
                false ->
                    DBRange = dht_node_state:get(State, db_range),
                    DBRange2 = case intervals:is_continuous(DBRange) of
                                   true -> intervals:get_bounds(DBRange);
                                   _    -> DBRange
                               end,
                    % it is possible that we received the message due to a
                    % forward while sliding and before the other node removed
                    % the forward -> do not warn then
                    SlidePred = dht_node_state:get(State, slide_pred),
                    SlideSucc = dht_node_state:get(State, slide_succ),
                    case ((SlidePred =/= null andalso
                               slide_op:get_sendORreceive(SlidePred) =:= 'send' andalso
                               intervals:in(Key, slide_op:get_interval(SlidePred)))
                         orelse
                              (SlideSucc =/= null andalso
                                   slide_op:get_sendORreceive(SlideSucc) =:= 'send' andalso
                                   intervals:in(Key, slide_op:get_interval(SlideSucc)))) of
                        true -> ok;
                        false ->
                            log:log(warn,
                                    "Routing is damaged!! Trying again...~n  myrange:~p~n  db_range:~p~n  Key:~p",
                                    [intervals:get_bounds(dht_node_state:get(State, my_range)),
                                     DBRange2, Key])
                    end,
                    dht_node_lookup:lookup_aux(State, Key, Hops, Msg)
            end;
        [Pid] -> comm:send(Pid, CompleteMsg)
    end,
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Database
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_key, Source_PID, HashedKey}, State) ->
    comm:send(Source_PID,
              {get_key_response, HashedKey,
               ?DB:read(dht_node_state:get(State, db), HashedKey)}),
    State;

on({get_key, Source_PID, SourceId, HashedKey}, State) ->
    Msg = {get_key_with_id_reply, SourceId, HashedKey,
           ?DB:read(dht_node_state:get(State, db), HashedKey)},
    comm:send(Source_PID, Msg),
    State;

%% for unit testing only: allow direct DB manipulation
on({get_key_entry, Source_PID, HashedKey}, State) ->
    Entry = ?DB:get_entry(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {get_key_entry_reply, Entry}),
    State;

on({set_key_entry, Source_PID, Entry}, State) ->
    NewDB = ?DB:set_entry(dht_node_state:get(State, db), Entry),
    comm:send(Source_PID, {set_key_entry_reply, Entry}),
    dht_node_state:set_db(State, NewDB);

on({delete_key, Source_PID, HashedKey}, State) ->
    {DB2, Result} = ?DB:delete(dht_node_state:get(State, db), HashedKey),
    comm:send(Source_PID, {delete_key_response, HashedKey, Result}),
    dht_node_state:set_db(State, DB2);

on({drop_data, Data, Sender}, State) ->
    comm:send(Sender, {drop_data_ack}),
    DB = ?DB:add_data(dht_node_state:get(State, db), Data),
    dht_node_state:set_db(State, DB);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Bulk owner messages (see bulkowner.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({bulk_owner, I, Msg}, State) ->
    bulkowner:bulk_owner(State, I, Msg),
    State;

on({start_bulk_owner, I, Msg}, State) ->
    bulkowner:bulk_owner(State, I, Msg),
    State;

on({bulkowner_deliver, Range, {bulk_read_entry, Issuer}}, State) ->
    MsgFwd = dht_node_state:get(State, msg_fwd),
    
    F = fun({FwdInt, FwdPid}, AccI) ->
                case intervals:is_subset(FwdInt, AccI) of
                    true ->
                        FwdRange = intervals:intersection(AccI, FwdInt),
                        comm:send(FwdPid, {bulkowner_deliver, FwdRange, {bulk_read_entry, Issuer}}),
                        intervals:minus(AccI, FwdRange);
                    _    -> AccI
                end
        end,
    MyRange = lists:foldl(F, Range, MsgFwd),
    Data = ?DB:get_entries(dht_node_state:get(State, db), MyRange),
    comm:send(Issuer, {bulk_read_entry_response, MyRange, Data}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_node_details, Pid}, State) ->
    comm:send(Pid, {get_node_details_response, dht_node_state:details(State)}),
    State;
on({get_node_details, Pid, Which}, State) ->
    comm:send(Pid, {get_node_details_response, dht_node_state:details(State, Which)}),
    State;

on({dump}, State) ->
    dht_node_state:dump(State),
    State;

on({web_debug_info, Requestor}, State) ->
    Load = dht_node_state:get(State, load),
    % get a list of up to 50 KV pairs to display:
    DataListTmp = [{"",
                    lists:flatten(io_lib:format("~p", [DBEntry]))}
                  || DBEntry <-
                         lists:sublist(?DB:get_data(dht_node_state:get(State, db)), 50)],
    DataList = case Load > 50 of
                   true  -> lists:append(DataListTmp, [{"...", ""}]);
                   false -> DataListTmp
               end,
    KeyValueList =
        [{"rt_algorithm", lists:flatten(io_lib:format("~p", [?RT]))},
         {"rt_size", ?RT:get_size(dht_node_state:get(State, rt))},
         {"me", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, node)]))},
         {"my_range", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, my_range)]))},
         {"db_range", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, db_range)]))},
         {"load", lists:flatten(io_lib:format("~p", [Load]))},
         {"join_time", lists:flatten(io_lib:format("~p UTC", [calendar:now_to_universal_time(dht_node_state:get(State, join_time))]))},
%%          {"db", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, db)]))},
%%          {"translog", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, trans_log)]))},
%%          {"proposer", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, proposer)]))},
         {"tx_tp_db", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, tx_tp_db)]))},
         {"slide_pred", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, slide_pred)]))},
         {"slide_succ", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, slide_succ)]))},
         {"msg_fwd", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, msg_fwd)]))},
         {"data (db_entry):", ""} |
            DataList 
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on({get_dht_nodes_response, _KnownHosts}, State) ->
    % will ignore these messages after join
    State;

%% messages handled as a transaction participant (TP)
on({init_TP, Params}, State) ->
    tx_tp:on_init_TP(Params, State);
on({tx_tm_rtm_commit_reply, Id, Result}, State) ->
    tx_tp:on_tx_commitreply(Id, Result, State);
on({tx_tm_rtm_commit_reply_fwd, RTLogEntry, Result, OwnProposal}, State) ->
    tx_tp:on_tx_commitreply_fwd(RTLogEntry, Result, OwnProposal, State);
%% messages handled as proxy for a proposer in the role of a
%% transaction participant (TP) (possible replies from an acceptor)
on({acceptor_ack, _PaxosId, _InRound, _Val, _Raccpeted} = Msg, State) ->
    tx_tp:on_forward_to_proposer(Msg, State);
on({acceptor_nack, _PaxosId, _NewerRound} = Msg, State) ->
    tx_tp:on_forward_to_proposer(Msg, State);
on({acceptor_naccepted, _PaxosId, _NewerRound} = Msg, State) ->
    tx_tp:on_forward_to_proposer(Msg, State).


%% userdevguide-begin dht_node:start
%% @doc joins this node in the ring and calls the main loop
-spec init(Options::[tuple()]) -> {join, {as_first | phase1}, msg_queue:msg_queue()}.
init(Options) ->
    {my_sup_dht_node_id, MySupDhtNode} = lists:keyfind(my_sup_dht_node_id, 1, Options),
    erlang:put(my_sup_dht_node_id, MySupDhtNode),
    % first node in this vm and also vm is marked as first
    % or unit-test
    case is_first(Options) of
        true ->
            trigger_known_nodes(),
            idholder:get_id(),
            {join, {as_first}, msg_queue:new()};
        _ ->
            idholder:get_id(),
            {join, {phase1}, msg_queue:new()}
    end.
%% userdevguide-end dht_node:start

%% userdevguide-begin dht_node:start_link
%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, Options,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node}, wait_for_init]).
%% userdevguide-end dht_node:start_link

%% @doc Find existing nodes and as a side-effect initialize the comm_layer.
-spec trigger_known_nodes() -> ok.
trigger_known_nodes() ->
    KnownHosts = config:read(known_hosts),
    % note, comm:this() may be invalid at this moment
    [comm:send(KnownHost, {get_dht_nodes, comm:this()})
     || KnownHost <- KnownHosts],
    timer:sleep(100),
    case comm:is_valid(comm:this()) of
        true ->
            ok;
        false ->
            trigger_known_nodes()
    end.

-spec is_first([tuple()]) -> boolean().
is_first(Options) ->
    lists:member({first}, Options) andalso
        (util:is_unittest() orelse preconfig:get_env(first, false) =:= true).

is_alive(Pid) ->
    element(1, gen_component:get_state(Pid)) =:= state.
