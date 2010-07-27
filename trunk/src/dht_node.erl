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

-export([start_link/1, start_link/2]).

-export([on/2, init/1]).

% state of the dht_node loop
-type(state() :: dht_node_join:join_state() | dht_node_state:state() | kill).

-type(bulkowner_message() ::
      {bulk_owner, I::intervals:interval(), Msg::comm:message()} |
      {start_bulk_owner, I::intervals:interval(), Msg::comm:message()} |
      {bulkowner_deliver, Range::intervals:interval(), {bulk_read_with_version, Issuer::comm:mypid()}}).

-type(database_message() ::
      {get_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {get_key, Source_PID::comm:mypid(), SourceId::any(), HashedKey::?RT:key()} |
      {set_key, Source_PID::comm:mypid(), Key::?RT:key(), Value::?DB:value(), Versionnr::?DB:value()} |
      {delete_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {delete_key, Source_PID::comm:mypid(), Key::?RT:key()} |
      {drop_data, Data::list(db_entry:entry()), Sender::comm:mypid()}).

-type(lookup_message() ::
      {lookup_aux, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()} |
      {lookup_fin, Hops::pos_integer(), Msg::comm:message()}).


% accepted messages of dht_node processes
-type(message() ::
      bulkowner_message() |
      database_message() |
      lookup_message() |
      dht_node_join:join_message()).

%% @doc message handler
-spec on(message(), state()) -> state().

% Join messages (see dht_node_join.erl)
on(Msg, State) when element(1, State) =:= join ->
    dht_node_join:process_join_msg(Msg, State);

on({get_node, Source_PID, Key}, State) ->
    comm:send(Source_PID, {get_node_response, Key, dht_node_state:get(State, node)}),
    State;

%% Kill Messages
on({kill}, _State) ->
    kill;

on({churn}, _State) ->
    idholder:reinit(),
    kill;

on({halt}, _State) ->
    util:sleep_for_ever();

on({die}, _State) ->
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring Maintenance (see rm_beh.erl) 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% {init_rm, pid()}
on({init_rm, Pid}, State) ->
    comm:send_local(Pid, {init_rm, dht_node_state:get(State, node),
                                   dht_node_state:get(State, pred),
                                   dht_node_state:get(State, succ)}),
    State;

%% {rm_update_neighbors, Neighbors::nodelist:neighborhood()}
on({rm_update_neighbors, Neighbors}, State) ->
    OldRT = dht_node_state:get(State, rt),
    NewRT = ?RT:update_pred_succ_in_dht_node(nodelist:pred(Neighbors),
                                             nodelist:succ(Neighbors),
                                             OldRT),
    State_NewNeighbors = dht_node_state:set_neighbors(State, Neighbors),
    dht_node_state:set_rt(State_NewNeighbors, NewRT);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rt_update, RoutingTable}, State) ->
    dht_node_state:set_rt(State, RoutingTable);

%% userdevguide-begin dht_node:rt_get_node
on({rt_get_node, Source_PID, Index}, State) ->
    comm:send(Source_PID, {rt_get_node_response, Index, dht_node_state:get(State, node)}),
    State;
%% userdevguide-end dht_node:rt_get_node

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
    MyRange = dht_node_state:get(State, my_range),
    case intervals:in(Message#tp_message.item_key, MyRange) of
        true ->
            comm:send(Leader, {tp, Message#tp_message.item_key, Message#tp_message.orig_key, comm:this()}),
            State;
        false ->
            log:log(info,"[ Node ] LookupTP: Got Request for Key ~p, it is not in ~p~n", [Message#tp_message.item_key, MyRange]),
            State
    end;

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
    Pid = process_dictionary:get_group_member(Process),
    GPid = comm:make_global(Pid),
    comm:send(Source_PID, {get_process_in_group_reply, Key, GPid}),
    State;

on({get_rtm, Source_PID, Key, Process}, State) ->
    InstanceId = erlang:get(instance_id),
    Pid = process_dictionary:get_group_member(Process),
    SupTx = process_dictionary:get_group_member(sup_dht_node_core_tx),
    NewPid = case Pid of
                 failed ->
                     %% start, if necessary
                     RTM_desc = util:sup_worker_desc(
                                  Process, tx_tm_rtm, start_link,
                                  [InstanceId, Process]),
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

on({lookup_fin, _Hops, Msg}, State) ->
    comm:send_local(self(), Msg),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Database
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_key, Source_PID, HashedKey}, State) ->
    MyRange = dht_node_state:get(State, my_range),
    case intervals:in(HashedKey, MyRange) of
        true ->
%%             log:log(info, "[ ~w | I | Node   | ~w ] get_key ~s~n",
%%                     [calendar:universal_time(), self(), HashedKey]),
            comm:send(Source_PID,
                      {get_key_response, HashedKey,
                       ?DB:read(dht_node_state:get(State, db), HashedKey)});
        false ->
            log:log(info, "[ Node ] get_Key: Got Request for Key ~p, it is not in ~p~n",
                    [HashedKey, MyRange])
    end,
    State;

on({get_key, Source_PID, SourceId, HashedKey}, State) ->
%      case 0 =:= randoms:rand_uniform(0,6) of
%          true ->
%              io:format("drop get_key request~n");
%          false ->
    comm:send(Source_PID,
              {get_key_with_id_reply, SourceId, HashedKey,
               ?DB:read(dht_node_state:get(State, db), HashedKey)}),
%    end,
    State;

on({delete_key, Source_PID, Key}, State) ->
    MyRange = dht_node_state:get(State, my_range),
    case intervals:in(Key, MyRange) of
        true ->
            {DB2, Result} = ?DB:delete(dht_node_state:get(State, db), Key),
            comm:send(Source_PID, {delete_key_response, Key, Result}),
            dht_node_state:set_db(State, DB2);
        false ->
            log:log(info,"[ Node ] delete_Key: Got Request for Key ~p, it is not in ~p~n", [Key, MyRange]),
            State
    end;

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

on({bulkowner_deliver, Range, {bulk_read_with_version, Issuer}}, State) ->
    comm:send(Issuer, {bulk_read_with_version_response, dht_node_state:get(State, my_range),
                       ?DB:get_range_with_version(dht_node_state:get(State, db), Range)}),
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

on({'$gen_cast', {debug_info, Requestor}}, State) ->
    Load = dht_node_state:get(State, load),
    % get a list of up to 50 KV pairs to display:
    DataListTmp = [{lists:flatten(io_lib:format("~p", [Key])),
                    lists:flatten(io_lib:format("~p", [Value]))}
                  || {Key, Value} <-
                         lists:sublist(?DB:get_data(dht_node_state:get(State, db)), 50)],
    DataList = case Load > 50 of
                   true  -> lists:append(DataListTmp, [{"...", ""}]);
                   false -> DataListTmp
               end,
    KeyValueList =
        [{"rt_size", ?RT:get_size(dht_node_state:get(State, rt))},
         {"succs", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, succlist)]))},
         {"preds", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, predlist)]))},
         {"me", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, node)]))},
         {"my_range", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, my_range)]))},
         {"load", lists:flatten(io_lib:format("~p", [Load]))},
%%          {"lb", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, lb)]))},
%%          {"deadnodes", lists:flatten(io_lib:format("~p", [dht_node_state:???(State)]))},
         {"join_time", lists:flatten(io_lib:format("~p UTC", [calendar:now_to_universal_time(dht_node_state:get(State, join_time))]))},
%%          {"db", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, db)]))},
%%          {"translog", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, trans_log)]))},
%%          {"proposer", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, proposer)]))},
         {"tx_tp_db", lists:flatten(io_lib:format("~p", [dht_node_state:get(State, tx_tp_db)]))},
         {"data (hashed_key, {value, writelock, readlocks, version}):", ""} |
            DataList 
        ],
    comm:send_local(Requestor, {debug_info_response, KeyValueList}),
    State;


%% unit_tests
on({bulkowner_deliver, Range, {unit_test_bulkowner, Owner}}, State) ->
    Res = ?DB:get_range(dht_node_state:get(State, db), Range),
    comm:send_local(Owner , {unit_test_bulkowner_response, Res, dht_node_state:get(State, node_id)}),
    State;

%% @TODO buggy ...
%on({get_node_response, _, _}, State) ->
%    State;

%% join messages (see dht_node_join.erl)
%% userdevguide-begin dht_node:join_message
on({join, NewPred}, State) ->
    dht_node_join:join_request(State, NewPred);
%% userdevguide-end dht_node:join_message

on({known_hosts_timeout}, State) ->
    % will ignore these messages after join
    State;

on({get_dht_nodes_response, _KnownHosts}, State) ->
    % will ignore these messages after join
    State;

on({lookup_timeout}, State) ->
    % will ignore these messages after join
    State;

%% messages handled as a transaction participant (TP)
on({init_TP, Params}, State) ->
    tx_tp:on_init_TP(Params, State);
on({tx_tm_rtm_commit_reply, Id, Result}, State) ->
    tx_tp:on_tx_commitreply(Id, Result, State).

%% userdevguide-begin dht_node:start
%% @doc joins this node in the ring and calls the main loop
-spec init([instanceid() | [any()]]) -> {join, {as_first}, msg_queue:msg_queue()} | {join, {phase1}, msg_queue:msg_queue()}.
init([_InstanceId, Options]) ->
    %io:format("~p~n", [Options]),
    % first node in this vm and also vm is marked as first
    % or unit-test
    case lists:member(first, Options) andalso
          (is_unittest() orelse
          application:get_env(boot_cs, first) =:= {ok, true}) of
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
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    start_link(InstanceId, []).

-spec start_link(instanceid(), [any()]) -> {ok, pid()}.
start_link(InstanceId, Options) ->
    gen_component:start_link(?MODULE, [InstanceId, Options],
                             [{register, InstanceId, dht_node}, wait_for_init]).
%% userdevguide-end dht_node:start_link

% @doc find existing nodes and initialize the comm_layer
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

% @doc try to check whether common-test is running
-spec is_unittest() -> boolean().
is_unittest() ->
    code:is_loaded(ct) =/= false andalso code:is_loaded(ct_framework) =/= false andalso
    lists:member(ct_logs, registered()).
