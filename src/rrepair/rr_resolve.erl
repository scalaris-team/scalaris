% @copyright 2011-2014 Zuse Institute Berlin

%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable request_resolvelaw or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    replica update resolve module
%%         Updates local and/or remote Key-Value-Pairs (kv-pair)
%%         Modes:
%%           1) key_upd: updates local db entries with received kvv-list, if received kv is newer
%%           2) key_upd_send: creates kvv-list out of a given key-list and sends it to dest
%%           3) interval_upd: works like key_upd +
%%                            sends all own db entries which are not in received kvv-list
%%                            to the feedback pid (if given)
%%           4) interval_upd_send: creates kvv-list from given interval and sends it to dest
%%           5) interval_upd_my: tries to resolve items from the given interval by
%%                               requesting data from replica nodes
%%         Options:
%%           1) Feedback: sends data ids to Node (A) which are outdated at (A)
%%           2) Send_Stats: sends resolution stats to given pid
%%         Usage:
%%           rrepair process provides API for resolve requests
%% @end
%% @version $Id$
-module(rr_resolve).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/0]).
-export([get_stats_session_id/1, get_stats_resolve_started/1, merge_stats/2]).
-export([print_resolve_stats/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([operation/0, options/0, kvv_list/0]).
-export_type([stats/0]).
-endif.

-type option()   :: {feedback_request, comm:mypid()} |
                    {send_stats, comm:mypid()} | %send stats to pid after completion
                    {session_id, rrepair:session_id()} |
                    {from_my_node, 0 | 1}.
-type options()  :: [option()].
-type kvv_list() :: [{?RT:key(), db_dht:value(), db_dht:version()}].
-type exit_reason() :: resolve_ok | resolve_abort.

-record(resolve_stats,
        {
         session_id       = null   :: rrepair:session_id() | null,
         diff_size        = 0      :: non_neg_integer(),
         regen_count      = 0      :: non_neg_integer(),
         update_count     = 0      :: non_neg_integer(),
         upd_fail_count   = 0      :: non_neg_integer(),
         regen_fail_count = 0      :: non_neg_integer(),
         resolve_started  = 0      :: non_neg_integer()
         }).
-type stats() :: #resolve_stats{}.

-type operation() ::
    {?key_upd, KvvListInAnyQ::kvv_list(), ReqKeys::[?RT:key()]} |
    {key_upd_send, DestPid::comm:mypid(), SendKeys::[?RT:key()], ReqKeys::[?RT:key()]} |
    {?interval_upd, intervals:interval(), KvvListInAnyQ::kvv_list()} |
    {interval_upd_send, intervals:interval(), DestPid::comm:mypid()} |
    {interval_upd_my, intervals:interval()}.

-record(rr_resolve_state,
        {
         ownerPid       = ?required(rr_resolve_state, ownerPid)   :: comm:erl_local_pid(),
         dhtNodePid     = ?required(rr_resolve_state, dhtNodePid) :: comm:erl_local_pid(),
         operation      = undefined                               :: undefined | operation(),
         my_range       = undefined                               :: undefined | intervals:interval(),
         fb_dest_pid    = undefined                               :: undefined | comm:mypid(),
         fb_send_kvv    = []                                      :: OutdatedOnOther::kvv_list(),
         fb_had_kvv_req = false                                   :: NonEmptyReqList::boolean(),
         fb_send_kvv_req= []                                      :: RequestedByOther::kvv_list(),
         other_kv_tree  = gb_trees:empty()                        :: MyIOtherKvTree::gb_trees:tree(?RT:key(), db_dht:version()),
         send_stats     = undefined                               :: undefined | comm:mypid(),
         stats          = #resolve_stats{}                        :: stats(),
         from_my_node   = 1                                       :: 0 | 1
         }).
-type state() :: #rr_resolve_state{}.

-type message() ::
    % API
    {start, operation(), options()} |
    % internal
    {get_entries_response, db_dht:db_as_list()} |
    {get_chunk_response, {intervals:interval(), rr_recon:db_chunk_kvv()}} |
    {get_state_response, intervals:interval()} |
    {update_key_entries_ack, [{db_entry:entry_ex(), Exists::boolean(), Done::boolean()}]} |
    {'DOWN', MonitorRef::reference(), process, Owner::comm:erl_local_pid(), Info::any()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w [~s:~p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~s:~.0p: ~.0p~n", [pid_groups:group_of(comm:make_local(element(1, comm:unpack_cookie(Pid, {no_msg})))), Pid, Msg])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({start, Operation, Options}, State) ->
    FBDest = proplists:get_value(feedback_request, Options, undefined),
    FromMyNode = proplists:get_value(from_my_node, Options, 1),
    StatsDest = proplists:get_value(send_stats, Options, undefined),
    SID = proplists:get_value(session_id, Options, null),
    NewState = State#rr_resolve_state{ operation = Operation,
                                       stats = #resolve_stats{session_id = SID},
                                       fb_dest_pid = FBDest,
                                       send_stats = StatsDest,
                                       from_my_node = FromMyNode },
    ?TRACE("RESOLVE START - Operation=~p~n FeedbackTo=~p~n SessionId:~p",
           [util:extint2atom(element(1, Operation)), FBDest, SID]),
    send_local(State#rr_resolve_state.dhtNodePid, {get_state, comm:this(), my_range}),
    NewState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: key_upd
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {?key_upd, KvvList, ReqKeys},
                          dhtNodePid = DhtPid, stats = _Stats }) ->
    MyIOtherKvvList = map_kvv_list(KvvList, MyI),
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p~n KVVListLen=~p",
           [key_upd, _Stats#resolve_stats.session_id, MyI, length(KvvList)]),

    % send requested entries (similar to key_upd_send handling)
    RepKeyInt = intervals:from_elements(map_key_list(ReqKeys, MyI)),
    send_local(DhtPid, {get_entries, self(), RepKeyInt}),

    % send entries in sender interval but not in sent KvvList
    % convert keys KvvList to a gb_tree for faster access checks
    MyIOtherKvTree =
        lists:foldl(fun({KeyX, _ValX, VersionX}, TreeX) ->
                            % assume, KVs at the same node have the same version
                            gb_trees:enter(KeyX, VersionX, TreeX)
                    end, gb_trees:empty(), MyIOtherKvvList),

    % allow the garbage collection to clean up the ReqKeys here:
    % also update the KvvList
    State#rr_resolve_state{operation = {?key_upd, MyIOtherKvvList, []},
                           other_kv_tree = MyIOtherKvTree,
                           fb_had_kvv_req = ReqKeys =/= []};

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {?key_upd, MyIOtherKvvList, []},
                          dhtNodePid = DhtPid, fb_send_kvv_req = FbReqKVV,
                          stats = Stats}) ->
    KvvList = [entry_to_kvv(E) || E <- EntryList],
    ToUpdate = start_update_key_entries(MyIOtherKvvList, comm:this(), DhtPid),
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p ; ToUpdate=~p - #Items: ~p",
           [key_upd, Stats#resolve_stats.session_id, ToUpdate, length(EntryList)]),

    % allow the garbage collection to clean up the KvvList here:
    NewState =
        State#rr_resolve_state{operation = {?key_upd, [], []},
                               stats = Stats#resolve_stats{diff_size = ToUpdate},
                               fb_send_kvv_req = lists:append(FbReqKVV, KvvList)},

    if ToUpdate =:= 0 ->
           % use the same options as above in get_state_response:
           shutdown(resolve_ok, NewState);
       true ->
           % note: shutdown and feedback handled by update_key_entries_ack
           NewState
    end;

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {key_upd_send, _Dest, SendKeys, _ReqKeys},
                          dhtNodePid = DhtPid, stats = _Stats }) ->
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p",
           [key_upd_send, _Stats#resolve_stats.session_id, MyI]),
    SendKeysMappedInterval = intervals:from_elements(map_key_list(SendKeys, MyI)),
    send_local(DhtPid, {get_entries, self(), SendKeysMappedInterval}),
    State;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {key_upd_send, Dest, _, ReqKeys},
                          fb_dest_pid = FBDest, from_my_node = FromMyNode,
                          stats = Stats }) ->
    SID = Stats#resolve_stats.session_id,
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p",
           [key_upd_send, SID, length(EntryList)]),
    KvvList = [entry_to_kvv(E) || E <- EntryList],
    % note: if ReqKeys contains any entries, we will get 2 replies per resolve!
    FBCount = if ReqKeys =/= [] -> 2;
                 true -> 1
              end,
    ResStarted = send_request_resolve(Dest, {?key_upd, KvvList, ReqKeys}, SID,
                                      FromMyNode, FBDest, [], false) * FBCount,

    NewState =
        State#rr_resolve_state{stats = Stats#resolve_stats{resolve_started = ResStarted},
                               fb_dest_pid = undefined},
    shutdown(resolve_ok, NewState);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: interval_upd
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = Op, dhtNodePid = DhtPid, stats = _Stats })
  when element(1, Op) =:= ?interval_upd;
       element(1, Op) =:= interval_upd_send ->
    OpSIs = intervals:get_simple_intervals(element(2, Op)),
    ISec = lists:foldl(
             fun(Q, AccJ) ->
                     lists:foldl(
                       fun(OpSI, AccI) ->
                               OpI = intervals:simple_interval_to_interval(OpSI),
                               I = intervals:intersection(OpI, Q),
                               case intervals:is_empty(I) of
                                   true  -> AccI;
                                   false -> MI = rr_recon:map_interval(MyI, I),
                                            intervals:union(AccI, MI)
                               end
                       end, AccJ, OpSIs)
             end, intervals:empty(), rr_recon:quadrant_intervals()),
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n IntervalBounds=~p~n MyInterval=~p~n IntersecBounds=~p",
           [util:extint2atom(element(1, Op)), _Stats#resolve_stats.session_id,
            intervals:get_bounds(element(2, Op)),
            MyI, intervals:get_bounds(ISec)]),
    NewState = State#rr_resolve_state{ my_range = MyI },
    case intervals:is_empty(ISec) of
        false ->
            send_local(DhtPid, {get_entries, self(), ISec}),
            NewState;
        true ->
            shutdown(resolve_abort, NewState)
    end;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {?interval_upd, I, KvvList},
                          my_range = MyI,
                          dhtNodePid = DhtPid,
                          stats = Stats }) ->
    MyIOtherKvvList = map_kvv_list(KvvList, MyI),
    ToUpdate = start_update_key_entries(MyIOtherKvvList, comm:this(), DhtPid),
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p, KVVListLen=~p ; ToUpdate=~p",
           [interval_upd, Stats#resolve_stats.session_id, length(EntryList), length(KvvList), ToUpdate]),

    % Send entries in sender interval but not in sent KvvList
    % convert keys KvvList to a gb_tree for faster access checks
    MyIOtherKvTree =
        lists:foldl(fun({KeyX, _ValX, VersionX}, TreeX) ->
                            % assume, KVs at the same node have the same version
                            gb_trees:enter(KeyX, VersionX, TreeX)
                    end, gb_trees:empty(), MyIOtherKvvList),
    MissingOnOther = [entry_to_kvv(X) || X <- EntryList,
                                         not gb_trees:is_defined(db_entry:get_key(X), MyIOtherKvTree)],

    % allow the garbage collection to clean up the KvvList here:
    NewState = State#rr_resolve_state{operation = {?interval_upd, I, []},
                                      stats = Stats#resolve_stats{diff_size = ToUpdate},
                                      fb_send_kvv = MissingOnOther,
                                      other_kv_tree = MyIOtherKvTree},
    if ToUpdate =:= 0 ->
           shutdown(resolve_ok, NewState);
       true ->
           % note: shutdown and feedback handled by update_key_entries_ack
           NewState
    end;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {interval_upd_send, I, Dest},
                          fb_dest_pid = FBDest, from_my_node = FromMyNode,
                          stats = Stats }) ->
    SID = Stats#resolve_stats.session_id,
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p",
           [interval_upd_send, SID, length(EntryList)]),

    KvvList = [entry_to_kvv(E) || E <- EntryList],
    ResStarted = send_request_resolve(Dest, {?interval_upd, I, KvvList}, SID,
                                      FromMyNode, FBDest, [], false),

    NewState =
        State#rr_resolve_state{stats = Stats#resolve_stats{resolve_started = ResStarted},
                               fb_dest_pid = undefined},
    shutdown(resolve_ok, NewState);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: interval_upd_my
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI} = _Msg,
   State = #rr_resolve_state{operation = {interval_upd_my, I}}) ->
    ?TRACE("GET INTERVAL - Operation=~p~n IntervalBounds=~p~n MyInterval=~p",
           [interval_upd_my, intervals:get_bounds(I), MyI]),
    ?DBG_ASSERT(State#rr_resolve_state.fb_dest_pid =:= undefined),
    ISec = intervals:intersection(MyI, I),
    NewState = State#rr_resolve_state{ my_range = MyI },
    case intervals:is_empty(ISec) of
        false -> case rrepair:select_sync_node(ISec, true) of
                     not_found ->
                         shutdown(resolve_abort, NewState);
                     DKey ->
                         % TODO: keep trying to resolve the whole intersection
                         %       e.g. by removing each sync interval and
                         %       continuing with the rest until the whole
                         %       interval is covered (at each step check with
                         %       the range reported from the dht_node!)
                         % -> the current implementation only tries once!
                         % note: bloom and art may not fully re-generate the
                         %       own range -> choose merkle_tree instead
                         send_local(pid_groups:get_my(rrepair),
                                    {request_sync, merkle_tree, DKey}),
                         shutdown(resolve_ok, NewState)
                 end;
        true  -> shutdown(resolve_abort, NewState)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({update_key_entries_ack, NewEntryList}, State =
       #rr_resolve_state{ operation = Op,
                          stats = #resolve_stats{ diff_size = _Diff,
                                                  regen_count = RegenOk,
                                                  update_count = UpdOk,
                                                  upd_fail_count = UpdFail,
                                                  regen_fail_count = RegenFail
                                                } = Stats,
                          fb_dest_pid = FBDest, fb_send_kvv = MissingOnOther,
                          other_kv_tree = MyIOtherKvTree
                        })
  when element(1, Op) =:= ?key_upd;
       element(1, Op) =:= ?interval_upd ->
    ?TRACE("GET ENTRY_ACK - Operation=~p~n SessionId:~p - #NewItems: ~p",
           [util:extint2atom(element(1, Op)), Stats#resolve_stats.session_id,
            length(NewEntryList)]),

    {NewUpdOk, NewUpdFail, NewRegenOk, NewRegenFail, NewFBItems} =
        integrate_update_key_entries_ack(
          NewEntryList, UpdOk, UpdFail, RegenOk, RegenFail, MissingOnOther, MyIOtherKvTree,
          FBDest =/= undefined),

    NewStats = Stats#resolve_stats{update_count     = NewUpdOk + 1,
                                   regen_count      = NewRegenOk +1,
                                   upd_fail_count   = NewUpdFail + 1,
                                   regen_fail_count = NewRegenFail + 1},
    NewState = State#rr_resolve_state{stats = NewStats, fb_send_kvv = NewFBItems},
    ?DBG_ASSERT(_Diff =:= (NewRegenOk + NewUpdOk + NewUpdFail + NewRegenFail)),
    ?TRACE("UPDATED = ~p - Regen=~p", [NewUpdOk, NewRegenOk]),
    shutdown(resolve_ok, NewState);

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Starts updating the local entries with the given KvvList.
%%      -> Returns number of send update requests.
%%      PreCond: KvvList contains only unique keys
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Maps the given tuple list (with keys as first elements) to the MyI
%%      interval.
%%      Precond: tuple list with unique keys
-spec map_kvv_list([Tpl], MyI::intervals:interval()) -> [Tpl]
        when is_subtype(Tpl, {?RT:key()} |
                             {?RT:key(), term()} |
                             {?RT:key(), term(), term()}).
map_kvv_list(TplList, MyI) ->
    ?DBG_ASSERT(length(TplList) =:= length(lists:ukeysort(1, TplList))),
    [setelement(1, E, RKey) || E <- TplList,
                               RKey <- ?RT:get_replica_keys(element(1, E)),
                               intervals:in(RKey, MyI)].

%% @doc Maps the given (unique!) key list to the MyI interval.
-spec map_key_list([?RT:key()], MyI::intervals:interval()) -> [?RT:key()].
map_key_list(KeyList, MyI) ->
    ?DBG_ASSERT(length(KeyList) =:= length(lists:usort(KeyList))),
    [RKey || Key <- KeyList,
             RKey <- ?RT:get_replica_keys(Key),
             intervals:in(RKey, MyI)].

-spec start_update_key_entries(MyIOtherKvvList::kvv_list(), comm:mypid(),
                               comm:erl_local_pid()) -> non_neg_integer().
start_update_key_entries([], _MyPid, _DhtPid) -> 0;
start_update_key_entries(MyIOtherKvvList, MyPid, DhtPid) ->
    send_local(DhtPid, {update_key_entries, MyPid, MyIOtherKvvList}),
    length(MyIOtherKvvList).

-spec integrate_update_key_entries_ack(
        [{Entry::db_entry:entry_ex(), Exists::boolean(), Done::boolean()}],
        UpdOk::non_neg_integer(), UpdFail::non_neg_integer(),
        RegenOk::non_neg_integer(), RegenFail::non_neg_integer(),
        FBItems::kvv_list(), OtherKvTree::gb_trees:tree(?RT:key(), db_dht:version()), FBOn::boolean())
        -> {UpdOk::non_neg_integer(), UpdFail::non_neg_integer(),
            RegenOk::non_neg_integer(), RegenFail::non_neg_integer(),
            FBItems::kvv_list()}.
integrate_update_key_entries_ack([], UpdOk, UpdFail, RegenOk, RegenFail, FBItems,
                               _OtherKvTree, _FBOn) ->
    {UpdOk, UpdFail, RegenOk, RegenFail, FBItems};
integrate_update_key_entries_ack([{Entry, Exists, Done} | Rest], UpdOk, UpdFail,
                               RegenOk, RegenFail, FBItems, OtherKvTree, FBOn) ->
    NewFBItems =
        if not Done andalso Exists andalso FBOn ->
               case gb_trees:lookup(db_entry:get_key(Entry), OtherKvTree) of
                   none -> [entry_to_kvv(Entry) | FBItems];
                   {value, OtherVersion} ->
                       MyVersion = db_entry:get_version(Entry),
                       if MyVersion > OtherVersion ->
                              [entry_to_kvv(Entry) | FBItems];
                          true ->
                              FBItems
                       end
               end;
           true -> FBItems
        end,
    if Done andalso Exists ->
           integrate_update_key_entries_ack(
             Rest, UpdOk + 1, UpdFail, RegenOk, RegenFail, NewFBItems, OtherKvTree, FBOn);
       Done andalso not Exists ->
           integrate_update_key_entries_ack(
             Rest, UpdOk, UpdFail, RegenOk + 1, RegenFail, NewFBItems, OtherKvTree, FBOn);
       not Done and Exists ->
           integrate_update_key_entries_ack(
             Rest, UpdOk, UpdFail + 1, RegenOk, RegenFail, NewFBItems, OtherKvTree, FBOn);
       not Done and not Exists ->
           integrate_update_key_entries_ack(
             Rest, UpdOk, UpdFail, RegenOk, RegenFail + 1, NewFBItems, OtherKvTree, FBOn)
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(_Reason, #rr_resolve_state{ownerPid = Owner, send_stats = SendStats,
                                    stats = #resolve_stats{resolve_started = ResStarted0} = Stats,
                                    operation = _Op, fb_dest_pid = FBDest,
                                    fb_send_kvv = FbKVV,
                                    fb_had_kvv_req = SendReqKeyReply,
                                    fb_send_kvv_req = FbReqKVV,
                                    from_my_node = FromMyNode} = _State) ->
    ?TRACE("SHUTDOWN ~p - Operation=~p~n SessionId:~p~n ~p items via key_upd to ~p~n Items: ~.2p",
           [_Reason, util:extint2atom(element(1, _Op)), Stats#resolve_stats.session_id,
            length(FbKVV), FBDest, FbKVV]),
    ResStarted =
        case FBDest of
            undefined ->
                0;
            _ when not SendReqKeyReply ->
                send_request_resolve(FBDest, {?key_upd, FbKVV, []},
                                     Stats#resolve_stats.session_id,
                                     FromMyNode, undefined, [], true);
            _ ->
                send_request_resolve(FBDest, {?key_upd, FbKVV, []},
                                     Stats#resolve_stats.session_id,
                                     FromMyNode, undefined, [], true) +
                    send_request_resolve(FBDest, {?key_upd, FbReqKVV, []},
                                         Stats#resolve_stats.session_id,
                                         FromMyNode, comm:make_global(Owner),
                                         [], true)
        end,
    Stats1 = Stats#resolve_stats{resolve_started = ResStarted0 + ResStarted},
    send_stats(SendStats, Stats1),
    % note: do not propagate the SessionId unless we report to the node
    %       the request came from (indicated by FromMyNode =:= 1),
    %       otherwise the resolve_progress_report on the other node will
    %       be counted for the session's rs_finish and it will not match
    %       its rs_called any more!
    if FromMyNode =:= 1 ->
           send_local(Owner, {resolve_progress_report, self(), Stats1});
       true ->
           send_local(Owner, {resolve_progress_report, self(),
                              Stats1#resolve_stats{session_id = null}})
    end,
    kill.

-spec send_request_resolve(Dest::comm:mypid(), Op::operation(),
                           SID::rrepair:session_id() | null,
                           FromMyNode::0 | 1, FBDest::comm:mypid() | undefined,
                           Options::options(), IsFeedback::boolean())
        -> ResolveStarted::0 | 1.
send_request_resolve(Dest, Op, SID, FromMyNode, FBDest, Options, IsFeedback) ->
    case FBDest of
        undefined -> Options1 = Options,
                     ResStarted = 0;
        _         -> Options1 = [{feedback_request, FBDest} | Options],
                     ResStarted = 1
    end,
    Options2 = [{from_my_node, FromMyNode bxor 1} | Options1],
    Tag = if IsFeedback -> continue_resolve;
             true       -> request_resolve
          end,
    case SID of
        null -> send(Dest, {Tag, Op, Options2});
        SID -> send(Dest, {Tag, SID, Op, Options2})
    end,
    ResStarted.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% resolve stats operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_stats_session_id(stats()) -> rrepair:session_id() | null.
get_stats_session_id(Stats) -> Stats#resolve_stats.session_id.

-spec get_stats_resolve_started(stats()) -> non_neg_integer().
get_stats_resolve_started(Stats) -> Stats#resolve_stats.resolve_started.

%% @doc merges two stats records with identical session_id, otherwise error will be raised
-spec merge_stats(stats(), stats()) -> stats() | error.
merge_stats(#resolve_stats{ session_id = ASID,
                            diff_size = ADiff,
                            regen_count = ARC,
                            regen_fail_count = AFC,
                            upd_fail_count = AUFC,
                            update_count = AUC },
            #resolve_stats{ session_id = BSID,
                            diff_size = BDiff,
                            regen_count = BRC,
                            regen_fail_count = BFC,
                            upd_fail_count = BUFC,
                            update_count = BUC }) ->
    case rrepair:session_id_equal(ASID, BSID) of
        true ->
            #resolve_stats{ session_id = ASID,
                            diff_size = ADiff + BDiff,
                            regen_count = ARC + BRC,
                            regen_fail_count = AFC + BFC,
                            upd_fail_count = AUFC + BUFC,
                            update_count = AUC + BUC };
        false ->
            log:log(error,"[ ~p ]: Trying to Merge stats with non identical rounds",[?MODULE]),
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send(Pid::comm:mypid(), Msg::comm:message() | comm:group_message()) -> ok.
send(Pid, Msg) ->
    ?TRACE_SEND(Pid, Msg),
    comm:send(Pid, Msg).

-spec send_local(Pid::comm:erl_local_pid(), Msg::comm:message() | comm:group_message()) -> ok.
send_local(Pid, Msg) ->
    ?TRACE_SEND(Pid, Msg),
    comm:send_local(Pid, Msg).

-spec entry_to_kvv(db_entry:entry_ex()) -> {?RT:key(), db_dht:value(), db_dht:version()}.
entry_to_kvv(Entry) ->
    {db_entry:get_key(Entry),
     db_entry:get_value(Entry),
     db_entry:get_version(Entry)}.

-spec send_stats(comm:mypid() | undefined, stats()) -> ok.
send_stats(undefined, _) ->
    ok;
send_stats(DestPid, Stats) ->
    send(DestPid, {resolve_stats, Stats}).

-spec print_resolve_stats(stats()) -> [any()].
print_resolve_stats(Stats) ->
    StatsL = tl(erlang:tuple_to_list(Stats)),
    FieldNames = record_info(fields, resolve_stats),
    [resolve_stats, lists:zip(FieldNames, StatsL)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(state()) -> state().
init(State) ->
    _ = erlang:monitor(process, State#rr_resolve_state.ownerPid),
    State.

-spec start() -> {ok, MyPid::pid()}.
start() ->
    State = #rr_resolve_state{ ownerPid = self(),
                               dhtNodePid = pid_groups:get_my(dht_node) },
    PidName = lists:flatten(io_lib:format("~s.~s", [?MODULE, randoms:getRandomString()])),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, State,
                             [{pid_groups_join_as, pid_groups:my_groupname(),
                               {short_lived, PidName}}]).
