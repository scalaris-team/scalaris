% @copyright 2011, 2012 Zuse Institute Berlin

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
-export([get_stats_session_id/1, get_stats_feedback/1, merge_stats/2]).
-export([print_resolve_stats/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([operation/0, options/0]).
-export_type([stats/0]).
-endif.

-type option()   :: feedback_response |
                    {feedback_request, comm:mypid()} | 
                    {send_stats, comm:mypid()} | %send stats to pid after completion
                    {session_id, rrepair:session_id()}.
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
         feedback_response= false  :: boolean() %true if this is a feedback response
         }).
-type stats() :: #resolve_stats{}.

-type operation() ::
    {key_upd, UniqueKvvListInAnyQ::kvv_list()} |
    {key_upd_send, DestPid::comm:mypid(), [?RT:key()]} |
    {interval_upd, intervals:interval(), UniqueKvvListInAnyQ::kvv_list()} |
    {interval_upd_send, intervals:interval(), DestPid::comm:mypid()} |
    {interval_upd_my, intervals:interval()}.

-record(rr_resolve_state,
        {
         ownerPid       = ?required(rr_resolve_state, ownerPid)         :: comm:erl_local_pid(),
         dhtNodePid     = ?required(rr_resolve_state, dhtNodePid)       :: comm:erl_local_pid(),
         operation      = undefined    									:: undefined | operation(),
         my_range       = undefined                                     :: undefined | intervals:interval(),
         feedbackDestPid= undefined                                     :: undefined | comm:mypid(),
         feedbackKvv    = {[], gb_trees:empty()}                        :: {MissingOnOther::kvv_list(), MyIKvTree::gb_tree()},
         send_stats     = undefined                                     :: undefined | comm:mypid(),
         stats          = #resolve_stats{}                              :: stats()
         }).
-type state() :: #rr_resolve_state{}.

-type message() ::
	% API
	{start, operation(), options()} |
    % internal
    {get_state_response, intervals:interval()} |    
    {update_key_entry_ack, [{db_entry:entry(), Exists::boolean(), Done::boolean()}]} |
    {'DOWN', MonitorRef::reference(), process, Owner::comm:erl_local_pid(), Info::any()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y,State), ok).
%-define(TRACE(X,Y,State), log:pal("~w [~p] " ++ X ++ "~n", [?MODULE, State#rr_resolve_state.ownerPid] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({start, Operation, Options}, State) ->
    FBDest = proplists:get_value(feedback_request, Options, undefined),
    FBResp = proplists:get_value(feedback_response, Options, false),
    StatsDest = proplists:get_value(send_stats, Options, undefined),
    SID = proplists:get_value(session_id, Options, null),
    NewState = State#rr_resolve_state{ operation = Operation,
                                       stats = #resolve_stats{ feedback_response = FBResp,
                                                               session_id = SID },
                                       feedbackDestPid = FBDest,
                                       send_stats = StatsDest },
    ?TRACE("RESOLVE START - Operation=~p~n FeedbackTo=~p - FeedbackResponse=~p~n SessionId:~p",
           [element(1, Operation), FBDest, FBResp, SID], NewState),
    comm:send_local(State#rr_resolve_state.dhtNodePid, {get_state, comm:this(), my_range}),
    NewState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: key_upd
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI}, State = 
       #rr_resolve_state{ operation = {key_upd, KvvList},
                          dhtNodePid = DhtPid, stats = Stats }) ->
    MyIKvvList = map_kvv_list(KvvList, MyI),
    ToUpdate = start_update_key_entry(MyIKvvList, comm:this(), DhtPid),
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p~n KVVListLen=~p ; ToUpdate=~p",
           [key_upd, Stats#resolve_stats.session_id, MyI, length(KvvList), ToUpdate], State),
    
    % Send entries in sender interval but not in sent KvvList
    % convert keys KvvList to a gb_tree for faster access checks
    MyIKvTree = lists:foldl(fun({KeyX, _ValX, VersionX}, TreeX) ->
                                    gb_trees:insert(KeyX, VersionX, TreeX)
                            end, gb_trees:empty(), MyIKvvList),
    
    % allow the garbage collection to clean up the KvvList here:
    NewState = State#rr_resolve_state{operation = {key_upd, []},
                                      stats = Stats#resolve_stats{diff_size = ToUpdate}},
    if ToUpdate =:= 0 ->
           shutdown(resolve_ok, NewState,
                    State#rr_resolve_state.feedbackDestPid, [], []);
       true ->
           % note: shutdown and feedback handled by update_key_entry_ack
           NewState#rr_resolve_state{feedbackKvv = {[], MyIKvTree}}
    end;

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {key_upd_send, _, KeyList},
                          dhtNodePid = DhtPid, stats = _Stats }) ->
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p",
           [key_upd_send, _Stats#resolve_stats.session_id, MyI], State),
    RepKeyInt = intervals:from_elements(
                    [K || X <- KeyList, K <- ?RT:get_replica_keys(X),
                          intervals:in(K, MyI)]),
    comm:send_local(DhtPid, {get_entries, self(), RepKeyInt}),
    State;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {key_upd_send, Dest, _},
                          feedbackDestPid = FBDest, stats = _Stats }) ->
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p",
           [key_upd_send, _Stats#resolve_stats.session_id, length(EntryList)], State),
    KvvList = [entry_to_kvv(E) || E <- EntryList],
    Options = ?IIF(FBDest =/= undefined, [{feedback_request, FBDest}], []),
    shutdown(resolve_ok, State, Dest, KvvList, Options);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: interval_upd 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {interval_upd, I, _KvvList},
                          dhtNodePid = DhtPid, stats = _Stats}) ->
    OpSIs = intervals:get_simple_intervals(I),
    ISec = lists:foldl(
             fun(Q, AccJ) ->
                     lists:foldl(
                       fun(OpSI, AccI) ->
                               OpI = intervals:simple_interval_to_interval(OpSI),
                               IX = intervals:intersection(OpI, Q),
                               case intervals:is_empty(IX) of
                                   true  -> AccI;
                                   false -> MI = rr_recon:map_interval(MyI, IX),
                                            intervals:union(AccI, MI)
                               end
                       end, AccJ, OpSIs)
             end, intervals:empty(), rr_recon:quadrant_intervals()),
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n IntervalBounds=~p~n MyInterval=~p~n IntersecBounds=~p",
           [interval_upd, _Stats#resolve_stats.session_id, intervals:get_bounds(I),
            MyI, intervals:get_bounds(ISec)], State),
    NewState = State#rr_resolve_state{ my_range = MyI },
    case intervals:is_empty(ISec) of
        false ->
            comm:send_local(DhtPid, {get_entries, self(), ISec}),
            NewState;
        true ->
            shutdown(resolve_abort, NewState,
                     State#rr_resolve_state.feedbackDestPid, [], [])
    end;

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {interval_upd_send, I, _Dest},
                          dhtNodePid = DhtPid, stats = _Stats }) ->
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n IntervalBounds=~p~n MyInterval=~p",
           [interval_upd_send, _Stats#resolve_stats.session_id, intervals:get_bounds(I), MyI], State),
    ISec = intervals:intersection(I, MyI),
    NewState = State#rr_resolve_state{ my_range = MyI },
    case intervals:is_empty(ISec) of
        false ->
            comm:send_local(DhtPid, {get_entries, self(), ISec}),
            NewState;
        true ->
            shutdown(resolve_abort, NewState,
                     State#rr_resolve_state.feedbackDestPid, [], [])
    end;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {interval_upd, I, KvvList},
                          my_range = MyI,
                          dhtNodePid = DhtPid,
                          feedbackDestPid = FBDest,
                          stats = Stats }) ->
    MyIKvvList = map_kvv_list(KvvList, MyI),
    ToUpdate = start_update_key_entry(MyIKvvList, comm:this(), DhtPid),
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p, KVVListLen=~p ; ToUpdate=~p",
           [interval_upd, Stats#resolve_stats.session_id, length(EntryList), length(KvvList), ToUpdate], State),
    
    % Send entries in sender interval but not in sent KvvList
    % convert keys KvvList to a gb_tree for faster access checks
    MyIKvTree = lists:foldl(fun({KeyX, _ValX, VersionX}, TreeX) ->
                                    gb_trees:insert(KeyX, VersionX, TreeX)
                            end, gb_trees:empty(), MyIKvvList),
    MissingOnOther = [MX || X <- EntryList,
                            not gb_trees:is_defined(element(1, (MX = entry_to_kvv(X))), MyIKvTree)],
    
    % allow the garbage collection to clean up the KvvList here:
    NewState = State#rr_resolve_state{operation = {interval_upd, I, []},
                                      stats = Stats#resolve_stats{diff_size = ToUpdate}},
    if ToUpdate =:= 0 ->
           shutdown(resolve_ok, NewState, FBDest, MissingOnOther, []);
       true ->
           % note: shutdown and feedback handled by update_key_entry_ack
           NewState#rr_resolve_state{feedbackKvv = {MissingOnOther, MyIKvTree}}
    end;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {interval_upd_send, I, Dest},
                          feedbackDestPid = FBDest,
                          stats = Stats }) ->
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p",
           [interval_upd_send, Stats#resolve_stats.session_id, length(EntryList)], State),
    Options = ?IIF(FBDest =/= undefined, [{feedback_request, FBDest}], []),
    KvvList = [entry_to_kvv(E) || E <- EntryList],
    SendList = make_unique_kvv(lists:keysort(1, KvvList), []),
    case Stats#resolve_stats.session_id of
        null -> comm:send(Dest, {request_resolve, {interval_upd, I, SendList}, Options});
        SID -> comm:send(Dest, {request_resolve, SID, {interval_upd, I, SendList}, Options})
    end,
    shutdown(resolve_ok, State, undefined, [], []);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: interval_upd_my
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI} = _Msg,
   State = #rr_resolve_state{operation = {interval_upd_my, I}}) ->
    ?TRACE("GET INTERVAL - Operation=~p~n IntervalBounds=~p~n MyInterval=~p",
           [interval_upd_my, intervals:get_bounds(I), MyI], State),
    ?ASSERT(State#rr_resolve_state.feedbackDestPid =:= undefined),
    ISec = intervals:intersection(MyI, I),
    NewState = State#rr_resolve_state{ my_range = MyI },
    case intervals:is_empty(ISec) of
        false -> case rrepair:select_sync_node(ISec, true) of
                     not_found ->
                         shutdown(resolve_abort, NewState, undefined, [], []);
                     DKey ->
                         % TODO: keep trying to resolve the whole intersection
                         %       e.g. by removing each sync interval and
                         %       continuing with the rest until the whole
                         %       interval is covered (at each step check with
                         %       the range reported from the dht_node!)
                         % -> the current implementation only tries once!
                         % note: bloom and art may not fully re-generate the
                         %       own range -> choose merkle_tree instead
                         comm:send_local(pid_groups:get_my(rrepair),
                                         {request_sync, merkle_tree, DKey}),
                         shutdown(resolve_ok, NewState, undefined, [], [])
                 end;
        true  -> shutdown(resolve_abort, NewState, undefined, [], [])
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({update_key_entry_ack, NewEntryList}, State =
       #rr_resolve_state{ operation = Op,
                          stats = #resolve_stats{ diff_size = _Diff,
                                                  regen_count = RegenOk,
                                                  update_count = UpdOk,
                                                  upd_fail_count = UpdFail,
                                                  regen_fail_count = RegenFail
                                                } = Stats,
                          feedbackDestPid = FBDest,
                          feedbackKvv = {MissingOnOther, MyIKvTree}
                        }) 
  when element(1, Op) =:= key_upd;
       element(1, Op) =:= interval_upd ->
    ?TRACE("GET ENTRY_ACK - Operation=~p~n SessionId:~p - #NewItems: ~p",
           [element(1, Op), Stats#resolve_stats.session_id, length(NewEntryList)], State),
    
    {NewUpdOk, NewUpdFail, NewRegenOk, NewRegenFail, NewFBItems} =
        integrate_update_key_entry_ack(
          NewEntryList, UpdOk, UpdFail, RegenOk, RegenFail, MissingOnOther, MyIKvTree,
          FBDest =/= undefined),
    
    NewStats = Stats#resolve_stats{update_count     = NewUpdOk + 1,
                                   regen_count      = NewRegenOk +1,
                                   upd_fail_count   = NewUpdFail + 1,
                                   regen_fail_count = NewRegenFail + 1},
    NewState = State#rr_resolve_state{stats = NewStats, feedbackKvv = {NewFBItems, MyIKvTree}},
    ?ASSERT(_Diff =:= (NewRegenOk + NewUpdOk + NewUpdFail + NewRegenFail)),
    ?TRACE("UPDATED = ~p - Regen=~p", [NewUpdOk, NewRegenOk], State),
    shutdown(resolve_ok, NewState, FBDest, NewFBItems, [feedback_response]);

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Starts updating the local entries with the given KvvList.
%%      -> Returns number of send update requests.
%%      PreCond: KvvList contains only unique keys
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec map_kvv_list(kvv_list(), intervals:interval()) -> kvv_list().
map_kvv_list(KvvList, MyI) ->
    ?ASSERT(length(KvvList) =:= length(lists:ukeysort(1, KvvList))),
    [{RKey, Val, Vers} || {Key, Val, Vers} <- KvvList,
                          RKey <- ?RT:get_replica_keys(Key),
                          intervals:in(RKey, MyI)].

-spec start_update_key_entry(MyIKvvList::kvv_list(), comm:mypid(),
                             comm:erl_local_pid()) -> non_neg_integer().
start_update_key_entry(MyIKvvList, MyPid, DhtPid) ->
    comm:send_local(DhtPid, {update_key_entry, MyPid, MyIKvvList}),
    length(MyIKvvList).

-spec integrate_update_key_entry_ack(
        [{Entry::db_entry:entry(), Exists::boolean(), Done::boolean()}],
        UpdOk::non_neg_integer(), UpdFail::non_neg_integer(),
        RegenOk::non_neg_integer(), RegenFail::non_neg_integer(),
        FBItems::kvv_list(), OtherKvTree::gb_tree(), FBOn::boolean())
        -> {UpdOk::non_neg_integer(), UpdFail::non_neg_integer(),
            RegenOk::non_neg_integer(), RegenFail::non_neg_integer(),
            FBItems::kvv_list()}.
integrate_update_key_entry_ack([], UpdOk, UpdFail, RegenOk, RegenFail, FBItems,
                               _OtherKvTree, _FBOn) ->
    {UpdOk, UpdFail, RegenOk, RegenFail, FBItems};
integrate_update_key_entry_ack([{Entry, Exists, Done} | Rest], UpdOk, UpdFail,
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
           integrate_update_key_entry_ack(
             Rest, UpdOk + 1, UpdFail, RegenOk, RegenFail, NewFBItems, OtherKvTree, FBOn);
       Done andalso not Exists ->
           integrate_update_key_entry_ack(
             Rest, UpdOk, UpdFail, RegenOk + 1, RegenFail, NewFBItems, OtherKvTree, FBOn);
       not Done and Exists ->
           integrate_update_key_entry_ack(
             Rest, UpdOk, UpdFail + 1, RegenOk, RegenFail, NewFBItems, OtherKvTree, FBOn);
       not Done and not Exists ->
           integrate_update_key_entry_ack(
             Rest, UpdOk, UpdFail, RegenOk, RegenFail + 1, NewFBItems, OtherKvTree, FBOn)
    end.

-spec shutdown(exit_reason(), state(), undefined | comm:mypid(), kvv_list(),
               options()) -> kill.
shutdown(_Reason, #rr_resolve_state{ownerPid = Owner, send_stats = SendStats,
                                    stats = Stats, operation = _Op} = _State,
         KUDest, KUItems, KUOptions) ->
    ?TRACE("SHUTDOWN ~p - Operation=~p~n SessionId:~p~n ~p items via key_upd to ~p~n Items: ~.2p",
           [_Reason, element(1, _Op), Stats#resolve_stats.session_id,
            length(KUItems), KUDest, KUItems], _State),
    send_key_upd(KUDest, KUItems, Stats#resolve_stats.session_id, KUOptions),
    send_stats(SendStats, Stats),
    comm:send_local(Owner, {resolve_progress_report, self(), Stats}),
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% resolve stats operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_stats_session_id(stats()) -> rrepair:session_id().
get_stats_session_id(Stats) -> Stats#resolve_stats.session_id.

-spec get_stats_feedback(stats()) -> boolean().
get_stats_feedback(Stats) -> Stats#resolve_stats.feedback_response.

%% @doc merges two stats records with identical session_id, otherwise error will be raised
-spec merge_stats(stats(), stats()) -> stats() | error.
merge_stats(#resolve_stats{ session_id = ASID,
                            diff_size = ADiff,
                            feedback_response = AFB,
                            regen_count = ARC,
                            regen_fail_count = AFC,
                            upd_fail_count = AUFC,
                            update_count = AUC },
            #resolve_stats{ session_id = BSID,
                            diff_size = BDiff,
                            feedback_response = BFB,
                            regen_count = BRC,
                            regen_fail_count = BFC,
                            upd_fail_count = BUFC,
                            update_count = BUC }) ->
    case rrepair:session_id_equal(ASID, BSID) of
        true ->
            #resolve_stats{ session_id = ASID,
                            diff_size = ADiff + BDiff,
                            feedback_response = AFB orelse BFB,
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

-spec entry_to_kvv(db_entry:entry()) -> {?RT:key(), db_dht:value(), db_dht:version()}.
entry_to_kvv(Entry) ->
    % map to any quadrant (here: 1) in order to be able to make replicated entries unique
    {rr_recon:map_key_to_quadrant(db_entry:get_key(Entry), 1),
     db_entry:get_value(Entry),
     db_entry:get_version(Entry)}.

make_unique_kvv([], Acc) -> Acc;
make_unique_kvv([H | T], []) -> make_unique_kvv(T, [H]);
make_unique_kvv([H | T], [AccH | AccT] = Acc) ->
    case element(1, H) =:= element(1, AccH) of
        true -> 
            case element(3, H) > element(3, AccH) of
                true -> make_unique_kvv(T, [H|AccT]);
                false -> make_unique_kvv(T, Acc)
            end;
        false -> make_unique_kvv(T, [H|Acc])
    end.

-spec send_key_upd(Dest::comm:mypid() | undefined, Items::kvv_list(), 
                   rrepair:session_id() | null, options()) -> ok.
send_key_upd(undefined, _, _, _) ->
    ok;
send_key_upd(DestPid, Items, SID, Options) ->
    SendList = make_unique_kvv(lists:keysort(1, Items), []),
    if SID =:= null ->
           comm:send(DestPid, {request_resolve, {key_upd, SendList}, Options});
       true ->
           comm:send(DestPid, {request_resolve, SID, {key_upd, SendList}, Options})
    end.

-spec send_stats(comm:mypid() | undefined, stats()) -> ok.
send_stats(undefined, _) -> 
    ok;
send_stats(DestPid, Stats) ->
    comm:send(DestPid, {resolve_stats, Stats}).

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
                             [{pid_groups_join_as, pid_groups:my_groupname(), {short_lived, PidName}}]).
