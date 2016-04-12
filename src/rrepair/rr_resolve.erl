% @copyright 2011-2015 Zuse Institute Berlin

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
%%           3) interval_upd_my: tries to resolve items from the given interval by
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
-include("client_types.hrl").

-export([init/1, on/2, start/1]).
-export([get_stats_session_id/1, merge_stats/2]).
-export([print_resolve_stats/1]).

% for tester:
-export([merge_stats_feeder/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([operation/0, options/0, kvv_list/0]).
-export_type([stats/0]).

-type option()   :: {feedback_request, comm:mypid()} |
                    {from_my_node, 0 | 1}.
-type options()  :: [option()].
-type kvv_list() :: [{?RT:key(), db_dht:value(), client_version()}].
-type exit_reason() :: resolve_ok | resolve_abort.

-record(resolve_stats,
        {
         session_id       = ?required(resolve_stats, session_id) :: rrepair:session_id() | null,
         diff_size        = 0      :: non_neg_integer(),
         regen_count      = 0      :: non_neg_integer(),
         update_count     = 0      :: non_neg_integer(),
         upd_fail_count   = 0      :: non_neg_integer(),
         regen_fail_count = 0      :: non_neg_integer()
         }).
-type stats() :: #resolve_stats{}.

-type operation() ::
    {?key_upd, KvvListInAnyQ::kvv_list(), ReqKeys::[?RT:key()]} |
    {key_upd_send, DestPid::comm:mypid(), SendKeys::[?RT:key()], ReqKeys::[?RT:key()]} |
    {interval_upd_my, intervals:interval()}.

-record(rr_resolve_state,
        {
         ownerPid       = ?required(rr_resolve_state, ownerPid)   :: pid(),
         operation      = undefined                               :: undefined | operation(),
         my_range       = undefined                               :: undefined | intervals:interval(),
         fb_dest_pid    = undefined                               :: undefined | comm:mypid(),
         fb_send_kvv    = []                                      :: OutdatedOnOther::kvv_list(),
         fb_had_kvv_req = false                                   :: NonEmptyReqList::boolean(),
         fb_send_kvv_req= []                                      :: RequestedByOther::kvv_list(),
         other_kv_tree  = mymaps:new()                            :: MyIOtherKvTree::mymaps:mymap(), % ?RT:key() => client_version()
         stats          = ?required(rr_resolve_state, stats)      :: stats(),
         from_my_node   = 1                                       :: 0 | 1
         }).
-type state() :: #rr_resolve_state{}.

-type message() ::
    % API
    {start, operation(), options(), StartTag::atom()} |
    % internal
    {get_entries_response, db_dht:db_as_list()} |
    {get_state_response, intervals:interval()} |
    {update_key_entries_ack, [{db_entry:entry_ex(), Exists::boolean(), Done::boolean()}]} |
    {'DOWN', MonitorRef::reference(), process, Owner::pid(), Info::any()}.

-include("gen_component.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), log:pal("~w [~p:~p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_START_END(X,Y), ok).
%-define(TRACE_START_END(X,Y), log:pal("~w [~p:~p] " ++ X ++ "~n", [?MODULE, pid_groups:my_groupname(), self()] ++ Y)).
-define(TRACE_SEND(Pid, Msg), ?TRACE("to ~p:~.0p: ~.0p~n", [pid_groups:group_of(comm:make_local(comm:get_plain_pid(Pid))), Pid, Msg])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state() | kill.

on({start, Operation, Options, _StartCont}, State) ->
    FBDest = proplists:get_value(feedback_request, Options, undefined),
    FromMyNode = proplists:get_value(from_my_node, Options, 1),
    NewState = State#rr_resolve_state{ operation = Operation,
                                       fb_dest_pid = FBDest,
                                       from_my_node = FromMyNode },
    ?TRACE_START_END("RESOLVE ~s - Operation=~p~n FeedbackTo=~p~n SessionId:~p (MyNode: ~p)",
           [_StartCont, util:extint2atom(element(1, Operation)), FBDest,
            NewState#rr_resolve_state.stats#resolve_stats.session_id,
            FromMyNode]),
    send_local(pid_groups:get_my(dht_node), {get_state, comm:this(), my_range}),
    NewState;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% MODE: key_upd
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {?key_upd, KvvList, ReqKeys},
                          stats = _Stats }) ->
    MyIOtherKvvList = map_kvv_list(KvvList, MyI),
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p~n KVVListLen=~p",
           [key_upd, _Stats#resolve_stats.session_id, MyI, length(KvvList)]),

    % send requested entries (similar to key_upd_send handling)
    RepKeyInt = intervals:from_elements(map_key_list(ReqKeys, MyI)),
    send_local(pid_groups:get_my(dht_node), {get_entries, self(), RepKeyInt}),

    % send entries in sender interval but not in sent KvvList
    % convert keys KvvList to a map for faster access checks
    MyIOtherKvTree = make_other_kv_tree(MyIOtherKvvList),

    % allow the garbage collection to clean up the ReqKeys here:
    % also update the KvvList
    State#rr_resolve_state{operation = {?key_upd, MyIOtherKvvList, []},
                           other_kv_tree = MyIOtherKvTree,
                           fb_had_kvv_req = ReqKeys =/= []};

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {?key_upd, MyIOtherKvvList, []},
                          fb_send_kvv_req = [],
                          stats = Stats}) ->
    % note: EntryList may not be unique! - it is made unique in shutdown/2 though
    KvvList = [entry_to_kvv(E) || E <- EntryList],
    ToUpdate = start_update_key_entries(MyIOtherKvvList, comm:this(),
                                        pid_groups:get_my(dht_node)),
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p ; ToUpdate=~p - #Items: ~p",
           [key_upd, Stats#resolve_stats.session_id, ToUpdate, length(EntryList)]),

    % allow the garbage collection to clean up the KvvList here:
    NewState =
        State#rr_resolve_state{operation = {?key_upd, [], []},
                               stats = Stats#resolve_stats{diff_size = ToUpdate},
                               fb_send_kvv_req = KvvList},

    if ToUpdate =:= 0 ->
           % use the same options as above in get_state_response:
           shutdown(resolve_ok, NewState);
       true ->
           % note: shutdown and feedback handled by update_key_entries_ack
           NewState
    end;

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {key_upd_send, _Dest, SendKeys, _ReqKeys},
                          stats = _Stats }) ->
    ?TRACE("GET INTERVAL - Operation=~p~n SessionId:~p~n MyInterval=~p",
           [key_upd_send, _Stats#resolve_stats.session_id, MyI]),
    SendKeysMappedInterval = intervals:from_elements(map_key_list(SendKeys, MyI)),
    send_local(pid_groups:get_my(dht_node), {get_entries, self(), SendKeysMappedInterval}),
    State;

on({get_entries_response, EntryList}, State =
       #rr_resolve_state{ operation = {key_upd_send, Dest, _, ReqKeys},
                          fb_dest_pid = FBDest, from_my_node = FromMyNode,
                          stats = Stats }) ->
    SID = Stats#resolve_stats.session_id,
    ?TRACE("GET ENTRIES - Operation=~p~n SessionId:~p - #Items: ~p",
           [key_upd_send, SID, length(EntryList)]),

    % note: EntryList may not be unique!
    KvvList = make_unique_kvv([entry_to_kvv(E) || E <- EntryList]),
    ?DBG_ASSERT2(length(ReqKeys) =:= length(lists:usort(ReqKeys)),
                 {non_unique_req_list, ReqKeys}),
    ?TRACE_START_END("sending key_upd with ~B items and ~B requests",
                     [length(KvvList), length(ReqKeys)]),
    send_request_resolve(Dest, {?key_upd, KvvList, ReqKeys}, SID,
                         FromMyNode, FBDest, []),
    NewState = State#rr_resolve_state{fb_dest_pid = undefined},
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
  when element(1, Op) =:= ?key_upd ->
    ?TRACE("GET ENTRY_ACK - Operation=~p~n SessionId:~p - #NewItems: ~p",
           [util:extint2atom(element(1, Op)), Stats#resolve_stats.session_id,
            length(NewEntryList)]),

    {NewUpdOk, NewUpdFail, NewRegenOk, NewRegenFail, NewFBItems} =
        integrate_update_key_entries_ack(
          NewEntryList, UpdOk, UpdFail, RegenOk, RegenFail, MissingOnOther, MyIOtherKvTree,
          FBDest =/= undefined),

    NewStats = Stats#resolve_stats{update_count     = NewUpdOk,
                                   regen_count      = NewRegenOk,
                                   upd_fail_count   = NewUpdFail,
                                   regen_fail_count = NewRegenFail},
    NewState = State#rr_resolve_state{stats = NewStats, fb_send_kvv = NewFBItems},
    ?DBG_ASSERT(_Diff =:= (NewRegenOk + NewUpdOk + NewUpdFail + NewRegenFail)),
    ?TRACE("UPDATED = ~p - Regen=~p", [NewUpdOk, NewRegenOk]),
    shutdown(resolve_ok, NewState);

on({'DOWN', MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "[ ~p - ~p] shutdown due to rrepair shut down", [?MODULE, comm:this()]),
    gen_component:demonitor(MonitorRef),
    kill.

%% @doc Maps the given tuple list (with keys as first elements) to the MyI
%%      interval.
%%      Precond: tuple list with unique keys
%%      Note: mapped tuples are not unique if MyI covers multiple replicas!
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
%%      Note: mapped keys are not unique if MyI covers multiple replicas!
-spec map_key_list([?RT:key()], MyI::intervals:interval()) -> [?RT:key()].
map_key_list(KeyList, MyI) ->
    ?DBG_ASSERT(length(KeyList) =:= length(lists:usort(KeyList))),
    [RKey || Key <- KeyList,
             RKey <- ?RT:get_replica_keys(Key),
             intervals:in(RKey, MyI)].

%% @doc Starts updating the local entries with the given KvvList.
%%      -> Returns number of send update requests.
%%      PreCond: KvvList contains only unique keys
-spec start_update_key_entries(MyIOtherKvvList::kvv_list(), comm:mypid(),
                               comm:erl_local_pid()) -> UpdRequests::non_neg_integer().
start_update_key_entries([], _MyPid, _DhtPid) -> 0;
start_update_key_entries(MyIOtherKvvList, MyPid, DhtPid) ->
    send_local(DhtPid, {update_key_entries, MyPid, MyIOtherKvvList}),
    length(MyIOtherKvvList).

-spec integrate_update_key_entries_ack(
        [{Entry::db_entry:entry_ex(), Exists::boolean(), Done::boolean()}],
        UpdOk::non_neg_integer(), UpdFail::non_neg_integer(),
        RegenOk::non_neg_integer(), RegenFail::non_neg_integer(),
        FBItems::kvv_list(), OtherKvTree::mymaps:mymap(), FBOn::boolean())
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
               case any_replica_in_tree(?RT:get_replica_keys(db_entry:get_key(Entry)), OtherKvTree) of
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

%% @doc Tries to find any key from a list in the tree.
-spec any_replica_in_tree(RKeys::[Key], Tree::mymaps:mymap())
        -> none | {value, Val} when is_subtype(Key, any()), is_subtype(Val, any()).
any_replica_in_tree([], _Tree) ->
    none;
any_replica_in_tree([Key | Rest], Tree) ->
    case mymaps:find(Key, Tree) of
        error -> any_replica_in_tree(Rest, Tree);
        {ok, X} -> {value, X}
    end.

-spec shutdown(exit_reason(), state()) -> kill.
shutdown(_Reason, #rr_resolve_state{ownerPid = Owner,
                                    stats = Stats,
                                    operation = _Op, fb_dest_pid = FBDest,
                                    fb_send_kvv = FbKVV,
                                    fb_had_kvv_req = SendReqKeyReply,
                                    fb_send_kvv_req = FbReqKVV,
                                    from_my_node = FromMyNode} = _State) ->
    ?TRACE_START_END("SHUTDOWN ~p - Operation=~p~n SessionId:~p (MyNode: ~p)~n"
                     " feedback: ~.2p",
           [_Reason, util:extint2atom(element(1, _Op)), Stats#resolve_stats.session_id,
            FromMyNode, ?IIF(FBDest =/= undefined,
                             {length(FbKVV), "items to", FBDest, "via key_upd:", FbKVV},
                             none)]),
        case FBDest of
            undefined ->
                ok;
            _ when not SendReqKeyReply ->
                FbKVV1 = make_unique_kvv(FbKVV),
                send_request_resolve(FBDest, {?key_upd, FbKVV1, []},
                                     Stats#resolve_stats.session_id,
                                     FromMyNode, undefined, []);
            _ ->
                % feedback item lists may not be unique -> make them!
                FbKVV1 = make_unique_kvv(FbKVV),
                FbReqKVV1 = make_unique_kvv(FbReqKVV),
                send_request_resolve(FBDest, {?key_upd, FbKVV1, []},
                                     Stats#resolve_stats.session_id,
                                     FromMyNode, undefined, []),
                send_request_resolve(FBDest, {?key_upd, FbReqKVV1, []},
                                     Stats#resolve_stats.session_id,
                                     FromMyNode, comm:make_global(Owner), [])
        end,
    % note: do not propagate the SessionId unless we report to the node
    %       the request came from (indicated by FromMyNode =:= 1),
    %       otherwise the resolve_progress_report on the other node will
    %       be counted for the session's rs_finish and it will not match
    %       its rs_expected anymore!
    if FromMyNode =:= 1 ->
           send_local(Owner, {resolve_progress_report, self(), Stats});
       true ->
           send_local(Owner, {resolve_progress_report, self(),
                              Stats#resolve_stats{session_id = null}})
    end,
    kill.

%% @doc Makes the given tuple list (with keys as first elements) unique, i.e.
%%      there are no duplicate replicas of keys in the result tuple list.
%%      Note: Assumes, KVs at the same node have the same version and chooses
%%            an arbitrary key for each replica group.
-spec make_unique_kvv([Tpl]) -> [Tpl]
        when is_subtype(Tpl, {?RT:key()} |
                             {?RT:key(), term()} |
                             {?RT:key(), term(), term()}).
make_unique_kvv([]) -> [];
make_unique_kvv([_|_] = KVV) ->
    MKVV = [{rr_recon:map_key_to_quadrant(element(1, Val), 1), Val} || Val <- KVV],
    [Val || {_MappedKeyX, Val} <- lists:ukeysort(1, MKVV)].

%% @doc Creates a Key-Version tree from another node's KVV list containing one
%%      entry for every replica key of the KVV list.
%%      Note: Assumes, KVs at the same node have the same version and sets
%%            an (arbitrary) version from these for each replica group.
-spec make_other_kv_tree([{?RT:key(), Val::term(), client_version()}])
        -> mymaps:mymap().
make_other_kv_tree(KVV) ->
    mymaps:from_list([{KeyX, VersionX} || {KeyX, _ValX, VersionX} <- KVV]).

-spec send_request_resolve(Dest::comm:mypid(), Op::operation(),
                           SID::rrepair:session_id() | null,
                           FromMyNode::0 | 1, FBDest::comm:mypid() | undefined,
                           Options::options())
        -> ok.
send_request_resolve(Dest, Op, SID, FromMyNode, FBDest, Options) ->
    Options1 = case FBDest of
                   undefined -> Options;
                   _         -> [{feedback_request, FBDest} | Options]
               end,
    Options2 = [{from_my_node, FromMyNode bxor 1} | Options1],
    send(Dest, {continue_resolve, SID, Op, Options2}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% resolve stats operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_stats_session_id(stats()) -> rrepair:session_id() | null.
get_stats_session_id(Stats) -> Stats#resolve_stats.session_id.

-spec merge_stats_feeder(stats(), stats()) -> {stats(), stats()}.
merge_stats_feeder(A, B) ->
    {A, B#resolve_stats{session_id = A#resolve_stats.session_id}}.

%% @doc Merges two stats records with an identical session_id
%%      (otherwise error will be raised).
-spec merge_stats(stats(), stats()) -> stats().
merge_stats(#resolve_stats{ session_id = SID,
                            diff_size = ADiff,
                            regen_count = ARC,
                            regen_fail_count = AFC,
                            upd_fail_count = AUFC,
                            update_count = AUC },
            #resolve_stats{ session_id = SID,
                            diff_size = BDiff,
                            regen_count = BRC,
                            regen_fail_count = BFC,
                            upd_fail_count = BUFC,
                            update_count = BUC }) ->
    #resolve_stats{ session_id = SID,
                    diff_size = ADiff + BDiff,
                    regen_count = ARC + BRC,
                    regen_fail_count = AFC + BFC,
                    upd_fail_count = AUFC + BUFC,
                    update_count = AUC + BUC }.

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

-spec entry_to_kvv(db_entry:entry_ex()) -> {?RT:key(), db_dht:value(), client_version()}.
entry_to_kvv(Entry) ->
    {db_entry:get_key(Entry),
     db_entry:get_value(Entry),
     db_entry:get_version(Entry)}.

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
    _ = gen_component:monitor(State#rr_resolve_state.ownerPid),
    State.

-spec start(SessionId::rrepair:session_id() | null) -> {ok, MyPid::pid()}.
start(SessionId) ->
    State = #rr_resolve_state{ownerPid = self(),
                              stats = #resolve_stats{session_id = SessionId}},
    PidName = lists:flatten(io_lib:format("~s.~s", [?MODULE, randoms:getRandomString()])),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, State,
                             [{pid_groups_join_as, pid_groups:my_groupname(),
                               {short_lived, PidName}}]).
