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
%%         Sync-Modes:
%%           1) key_upd: updates local kv-pairs with received kv-list, if received kv is newer
%%           2) key_upd_send: creates kv-list out of a given key-list and sends it to dest
%%         Options:
%%           1) Feedback: sends data ids to Node (A) which are outdated at (A)
%%           2) Send_Stats: sends resolution stats to given pid
%%         Usage:
%%           rrepair process provides API for resolve requests
%% @end
%% @version $Id$

-module(rr_resolve).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/0, start/1]).
-export([get_stats_session_id/1, get_stats_feedback/1, merge_stats/2]).
-export([print_resolve_stats/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([operation/0, options/0]).
-export_type([stats/0]).
-endif.

-type option()   :: feedback_response |
                    {feedback, comm:mypid()} | 
                    {send_stats, comm:mypid()}. %send stats to pid after completion
-type options()  :: [option()].
-type feedback() :: {nil | comm:mypid(),        %feedback destination adress
                     ?DB:kvv_list()}.

-record(resolve_stats,
        {
         session_id       = null   :: rrepair:session_id() | null,
         diff_size        = 0      :: non_neg_integer(),
         regen_count      = 0      :: non_neg_integer(),
         update_count     = 0      :: non_neg_integer(),
         upd_fail_count   = 0      :: non_neg_integer(),
         regen_fail_count = 0      :: non_neg_integer(),
         feedback_response= false  :: boolean()            %true if this is a feedback response
         }).
-type stats() :: #resolve_stats{}.

-type operation() ::
    {key_upd, ?DB:kvv_list()} |
    {key_upd_send, DestPid::comm:mypid(), [?RT:key()]}.

-record(rr_resolve_state,
        {
         ownerLocalPid  = ?required(rr_resolve_state, ownerLocalPid)    :: comm:erl_local_pid(),
         ownerRemotePid = ?required(rr_resolve_state, ownerRemotePid)   :: comm:mypid(),
         ownerMonitor   = null                                          :: null | reference(),
         dhtNodePid     = ?required(rr_resolve_state, dhtNodePid)       :: comm:erl_local_pid(),
         operation      = nil        									:: nil | operation(),
         stats          = #resolve_stats{}                              :: stats(),
         feedback       = {nil, []}                                     :: feedback(),           
         send_stats     = nil                                           :: nil | comm:mypid()
         }).
-type state() :: #rr_resolve_state{}.

-type message() ::
	% API
	{start, operation(), options()} |
    % internal
    {get_state_response, intervals:interval()} |
    {update_key_entry_ack, db_entry:entry(), Exists::boolean(), Done::boolean()} |
    {shutdown, atom()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

on({start, Operation, Options}, State = #rr_resolve_state{ dhtNodePid = DhtPid, 
                                                           stats = Stats }) ->
    FBDest = proplists:get_value(feedback, Options, nil),
    FBResp = proplists:get_value(feedback_response, Options, false),
    StatsDest = proplists:get_value(send_stats, Options, nil),
    NewState = State#rr_resolve_state{ operation = Operation,
									   stats = Stats#resolve_stats{ feedback_response = FBResp },
									   feedback = {FBDest, []},
									   send_stats = StatsDest },
    ?TRACE("RESOLVE - START~nOperation=~p - FeedbackTo=~p - FeedbackResponse=~p", 
           [element(1, Operation), FBDest, FBResp]),
	comm:send_local(DhtPid, {get_state, comm:this(), my_range}),
	NewState;

on({get_state_response, MyI}, State = 
       #rr_resolve_state{ operation = {key_upd, KvvList},
                          dhtNodePid = DhtPid,
                          stats = Stats                          
                          }) ->
    MyPid = comm:this(),
    FullKvvList = [{X, Value, Vers} 
                   || {K, Value, Vers} <- KvvList,
                      X <- ?RT:get_replica_keys(K),
                      intervals:in(X, MyI)],
    UpdList = make_unique_kvv(lists:keysort(1, FullKvvList), []),
    _ = [comm:send_local(DhtPid, {update_key_entry, MyPid, Key, Val, Vers}) || {Key, Val, Vers} <- UpdList],
    ToUpdate = length(UpdList),

    %kill is done by update_key_entry_ack
    ?TRACE("GET INTERVAL - KEY UPD - KVVListLen=~p ; ToUpdate=~p", [length(KvvList), ToUpdate]),
    if ToUpdate =:= 0 -> comm:send_local(self(), {shutdown, resolve_ok});
       true -> ok
    end,
    State#rr_resolve_state{ stats = Stats#resolve_stats{ diff_size = ToUpdate } };

on({get_state_response, MyI}, State =
       #rr_resolve_state{ operation = {key_upd_send, _, KeyList},
                          dhtNodePid = DhtPid }) ->    
    FKeyList = [K || X <- KeyList, K <- ?RT:get_replica_keys(X), 
                     intervals:in(K, MyI)],
    KeyTree = gb_sets:from_list(FKeyList),
    comm:send_local(DhtPid, {get_entries, self(),
                             fun(X) -> gb_sets:is_element(db_entry:get_key(X), KeyTree) end,
                             fun(X) -> {rr_recon:map_key_to_quadrant(db_entry:get_key(X), 1),
                                        db_entry:get_value(X), 
                                        db_entry:get_version(X)} end}),
    State;

on({get_entries_response, KVVList}, State =
       #rr_resolve_state{ operation = {key_upd_send, Dest, _},
                          feedback = {FB, _},
                          stats = Stats }) ->
    Options = ?IIF(FB =/= nil, [{feedback, FB}], []),
    SendList = make_unique_kvv(lists:keysort(1, KVVList), []),    
    case Stats#resolve_stats.session_id of
        null -> comm:send(Dest, {request_resolve, {key_upd, SendList}, Options});
        SID -> comm:send(Dest, {request_resolve, SID, {key_upd, SendList}, Options})
    end,
    comm:send_local(self(), {shutdown, resolve_ok}),
    State;

on({update_key_entry_ack, Entry, Exists, Done}, State =
       #rr_resolve_state{ operation = {key_upd, _},
                          stats = #resolve_stats{ diff_size = Diff,
                                                  regen_count = RegenOk,
                                                  update_count = UpdOk, 
                                                  upd_fail_count = UpdFail,
                                                  regen_fail_count = RegenFail
                                                } = Stats,                          
                          feedback = FB = {DoFB, FBItems}
                        }) ->
    NewStats = if
                   Done andalso Exists -> Stats#resolve_stats{ update_count = UpdOk +1 };
                   Done andalso not Exists -> Stats#resolve_stats{ regen_count = RegenOk +1 };
                   not Done and Exists -> Stats#resolve_stats{ upd_fail_count = UpdFail + 1 };
                   not Done and not Exists -> Stats#resolve_stats{ regen_fail_count = RegenFail + 1 }
               end,
    NewFB = if
                not Done andalso Exists andalso DoFB =/= nil -> 
                    {DoFB, [{rr_recon:map_key_to_quadrant(db_entry:get_key(Entry), 1), %db_entry:get_key(Entry),
                             db_entry:get_value(Entry),
                             db_entry:get_version(Entry)} | FBItems]};
                true -> FB
            end,
    if
        (Diff -1) =:= (RegenOk + UpdOk + UpdFail + RegenFail) ->
                send_feedback(NewFB, Stats#resolve_stats.session_id),
                comm:send_local(self(), {shutdown, resolve_ok});
        true -> ok
    end,
    State#rr_resolve_state{ stats = NewStats, feedback = NewFB };

on({shutdown, _}, #rr_resolve_state{ ownerLocalPid = Owner,
                                     ownerMonitor = Mon, 
                                     send_stats = SendStats,
                                     stats = Stats }) ->
    erlang:demonitor(Mon),
    send_stats(SendStats, Stats),
    comm:send_local(Owner, {resolve_progress_report, self(), Stats}),
    kill;

on({'DOWN', _MonitorRef, process, _Owner, _Info}, _State) ->
    log:log(info, "shutdown rr_resolve due to rrepair shut down", []),
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

-spec send_feedback(feedback(), rrepair:session_id()) -> ok.
send_feedback({nil, _}, _) -> ok;
send_feedback({Dest, Items}, null) ->
    comm:send(Dest, {request_resolve, {key_upd, gb_sets:to_list(gb_sets:from_list(Items))}, [feedback_response]});
send_feedback({Dest, Items}, SID) ->
    comm:send(Dest, {request_resolve, SID, {key_upd, gb_sets:to_list(gb_sets:from_list(Items))}, [feedback_response]}).

-spec send_stats(nil | comm:mypid(), stats()) -> ok.
send_stats(nil, _) -> ok;
send_stats(SendStats, Stats) ->
    comm:send(SendStats, {resolve_stats, Stats}).

-spec print_resolve_stats(stats()) -> [any()].
print_resolve_stats(Stats) ->
    FieldNames = record_info(fields, resolve_stats),
    Res = util:for_to_ex(1, length(FieldNames), 
                         fun(I) ->
                                 {lists:nth(I, FieldNames), erlang:element(I + 1, Stats)}
                         end),    
    [erlang:element(1, Stats), lists:flatten(Res)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(state()) -> state().
init(State) ->
    Mon = erlang:monitor(process, State#rr_resolve_state.ownerLocalPid),
    State#rr_resolve_state{ ownerMonitor = Mon }.

-spec start() -> {ok, MyPid::pid()}.
start() ->        
    gen_component:start(?MODULE, fun ?MODULE:on/2, get_start_state(), []).

-spec start(rrepair:session_id()) -> {ok, MyPid::pid()}.
start(SID) ->        
    State = get_start_state(),
    Stats = State#rr_resolve_state.stats,
    gen_component:start(?MODULE, fun ?MODULE:on/2, 
                        State#rr_resolve_state{ stats = Stats#resolve_stats{ session_id = SID } }, []).

-spec get_start_state() -> state().
get_start_state() ->
    #rr_resolve_state{ ownerLocalPid = self(), 
                       ownerRemotePid = comm:this(), 
                       dhtNodePid = pid_groups:get_my(dht_node)
                     }.
