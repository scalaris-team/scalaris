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
%% @doc    replica update resolution module
%%         Updates local and/or remote Key-Value-Pairs (kv-pair)
%%         Sync-Modes:
%%           1) key_upd: updates local kv-pairs with received kv-list, if received kv is newer
%%           2) key_sync: synchronizes kv-pairs between two nodes (only for small lists)
%%         Options:
%%           1) Feedback: sends data ids to Node (A) which are outdated at (A)
%%           2) Send_Stats: sends resolution stats to given pid
%%         Examples: 
%%            1) remote node D should get one kvv-pair (key,value,version),
%%               >>comm:send(RemoteRepUpdPid, {request_resolve, {key_upd, [{Key, Value, Version}]}, []}).
%% @end
%% @version $Id$

-module(rep_upd_resolve).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/3]).
-export([print_resolve_stats/1]).


-ifdef(with_export_type_support).
-export_type([operation/0, options/0]).
-export_type([stats/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

-define(IIF(C, A, B), case C of
                          true -> A;
                          _ -> B
                      end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type option()   :: feedback_response |
                    {feedback, comm:mypid()} | 
                    {send_stats, comm:mypid()}. %send stats to pid after completion
-type options()  :: [option()].
-type feedback() :: {nil | comm:mypid(),        %feedback destination adress
                     ?DB:kvv_list()}.

-record(resolve_stats,
        {
         round            = {0, 0} :: rep_upd:round(),
         diff_size        = 0      :: non_neg_integer(),
         regen_count      = 0      :: non_neg_integer(),
         update_count     = 0      :: non_neg_integer(),
         upd_fail_count   = 0      :: non_neg_integer(),
         regen_fail_count = 0      :: non_neg_integer(),
         comment          = []     :: [any()]
         }).
-type stats() :: #resolve_stats{}.

-type operation() ::
    {key_upd, ?DB:kvv_list()} |
    {key_sync, DestPid::comm:mypid(), [?RT:key()]}.

-record(ru_resolve_state,
        {
         ownerLocalPid  = ?required(ru_resolve_state, ownerLocalPid)    :: comm:erl_local_pid(),
         ownerRemotePid = ?required(ru_resolve_state, ownerRemotePid)   :: comm:mypid(),         
         dhtNodePid     = ?required(ru_resolve_state, dhtNodePid)       :: comm:erl_local_pid(),
         operation      = ?required(ru_resolve_state, operation)        :: operation(),
         stats          = #resolve_stats{}                              :: stats(),
         feedback       = {nil, []}                                     :: feedback(),
         feedback_resp  = false                                         :: boolean(),
         send_stats     = nil                                           :: nil | comm:mypid() 
         }).
-type state() :: #ru_resolve_state{}.

-type message() ::
    {get_state_response, intervals:interval()} |
    {update_key_entry_ack, db_entry:entry(), Exists::boolean(), Done::boolean()} |
    {shutdown, {atom(), stats()}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

on({get_state_response, MyI}, State = 
       #ru_resolve_state{ operation = {key_upd, KvvList},
                          dhtNodePid = DhtNodePid,
                          stats = Stats
                          }) ->
    ?TRACE("START GET INTERVAL - KEY UPD - MYI=~p;KVVListLen=~p", [MyI, length(KvvList)]),
    MyPid = comm:this(),
    ToUpdate = lists:foldl(
                 fun({Key, Value, Vers}, Acc) ->
                         UpdKeys = [X || X <- ?RT:get_replica_keys(Key), 
                                         intervals:in(X, MyI)],
                         lists:foreach(fun(UpdKey) ->
                                               comm:send_local(DhtNodePid, 
                                                               {update_key_entry, MyPid, UpdKey, Value, Vers})
                                       end, UpdKeys),
                         Acc + length(UpdKeys)
                 end, 0, KvvList),
    %kill is done by update_key_entry_ack
    ?TRACE("DetailSync START ToDo=~p", [ToUpdate]),
    ToUpdate =:= 0 andalso
        comm:send_local(self(), {shutdown, {resolve_ok, Stats}}),
    State#ru_resolve_state{ stats = Stats#resolve_stats{ diff_size = ToUpdate } };

on({get_state_response, MyI}, State =
       #ru_resolve_state{ operation = {key_sync, _, KeyList},
                          dhtNodePid = DhtNodePid }) ->    
    FilterKeyList = [K || X <- KeyList, 
                          K <- ?RT:get_replica_keys(X), 
                          intervals:in(K, MyI)],
    comm:send_local(DhtNodePid, 
                    {get_entries, self(), intervals:from_elements(FilterKeyList)}),
    State;

on({get_entries_response, Entries}, State =
       #ru_resolve_state{ operation = {key_sync, Dest, _},
                          ownerRemotePid = MyNodePid,
                          stats = Stats }) ->
    ?TRACE("START GET ENTRIES - KEY SYNC", []),
    KVVList = [{db_entry:get_key(X), 
                db_entry:get_value(X), 
                db_entry:get_version(X)} || X <- Entries],
    comm:send(Dest, {request_resolve, 
                     Stats#resolve_stats.round, 
                     {key_upd, KVVList}, 
                     [{feedback, MyNodePid}]}),
    comm:send_local(self(), {shutdown, {resolve_ok, Stats}}),
    State;

on({update_key_entry_ack, Entry, Exists, Done}, State =
       #ru_resolve_state{ operation = {key_upd, _},
                          stats = #resolve_stats{ diff_size = Diff,
                                                  regen_count = RegenOk,
                                                  update_count = UpdOk, 
                                                  upd_fail_count = UpdFail,
                                                  regen_fail_count = RegenFail,
                                                  round = Round
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
                not Done 
                    andalso Exists 
                    andalso DoFB =/= nil -> 
                    {DoFB,
                     [{db_entry:get_key(Entry),
                       db_entry:get_value(Entry),
                       db_entry:get_version(Entry)} | FBItems]};
                true -> FB
            end,
    if
        (Diff -1) =:= (RegenOk + UpdOk + UpdFail + RegenFail) ->
                send_feedback(NewFB, Round),
                comm:send_local(self(), {shutdown, {resolve_ok, NewStats}});
        true -> ok
    end,
    State#ru_resolve_state{ stats = NewStats, feedback = NewFB };

on({shutdown, _}, #ru_resolve_state{ ownerLocalPid = Owner,   
                                     send_stats = SendStats,                                 
                                     stats = Stats } = State) ->
    NStats = build_comment(State, Stats),
    send_stats(SendStats, NStats),
    comm:send_local(Owner, {resolve_progress_report, self(), NStats}),    
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

build_comment(#ru_resolve_state{ operation = Operation,
                                 feedback = {FBDest, _},
                                 feedback_resp = Resp 
                               }, Stats) ->
    Comment = case Operation of 
                  {key_upd, _} when Resp ->
                      "key_upd by feedback";
                  {key_upd, _} when not Resp andalso FBDest =:= nil ->
                      "key_upd without feedback"; 
                  {key_upd, _} when not Resp andalso FBDest =/= nil -> 
                      ["key_upd with feedback to ", FBDest];
                  {key_sync, Dest, _} -> 
                      ["key_sync with", Dest]
              end,
    Stats#resolve_stats{ comment = Comment }.

-spec send_feedback(feedback(), rep_upd:round()) -> ok.
send_feedback({nil, _}, _) -> ok;
send_feedback({_, []}, _) -> ok;
send_feedback({Dest, Items}, Round) ->
    comm:send(Dest, {request_resolve, Round, {key_upd, Items}, [feedback_response]}).

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
    [erlang:element(1, Stats), lists:flatten(lists:reverse(Res))].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% STARTUP
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc init module
-spec init(state()) -> state().
init(State) ->
    comm:send_local(State#ru_resolve_state.dhtNodePid, {get_state, comm:this(), my_range}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start(Round, Operation, Options) -> {ok, MyPid} when
      is_subtype(Round,     rep_upd:round()),                                                        
      is_subtype(Operation, operation()),
      is_subtype(Options,   options()),
      is_subtype(MyPid,     pid()).
start(Round, Operation, Options) ->        
    FBDest = proplists:get_value(feedback, Options, nil),
    FBResp = proplists:get_value(feedback_response, Options, false),
    StatsDest = proplists:get_value(send_stats, Options, nil),
    State = #ru_resolve_state{ ownerLocalPid = self(), 
                               ownerRemotePid = comm:this(), 
                               dhtNodePid = pid_groups:get_my(dht_node),
                               operation = Operation,
                               stats = #resolve_stats{ round = Round },
                               feedback = {FBDest, []},
                               feedback_resp = FBResp,
                               send_stats = StatsDest },    
    gen_component:start(?MODULE, fun ?MODULE:on/2, State, []).
