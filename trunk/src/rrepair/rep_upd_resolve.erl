% @copyright 2011 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    replica update resolution module 
%% @end
%% @version $Id$

-module(rep_upd_resolve).

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([init/1, on/2, start/5]).
-export([print_resolve_stats/1]).


-ifdef(with_export_type_support).
-export_type([ru_resolve_struct/0, ru_resolve_method/0, ru_resolve_answer/0]).
-endif.

-define(TRACE(X,Y), io:format("~w [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
%-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% type definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type ru_resolve_method() :: simple.

-type ru_resolve_struct() :: {simple_detail_sync, [rep_upd_sync:keyValVers()]}.

-type ru_resolve_answer() :: {true | false, Dest::comm:mypid()}.

-record(ru_resolve_stats,
        {
         diffCount       = 0  :: non_neg_integer(),
         updatedCount    = 0  :: non_neg_integer(),
         notUpdatedCount = 0  :: non_neg_integer(),
         errorCount      = 0  :: non_neg_integer()         
         }).
-type ru_resolve_stats() :: #ru_resolve_stats{}.

-record(ru_resolve_state,
        {
         ownerLocalPid  = ?required(ru_resolve_state, ownerLocalPid)    :: comm:erl_local_pid(),
         ownerRemotePid = ?required(ru_resolve_state, ownerRemotePid)   :: comm:mypid(),         
         dhtNodePid     = ?required(ru_resolve_state, dhtNodePid)       :: comm:erl_local_pid(),
         sync_round     = 0.0                                           :: float(),
         resolve_method = ?required(ru_resolve_state, resolve_method)   :: ru_resolve_method(),
         resolve_struct = ?required(ru_resolve_state, resolve_struct)   :: ru_resolve_struct(),
         stats          = #ru_resolve_stats{}                           :: ru_resolve_stats(),
         feedback       = ?required(ru_resolve_state, feedback)         :: ru_resolve_answer(), %if active sends data ids to given Dest which are outdated at Dest
         feedbackItems  = []                                            :: [rep_upd_sync:keyValVers()],
         send_stats     = ?required(ru_resolve_state, send_stats)       :: ru_resolve_answer()  %if active sends resolve results (stats) to given dest
         }).
-type state() :: #ru_resolve_state{}.

-type message() ::
    {get_state_response, intervals:interval()} |
    {update_key_entry_ack, db_entry:entry(), Exists::boolean(), Done::boolean()} |
    {shutdown, {atom(), ru_resolve_stats()}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

on({get_state_response, NodeDBInterval}, State = 
       #ru_resolve_state{ resolve_struct = {simple_detail_sync, DiffList},
                          dhtNodePid = DhtNodePid,
                          stats = Stats
                          }) ->
    MyPid = comm:this(),
    lists:foreach(fun({MinKey, Val, Vers}) ->
                          PosKeys = ?RT:get_replica_keys(MinKey),
                          UpdKeys = lists:filter(fun(X) -> 
                                                         intervals:in(X, NodeDBInterval)
                                                 end, PosKeys),
                          lists:foreach(fun(Key) ->
                                                comm:send_local(DhtNodePid, 
                                                                {update_key_entry, MyPid, Key, Val, Vers})
                                        end, UpdKeys)                  end,
                  DiffList),
    %kill is done by update_key_entry_ack
    ?TRACE("DetailSync START ToDo=~p", [length(DiffList)]),
    State#ru_resolve_state{ stats = Stats#ru_resolve_stats{ diffCount = length(DiffList) } };


on({update_key_entry_ack, Entry, Exists, Done}, State =
       #ru_resolve_state{ resolve_struct = {simple_detail_sync, _},
                          resolve_method = simple,
                          stats = #ru_resolve_stats{ diffCount = Diff,
                                                     updatedCount = Ok, 
                                                     notUpdatedCount = Failed                                                         
                                                   } = Stats,
                          feedback = {DoFeedback, FeedbackPid},
                          feedbackItems = FBItems,
                          send_stats = {DoSendResult, ResultDest},
                          sync_round = Round
                        }) ->
    NewStats = case Done of 
                   true  -> Stats#ru_resolve_stats{ updatedCount = Ok +1 };
                   false -> Stats#ru_resolve_stats{ notUpdatedCount = Failed + 1 } 
               end,
    NewFBItems = case not Done andalso Exists of
                   true -> [{db_entry:get_key(Entry),
                             db_entry:get_value(Entry),
                             db_entry:get_version(Entry)} | FBItems];
                   false -> FBItems
               end,
    _ = case Diff - 1 =:= Ok + Failed of
            true ->
                ?TRACE("DETAIL SYNC FINISHED Ok/Failed/Todo = ~p/~p/~p", 
                       [NewStats#ru_resolve_stats.updatedCount, 
                        NewStats#ru_resolve_stats.notUpdatedCount, 
                        Diff]),
                DoFeedback andalso
                        comm:send(FeedbackPid, {request_resolve, Round, simple, 
                                                    {simple_detail_sync, NewFBItems},
                                                    {false, self()}, {false, self()}}),
                DoSendResult andalso
                    comm:send(ResultDest, {resolve_progress_report, self(), Round, NewStats}),
                comm:send_local(self(), {shutdown, {simple_resolve_ok, NewStats}});
            _ ->
                ok
        end,
    State#ru_resolve_state{ stats = NewStats, feedbackItems = NewFBItems };

on({shutdown, _}, _) ->
    kill.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% HELPER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec print_resolve_stats(ru_resolve_stats()) -> [any()].
print_resolve_stats(Stats) ->
    FieldNames = record_info(fields, ru_resolve_stats),
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

-spec start(float(), ru_resolve_method(), ru_resolve_struct(), 
            ru_resolve_answer(), ru_resolve_answer()) -> {ok, pid()}.
start(Round, RMethod, RStruct, Feedback, SendStats) ->
    State = #ru_resolve_state{ ownerLocalPid = self(), 
                               ownerRemotePid = comm:this(), 
                               dhtNodePid = pid_groups:get_my(dht_node), 
                               sync_round = Round,
                               resolve_method = RMethod,
                               resolve_struct = RStruct,
                               feedback = Feedback,
                               send_stats = SendStats },
    gen_component:start(?MODULE, State, []).