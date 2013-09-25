%% @copyright 2007-2013 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@informatik.hu-berlin.de>
%% @doc Map reduce master file
%%      The master is in charge of orchestrating the single phases of the job.
%%      So far it is the single point of failiure. It should be replicated and
%%      it hsould have a mechanism to recover to an earlier stage of the job
%% @end
%% @version $Id$
-module(mr_master).
-author('fajerski@informatik.hu-berlin.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), io:format(X, Y)).

-behaviour(gen_component).

-export([
        start_link/2
        , on/2
        , init/1
        ]).

-include("scalaris.hrl").

-type state() :: nonempty_string(). % jobid

-type(message() ::
      any()).

-spec init({nonempty_string(), comm:mypid(), mr_state:state()}) -> state().
init({JobId, Client, Job}) ->
    Data = api_tx:get_system_snapshot(),
    ?TRACE("mr_master: job ~p started~n", [JobId]),
    %% TODO do we need an ack?
    bulkowner:issue_bulk_distribute(uid:get_global_uid(),
                                    dht_node, 7, {mr, job, JobId, comm:this(),
                                                  Client, Job, '_'},
                                   [{X, Y} || {_Hashed, {X, Y}, _Version} <-
                                              Data]),
    {JobId, []}.

-spec start_link(pid_groups:groupname(), tuple()) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    ?TRACE("mr_master: running on node ~p~n", [DHTNodeGroup]),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{pid_groups_join_as, DHTNodeGroup, "mr_master_" ++
                              element(1, Options)}]).

-spec on(message(), state()) -> state().

on({mr, phase_completed, Range}, {JobId, I}) ->
    NewInterval = intervals:union(I, Range),
    case intervals:is_all(NewInterval) of
        false ->
            {JobId, NewInterval};
        _ ->
            io:format("mr_master_~s: phase completed...initiating next phase~n",
                     [JobId]),
            bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                                       {mr, next_phase, JobId}),
            {JobId, []}
    end;

on(Msg, State) ->
    ?TRACE("~p mr_master: revceived ~p~n",
           [comm:this(), Msg]),
    State.

%% on({read_finished, Range}, State) ->
%%     OldRange = state_get(completedrange, State),
%%     NewRange = intervals:union(OldRange, Range),
%%     case intervals:is_all(NewRange) of
%%         true ->
%%             JobId = state_get(jobid, State),
%%             bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
%%                                        {?send_to_group_member, "mr",
%%                                         {mr_send_to_worker, "mr_reader_" ++
%%                                          JobId, {check_consistency}}}),
%%             state_set({completedrange, intervals:empty()}, State);
%%         _ ->
%%             state_set({completedrange, NewRange}, State)
%%     end;
%% on({worker_finished, Range}, State) ->
%%     OldRange = state_get(completedrange, State),
%%     NewRange = intervals:union(OldRange, Range),
%%     case intervals:is_all(NewRange) of
%%         true ->
%%             io:format("~s: Phase ~p of job ~s completed~n",
%%                       [pid_groups:my_pidname(), element(1,
%%                                                         hd(state_get(currentphase,
%%                                                                   State))),
%%                        state_get(jobid, State)]),
%%             start_next_phase(State);
%%         _ ->
%%             state_set({completedrange, NewRange}, State)
%%     end;
%% 
%% on({mr_results, Results, Range}, State) ->
%%     OldRange = state_get(completedrange, State),
%%     NewRange = intervals:union(OldRange, Range),
%%     NewState = state_append({results, Results}, State),
%%     case intervals:is_all(NewRange) of
%%         true ->
%%             io:format("~s: Phase ~p of job ~s completed~n",
%%                       [pid_groups:my_pidname(), element(1,
%%                                                         hd(state_get(currentphase,
%%                                                                   NewState))),
%%                        state_get(jobid, NewState)]),
%%             finish_job(NewState);
%%         _ ->
%%             state_set({completedrange, NewRange}, NewState)
%%     end;
%% 
%% % start the next phase found in openphases
%% % assumes openphases is not empty
%% start_next_phase(State) ->
%%     {NextPhase, NewState} = get_next_phase(State),
%%     JobId = state_get(jobid, NewState),
%%     Phase = case element(1, NextPhase) of
%%         last ->
%%             Completed = state_get(completedphases, NewState),
%%             integer_to_list(length(Completed));
%%         I ->
%%             integer_to_list(I)
%%     end,
%%     if
%%         Phase == "0" ->
%%             bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
%%                                        {?send_to_group_member, "mr",
%%                                         {mr_send_to_worker, "mr_reader_" ++
%%                                          JobId, {read_from_node, comm:this()}}});
%%         true ->
%%             bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
%%                                        {?send_to_group_member, "mr",
%%                                         {mr_send_to_worker, "mr_worker_" ++ JobId ++
%%                                          "_" ++ Phase, {setup_worker, comm:this(), NextPhase}}})
%%     end,
%%     NewState.
%% 
%% get_next_phase(State) ->
%%     % move current to completed
%%     Current = state_get(currentphase, State),
%%     State1 = state_rem(currentphase, State),
%%     State2 = state_append({completedphases, Current}, State1),
%%     % pop one phase off open and set it to current
%%     {NextPhase, RemainingOpenPhases} = lists:split(1, state_get(openphases,
%%                                                                 State2)),
%%     State3 = state_set([{currentphase, NextPhase}
%%                        , {completedrange, intervals:empty()}
%%                        , {openphases, RemainingOpenPhases}]
%%                        , State2),
%%     {hd(NextPhase), State3}.
%% 
%% finish_job(State) ->
%%     JobId = state_get(jobid, State),
%%     comm:send(state_get(client, State), {mr_results, JobId,
%%                                          state_get(results, State)}),
%%     bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
%%                                          {?send_to_group_member, "mr",
%%                                          {terminate_job, JobId}}),
%%     State.
