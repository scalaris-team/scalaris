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
%%      it should have a mechanism to recover to an earlier stage of the job
%% @end
%% @version $Id$
-module(mr_master).
-author('fajerski@zib.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), io:format(X, Y)).

-behaviour(gen_component).

-export([
        start_link/2
        , on/2
        , init/1
        ]).

-include("scalaris.hrl").

-type state() :: {JobID::nonempty_string(),
                  AckedInterval::intervals:interval()}.

-type(message() :: {mr, phase_completed, intervals:interval()} |
                   {mr, job_completed, intervals:interval()}).

-spec init({nonempty_string(), comm:mypid(), mr:job_description()}) -> state().
init({JobId, Client, Job}) ->
    Data = filter_data(api_tx:get_system_snapshot(),
                          element(2, Job)),
    ?TRACE("mr_master: job ~p started~n", [JobId]),
    bulkowner:issue_bulk_distribute(uid:get_global_uid(),
                                    dht_node, 7, {mr, job, JobId, comm:this(),
                                                  Client, Job, '_'},
                                    Data),
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
            ?TRACE("mr_master_~s: phase completed...initiating next phase~n",
                     [JobId]),
            bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                                       {mr, next_phase, JobId}),
            {JobId, []}
    end;

on({mr, job_completed, Range}, {JobId, I}) ->
    NewInterval = intervals:union(I, Range),
    case intervals:is_all(NewInterval) of
        false ->
            {JobId, NewInterval};
        _ ->
            ?TRACE("mr_master_~s: job completed...shutting down~n",
                     [JobId]),
            bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                                       {mr, terminate_job, JobId}),
            exit(self(), shutdown),
            {JobId, []}
    end;

on(Msg, State) ->
    ?TRACE("~p mr_master: revceived ~p~n",
           [comm:this(), Msg]),
    State.

-spec filter_data(mr_state:data() | [{atom(), string(), term()}], [mr:option()]) ->
    mr_state:data().
filter_data(Data, Options) ->
    case lists:keyfind(tag, 1, Options) of
        {tag, FilterTag} ->
            ?TRACE("Filtering! tag is ~p~n", [FilterTag]),
            [{K, V} || {_HashedKey, {Tag, K, V}, _Version} <- Data, Tag =:= FilterTag];
        false ->
            [{K, V} || {_HashedKey, {K, V}, _Version} <- Data]
    end.
