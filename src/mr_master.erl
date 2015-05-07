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
%% @end
%%      So far it is the single point of failure. It should be replicated and
%%      it should have a mechanism to recover to an earlier stage of the job
%% @version $Id$
-module(mr_master).
-author('fajerski@zib.de').
-vsn('$Id$ ').

%% -define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE(X, Y), ok).

-export([
         init_job/4,
         on/2,
         dispatch_snapshot/1
        ]).

-export_type([message/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-type(message() :: {mr_master, init, Client::comm:mypid(), mr_state:jobid(),
                    JobSpec::mr_state:job_description()} |
                   {mr_master, mr_state:jobid(), snapshot,
                    snapshot_leader:result_message(),
                    mr_state:job_description(), comm:mypid()} |
                   {mr_master, mr_state:jobid(), phase_completed, Round::non_neg_integer(), Range::intervals:interval()} |
                   {mr_master, mr_state:jobid(), job_completed, intervals:interval()} |
                   {mr_master, mr_state:jobid(), job_error, intervals:interval()}).

-spec init_job(dht_node_state:state(), nonempty_string(), mr_state:job_description(),
              comm:mypid()) ->
    dht_node_state:state().
init_job(State, JobId, Job, Client) ->
    Id = dht_node_state:get(State, node_id),
    MasterState = mr_master_state:new(Id, Job, Client),
    dispatch_snapshot(JobId),
    dht_node_state:set_mr_master_state(State, JobId, MasterState).

-spec dispatch_snapshot(mr_state:jobid()) -> ok.
dispatch_snapshot(JobId) ->
    Reply = comm:reply_as(comm:this(), 4, {mr_master, JobId, snapshot, '_'}),
    comm:send_local(pid_groups:find_a(snapshot_leader), {init_snapshot,
                                                  Reply}).

-spec on(message(), dht_node_state:state()) -> dht_node_state:state().
on({mr_master, init, Client, JobId, Job}, State) ->
    %% this is the inital message
    %% it creates a JobId and starts the master process,
    %% which in turn starts the worker supervisor on all nodes.
    ?TRACE("mr_master: ~p~n received init message from ~p~n starting job ~p~n",
           [comm:this(), Client, Job]),
    case validate_job(Job) of
        ok ->
            init_job(State, JobId, Job, Client);
        {error, Reason} ->
            comm:send(Client, {mr_results, {error, Reason}, intervals:all(),
                               JobId}),
            State
    end;

on({mr_master, JobId, snapshot, {global_snapshot_done, Data}}, State) ->
    MasterState = dht_node_state:get_mr_master_state(State, JobId),
    Job = mr_master_state:get(job, MasterState),
    FilteredData = case lists:keyfind(tag, 1, element(2, Job)) of
                       false ->
                           filter_data(Data);
                       Tag ->
                           filter_data(Data, Tag)
                   end,
    ?TRACE("mr_master: starting job ~p~n", [JobId]),
    bulkowner:issue_bulk_distribute(uid:get_global_uid(),
                                    dht_node, 7,
                                    {mr, job, JobId,
                                     mr_master_state:get(id, MasterState),
                                     mr_master_state:get(client, MasterState),
                                     Job,
                                     '_'},
                                    FilteredData),
    dht_node_state:set_mr_master_state(State,
                                       JobId,
                                       mr_master_state:set(MasterState,
                                                           [{outstanding, false}]));

%% in case the snapshot failed, try again
on({mr_master, JobId, snapshot, {global_snapshot_done_with_errors,
                                 _ErrorInterval, _PartData}}, State) ->
    dispatch_snapshot(JobId),
    State;

on({mr_master, JobId, snapshot, {worker_died, Reason}}, State) ->
    log:log(error, "mr_master_~s: snapshot failed ~p", [JobId, Reason]),
    dht_node_state:delete_mr_master_state(State, JobId);

on({mr_master, JobId, phase_completed, Round, Range}, State) ->
    MasterState = dht_node_state:get_mr_master_state(State, JobId),
    I = mr_master_state:get(acked, MasterState),
    NewInterval = intervals:union(I, Range),
    NewMasterState = case intervals:is_all(NewInterval) of
        false ->
            ?TRACE("~p mr_master_~s: phase ~p not yet completed...~p~n",
                     [self(), JobId, Round, NewInterval]),
            mr_master_state:set(MasterState, [{acked, NewInterval}]);
        _ ->
            ?TRACE("mr_master_~s: phase ~p completed...initiating next phase~n",
                     [JobId, Round]),
            bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                                       {mr, next_phase, JobId, Round + 1}),
            mr_master_state:set(MasterState, [{acked, intervals:empty()},
                                        {round, Round + 1}])
    end,
    dht_node_state:set_mr_master_state(State, JobId, NewMasterState);

on({mr_master, JobId, job_completed, Range}, State) ->
    MasterState = dht_node_state:get_mr_master_state(State, JobId),
    I = mr_master_state:get(acked, MasterState),
    NewInterval = intervals:union(I, Range),
    case intervals:is_all(NewInterval) of
        false ->
            dht_node_state:set_mr_master_state(State,
                                               JobId,
                                               mr_master_state:set(MasterState,
                                                                   [{acked,
                                                                     NewInterval}]));
        _ ->
            ?TRACE("~p mr_master_~s: job completed...shutting down~n",
                     [self(), JobId]),
            bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                                       {mr, terminate_job, JobId}),
            dht_node_state:delete_mr_master_state(State, JobId)
    end;

on({mr_master, JobId, job_error, _Range}, State) ->
    ?TRACE("~p mr_master_~s: job crashed...shutting down~n",
             [self(), JobId]),
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(),
                               {mr, terminate_job, JobId}),
    dht_node_state:delete_mr_master_state(State, JobId);

on(_Msg, State) ->
    ?TRACE("~p mr_master: revceived ~p~n",
           [comm:this(), Msg]),
    State.

-spec filter_data([{?RT:key(),
                   {{string(), term()} | {atom(), string(), term()}},
                   client_version()}],
                  {tag, atom()}) -> mr_state:data_list().
filter_data(Data, {tag, FilterTag}) ->
    ?TRACE("Filtering! tag is ~p~ndata: ~p~n", [FilterTag, Data]),
    [{?RT:hash_key(K), K, V} || {_HashedKey, {Tag, K, V}, _Version} <- Data, Tag
                                =:= FilterTag].

-spec filter_data([{?RT:key(),
                   {{string(), term()} | {atom(), string(), term()}},
                   client_version()}]) -> mr_state:data_list().
filter_data(Data) ->
    [{?RT:hash_key(K), K, V} || {_HashedKey, {K, V}, _Version} <- Data].

-spec validate_job(mr_state:job_description()) -> ok | {error, term()}.
validate_job({Phases, _Options}) ->
    validate_phases(Phases).

-spec validate_phases([mr_state:fun_term()]) -> ok | {error, term()}.
validate_phases([]) -> ok;
validate_phases([H | T]) ->
    case validate_phase(H) of
        ok ->
            validate_phases(T);
        Error ->
            Error
    end.

-spec validate_phase(mr_state:fun_term()) -> ok | {error, term()}.
validate_phase(Phase) ->
    MoR = element(1, Phase),
    FunTag = element(2, Phase),
    Fun = element(3, Phase),
    case MoR == map orelse MoR == reduce of
        true ->
            case FunTag of
                erlanon ->
                    case is_function(Fun, 1) of
                        true ->
                            ok;
                        false ->
                            {error ,{badfun, "Fun should be a fun"}}
                    end;
                jsanon ->
                    case is_binary(Fun) of
                        true ->
                            ok;
                        false ->
                            {error ,{badfun, "Fun should be a binary"}}
                    end;
                Tag ->
                    {error, {bad_tag, {Tag, Fun}}}
            end;
        false ->
            {error, {bad_phase, "phase must be either map or reduce"}}
    end.
