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
%% @doc Map Reduce functions
%%      this is part of the dht node
%%
%% @end
%% @version $Id$
-module(mr).
-author('fajerski@informatik.hu-berlin.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), io:format(X, Y)).
%% -define(TRACE(X, Y), ok).

-export([
        on/2
        ]).

-ifdef(with_export_type_support).
-export_type([job_description/0, option/0]).
-endif.

-include("scalaris.hrl").

-type(phase_desc() :: {map | reduce,
                     mr_state:fun_term()}).

-type(option() :: {tag, atom()}).

-type(job_description() :: {[phase_desc(),...], [option()]}).

-type(bulk_message() :: {mr, job, mr_state:jobid(), comm:mypid(), comm:mypid(),
                         job_description(), mr_state:data()} |
                        {mr, next_phase_data, mr_state:jobid(), comm:mypid(),
                         mr_state:data()}).

-type(message() :: {mr, init, Client::comm:mypid(), mr_state:jobid(),
                    JobSpec::job_description()} |
                   {bulk_distribute, uid:global_uid(), intervals:interval(),
                    bulk_message(), Parents::[comm:mypid(),...]} |
                   {mr, phase_results, mr_state:jobid(), comm:message(),
                    intervals:interval()} |
                   {mr, next_phase_data_ack, {mr_state:jobid(), reference(),
                                              intervals:interval()},
                    intervals:interval()} |
                   {mr, next_phase, mr_state:jobid()} |
                   {mr, terminate_job, mr_state:jobid()}).

-spec on(message(), dht_node_state:state()) -> dht_node_state:state().
on({mr, init, Client, JobId, Job}, State) ->
    %% this is the inital message
    %% it creates a JobID and starts the master process,
    %% which in turn starts the worker supervisor on all nodes.
    ?TRACE("mr: ~p~n received init message from ~p~n starting job ~p~n",
           [comm:this(), Client, Job]),
    case validate_job(Job) of
        ok ->
            mr_master:init_job(State, JobId, Job, Client);
        {error, Reason} ->
            comm:send(Client, {mr_results, {error, Reason}, intervals:all(),
                               JobId}),
            State
    end;

on({bulk_distribute, _Id, Interval,
    {mr, job, JobId, MasterId, Client, Job, InitalData}, _Parents}, State) ->
    %% this message starts the worker supervisor and adds a job specific state
    %% to the dht node
    ?TRACE("mr_~s on ~p: received job with initial data: ~p...~n",
           [JobId, self(), InitalData]),
    JobState = mr_state:new(JobId, Client, MasterId, InitalData, Job, Interval),
    %% send acc to master
    api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId, phase_completed,
                                             0, Interval}),
    dht_node_state:set_mr_state(State, JobId, JobState);

on({mr, phase_result, JobId, {work_done, Data}, Range, Round}, State) ->
    %% processing of phase results from worker.
    %% distribute data and start sync phase
    ?TRACE("mr_~s on ~p: received phase results (round ~p): ~p...~ndistributing...~n",
           [JobId, self(), Round, ets:tab2list(Data)]),
    MRState = dht_node_state:get_mr_state(State, JobId),
    NewMRState = mr_state:interval_processed(MRState, Range, Round),
    case mr_state:is_last_phase(MRState, Round) of
        false ->
            Ref = uid:get_global_uid(),
            bulkowner:issue_bulk_distribute(Ref, dht_node,
                                            5, {mr, next_phase_data, JobId,
                                                Range, '_', Round},
                                            {ets, Data});
        _ ->
            ?TRACE("jobs last phase done...sending to client~n", []),
            MasterId = mr_state:get(MRState, master_id),
            api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId,
                                                     job_completed,
                                                     Range}),
            Client = mr_state:get(MRState, client),
            comm:send(Client, {mr_results,
                               ets:select(Data, [{{'_','$1','$2'}, [], [{{'$1', '$2'}}]}]),
                               %% ets:tab2list(Data),
                               Range, JobId})
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, phase_result, JobId, {worker_died, Reason}, Range, Round}, State) ->
    %% processing of a failed worker result.
    %% for now abort the job
    ?TRACE("runtime error in phase ~p...terminating job~n", [Round]),
    MRState = dht_node_state:get_mr_state(State, JobId),
    MasterId = mr_state:get(MRState, master_id),
    api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId, job_error, Range}),
    Client = mr_state:get(MRState, client),
    comm:send(Client, {mr_results, {error, Reason}, Range, JobId}),
    State;

on({bulk_distribute, _Id, Interval,
   {mr, next_phase_data, JobId, AckRange, Data, Round}, _Parents}, State) ->
    %% processing of data for next phase.
    %% save data and send ack
    ?TRACE("mr_~s on ~p: received next phase data (round ~p) interval ~p: ~p~n",
           [JobId, self(), Round, Interval, Data]),
    NewMRState = mr_state:add_data_to_phase(dht_node_state:get_mr_state(State,
                                                                            JobId),
                                                 Data, Interval, Round + 1),
    %% send ack with delivery interval
    bulkowner:issue_bulk_owner(uid:get_global_uid(), AckRange, {mr,
                                                                next_phase_data_ack,
                                                               Interval, JobId,
                                                               Round}),
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase_data_ack, AckInterval, JobId, Round, DeliveryInterval}, State) ->
    %% ack from other mr nodes.
    %% check if the whole interval waas acked. If so inform master, wait
    %% otherwise.
    NewMRState = mr_state:set_acked(dht_node_state:get_mr_state(State, JobId),
                                    AckInterval),
    case mr_state:is_acked_complete(NewMRState) of
        true ->
            MasterId = mr_state:get(NewMRState, master_id),
            api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId,
                                                     phase_completed, Round, DeliveryInterval}),
            ?TRACE("Phase ~p complete...~p informing master at ~p~n", [Round, self(),
                                                                    MasterId]);
        false ->
            ?TRACE("~p is still waiting for phase ~p to complete~n", [self(),
                                                                      Round])
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase, JobId, Round, _DeliveryInterval}, State) ->
    %% master started next round.
    ?TRACE("master initiated phase ~p in ~p ~p~n",
              [Round, JobId, self()]),
    work_on_phase(JobId, State, Round),
    State;

on({mr, terminate_job, JobId, _DeliveryInterval}, State) ->
    %% master wants to terminate job.
    MRState = dht_node_state:get_mr_state(State, JobId),
    _ = mr_state:clean_up(MRState),
    dht_node_state:delete_mr_state(State, JobId);

on(Msg, State) ->
    ?TRACE("~p mr: unknown message ~p~n", [comm:this(), Msg]),
    State.

-spec work_on_phase(mr_state:jobid(), dht_node_state:state(), pos_integer()) -> ok.
work_on_phase(JobId, State, Round) ->
    MRState = mr_state:reset_acked(dht_node_state:get_mr_state(State, JobId)),
    {Round, MoR, FunTerm, ETS, Open, Done} = mr_state:get_phase(MRState, Round),
    TmpETS = mr_state:get(MRState, phase_res),
    ets:delete_all_objects(TmpETS),
    case db_ets:get_load(ETS) of
        0 ->
            ?TRACE("mr_~s on ~p: no data for this phase...phase complete ~p~n",
                   [JobId, self(), Round]),
            %% if there is no data, ack with Done U Open
            %% TODO This probably should be done in the state as well...
            AckInterval = intervals:union(Open, Done),
            case mr_state:is_last_phase(MRState, Round) of
                false ->
                    %% io:format("no data for phase...done...~p informs master~n", [self()]),
                    MasterId = mr_state:get(MRState, master_id),
                    api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId,
                                                             phase_completed,
                                                             Round, AckInterval});
                _ ->
                    %% io:format("last phase and no data ~p~n", [Round]),
                    MasterId = mr_state:get(MRState, master_id),
                    api_dht_raw:unreliable_lookup(MasterId, {mr_master, JobId, job_completed, AckInterval}),
                    Client = mr_state:get(MRState, client),
                    comm:send(Client, {mr_results, [], AckInterval, JobId})
            end;
        _Load ->
            ?TRACE("mr_~s on ~p: starting to work on phase ~p
                    sending work to ~p~n", [JobId, self(), Round,
                                            pid_groups:get_my(wpool)]),
            Reply = comm:reply_as(comm:this(), 4, {mr, phase_result, JobId, '_',
                                                   Open, Round}),
            comm:send_local(pid_groups:get_my(wpool),
                            {do_work, Reply, {Round, MoR, FunTerm, ETS, Open, TmpETS}})
    end.

-spec validate_job(job_description) -> ok | {error, term()}.
validate_job({Phases, _Options}) ->
    validate_phases(Phases).

-spec validate_phases([phase_desc()]) -> ok | {error, term()}.
validate_phases([]) -> ok;
validate_phases([H | T]) ->
    case validate_phase(H) of
        ok ->
            validate_phases(T);
        Error ->
            Error
    end.

-spec validate_phase(phase_desc()) -> ok | {error, term()}.
validate_phase(Phase) ->
    El1 = element(1, Phase), {FunTag, Fun} = element(2, Phase),
    case El1 == map orelse El1 == reduce of
        true ->
            case FunTag of
                erlanon ->
                    case is_function(Fun) of
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
