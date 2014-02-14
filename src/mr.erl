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
            JobDesc = {"mr_master_" ++ JobId,
                       {mr_master, start_link,
                        [pid_groups:my_groupname(), {JobId}]},
                       temporary, brutal_kill, worker, []},
            SupDHT = pid_groups:get_my(sup_dht_node),
            %% TODO handle failed starts
            {ok, Pid} = supervisor:start_child(SupDHT, JobDesc),
            comm:send_local(Pid, {start_job, Client, Job});
        {error, Reason} ->
            comm:send(Client, {mr_results, {error, Reason}, intervals:all(), JobId})
    end,
    State;

on({bulk_distribute, _Id, Interval,
    {mr, job, JobId, Master, Client, Job, InitalData}, _Parents}, State) ->
    ?TRACE("mr_~s on ~p: received job with initial data: ~p...~n",
           [JobId, self(), InitalData]),
    %% @doc
    %% this message starts the worker supervisor and adds a job specific state
    %% to the dht node
    JobState = mr_state:new(JobId, Client, Master, InitalData, Job, Interval),
    %% send acc to master
    comm:send(Master, {mr, phase_completed, Interval}),
    dht_node_state:set_mr_state(State, JobId, JobState);

on({mr, phase_result, JobId, {work_done, Data}, Range, Round}, State) ->
    ?TRACE("mr_~s on ~p: received phase results: ~p...~ndistributing...~n",
           [JobId, self(), ets:tab2list(Data)]),
    MRState = dht_node_state:get_mr_state(State, JobId),
    NewMRState = case mr_state:is_last_phase(MRState, Round) of
        false ->
            Ref = uid:get_global_uid(),
            bulkowner:issue_bulk_distribute(Ref, dht_node,
                                            5, {mr, next_phase_data, JobId,
                                                Range, '_', Round + 1},
                                            {ets, Data}),
            mr_state:reset_acked(MRState);
        _ ->
            ?TRACE("jobs last phase done...sending to client~n", []),
            Master = mr_state:get(MRState, master),
            comm:send(Master, {mr, job_completed, Range}),
            Client = mr_state:get(MRState, client),
            %% TODO send back only {K, V} without HK
            comm:send(Client, {mr_results,
                               ets:select(Data, [{{'_','$1','$2'}, [], [{{'$1', '$2'}}]}]),
                               %% ets:tab2list(Data),
                               Range, JobId}),
            MRState
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, phase_result, JobId, {worker_died, Reason, _Round}, Range}, State) ->
    ?TRACE("runtime error in phase...terminating job~n", []),
    MRState = dht_node_state:get_mr_state(State, JobId),
    Master = mr_state:get(MRState, master),
    comm:send(Master, {mr, job_error, Range}),
    Client = mr_state:get(MRState, client),
    comm:send(Client, {mr_results, {error, Reason}, Range, JobId}),
    State;

on({bulk_distribute, _Id, Interval,
   {mr, next_phase_data, JobId, AckRange, Data, Round}, _Parents}, State) ->
    ?TRACE("mr_~s on ~p: received next phase data for ~p: ~p~n",
           [JobId, self(), Interval, Data]),
    NewMRState = mr_state:add_data_to_phase(dht_node_state:get_mr_state(State,
                                                                            JobId),
                                                 Data, Interval, Round),
    %% send ack with delivery interval
    bulkowner:issue_bulk_owner(uid:get_global_uid(), AckRange, {mr,
                                                                next_phase_data_ack,
                                                               Interval, JobId}),
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase_data_ack, AckInterval, JobId, DeliveryInterval}, State) ->
    NewMRState = mr_state:set_acked(dht_node_state:get_mr_state(State, JobId),
                                    AckInterval),
    case mr_state:is_acked_complete(NewMRState) of
        true ->
            Master = mr_state:get(NewMRState, master),
            comm:send(Master, {mr, phase_completed, DeliveryInterval}),
            ?TRACE("Phase complete...~p informing master~n", [self()]);
        false ->
            ?TRACE("~p is still waiting for phase to complete~n", [self()])
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase, JobId, Round, _DeliveryInterval}, State) ->
    ?TRACE("master initiated phase ~p in ~p ~p~n",
              [Round, JobId, self()]),
    work_on_phase(JobId, State, Round),
    State;

on({mr, terminate_job, JobId, _DeliveryInterval}, State) ->
    MRState = dht_node_state:get_mr_state(State, JobId),
    _ = mr_state:clean_up(MRState),
    dht_node_state:delete_mr_state(State, JobId);

on(Msg, State) ->
    ?TRACE("~p mr: unknown message ~p~n", [comm:this(), Msg]),
    State.

-spec work_on_phase(mr_state:jobid(), dht_node_state:state(), pos_integer()) -> ok.
work_on_phase(JobId, State, Round) ->
    MRState = dht_node_state:get_mr_state(State, JobId),
    {Round, MoR, FunTerm, ETS, Interval} = mr_state:get_phase(MRState, Round),
    TmpETS = mr_state:get(MRState, phase_res),
    ets:delete_all_objects(TmpETS),
    case db_ets:get_load(ETS) of
        0 ->
            ?TRACE("mr_~s on ~p: no data for this phase...phase complete ~p~n",
                   [JobId, self(), Round]),
            case mr_state:is_last_phase(MRState, Round) of
                false ->
                    %% io:format("no data for phase...done...~p informs master~n", [self()]),
                    Master = mr_state:get(MRState, master),
                    comm:send(Master, {mr, phase_completed, Interval});
                _ ->
                    %% io:format("last phase and no data ~p~n", [Round]),
                    Master = mr_state:get(MRState, master),
                    comm:send(Master, {mr, job_completed, Interval}),
                    Client = mr_state:get(MRState, client),
                    comm:send(Client, {mr_results, [], Interval, JobId})
            end;
        _Load ->
            ?TRACE("mr_~s on ~p: starting to work on phase ~p~n", [JobId,
                                                                   self(), Round]),
            Reply = comm:reply_as(comm:this(), 4, {mr, phase_result, JobId, '_',
                                                   Interval, Round}),
            comm:send_local(pid_groups:get_my(wpool),
                            {do_work, Reply, {Round, MoR, FunTerm, ETS, TmpETS}})
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
