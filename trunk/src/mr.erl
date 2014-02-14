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

on({bulk_distribute, _Id, _Interval,
    {mr, job, JobId, Master, Client, Job, InitalData}, _Parents}, State) ->
    ?TRACE("mr_~s on ~p: received job with initial data: ~p...~n",
           [JobId, self(), InitalData]),
    %% @doc
    %% this message starts the worker supervisor and adds a job specific state
    %% to the dht node
    JobState = mr_state:new(JobId, Client, Master, InitalData, Job),
    Range = lists:foldl(fun({I, _SlideOp}, AccIn) -> intervals:union(I, AccIn) end,
                        dht_node_state:get(State, my_range),
                        dht_node_state:get(State, db_range)),
    %% send acc to master
    comm:send(Master, {mr, phase_completed, Range}),
    dht_node_state:set_mr_state(State, JobId, JobState);

on({mr, phase_result, JobId, {work_done, Data}, Range}, State) ->
    ?TRACE("mr_~s on ~p: received phase results: ~p...~ndistributing...~n",
           [JobId, self(), ets:tab2list(Data)]),
    MRState = dht_node_state:get_mr_state(State, JobId),
    NewMRState = case mr_state:is_last_phase(MRState) of
        false ->
            MyRange = lists:foldl(fun({I, _SlideOp}, AccIn) -> intervals:union(I, AccIn) end,
                                dht_node_state:get(State, my_range),
                                dht_node_state:get(State, db_range)),
            %% TODO partition data in Keep and Send
            Ref = uid:get_global_uid(),
            Reply = comm:reply_as(comm:this(), 4, {mr, next_phase_data_ack,
                                                   {JobId, Ref, Range}, '_'}),
            bulkowner:issue_bulk_distribute(Ref, dht_node,
                                            5, {mr, next_phase_data, JobId, Reply, '_'},
                                            {ets, Data}),
            mr_state:reset_acked(MRState, Ref);
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

on({mr, phase_result, JobId, {worker_died, Reason}, Range}, State) ->
    ?TRACE("runtime error in phase...terminating job~n", []),
    MRState = dht_node_state:get_mr_state(State, JobId),
    Master = mr_state:get(MRState, master),
    comm:send(Master, {mr, job_error, Range}),
    Client = mr_state:get(MRState, client),
    comm:send(Client, {mr_results, {error, Reason}, Range, JobId}),
    State;

on({bulk_distribute, _Id, Interval,
   {mr, next_phase_data, JobId, Source, Data}, _Parents}, State) ->
    ?TRACE("mr_~s on ~p: received next phase data: ~p~n",
           [JobId, self(), Data]),
    NewMRState = mr_state:add_data_to_next_phase(dht_node_state:get_mr_state(State,
                                                                            JobId),
                                                 Data),
    %% send ack with delivery interval
    comm:send(Source, Interval),
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase_data_ack, {JobId, Ref, Range}, Interval}, State) ->
    NewMRState = mr_state:set_acked(dht_node_state:get_mr_state(State, JobId),
                                    {Ref, Interval}),
    case mr_state:is_acked_complete(NewMRState) of
        true ->
            Master = mr_state:get(NewMRState, master),
            comm:send(Master, {mr, phase_completed, Range}),
            ?TRACE("Phase complete...~p informing master~n", [self()]);
        false ->
            ?TRACE("~p is still waiting for phase to complete~n", [self()])
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, next_phase, JobId}, State) ->
    ?TRACE("master initiated next phase ~p ~p~n",
              [JobId, self()]),
    MrState = mr_state:next_phase(dht_node_state:get_mr_state(State, JobId)),
    Range = lists:foldl(fun({I, _SlideOp}, AccIn) -> intervals:union(I, AccIn) end,
                        dht_node_state:get(State, my_range),
                        dht_node_state:get(State, db_range)),
    work_on_phase(JobId, MrState, Range),
    dht_node_state:set_mr_state(State, JobId, MrState);

on({mr, terminate_job, JobId}, State) ->
    MRState = dht_node_state:get_mr_state(State, JobId),
    _ = mr_state:clean_up(MRState),
    dht_node_state:delete_mr_state(State, JobId);

on(Msg, State) ->
    ?TRACE("~p mr: unknown message ~p~n", [comm:this(), Msg]),
    State.

-spec work_on_phase(mr_state:jobid(), mr_state:state(), intervals:interval()) -> ok.
work_on_phase(JobId, MRState, MyRange) ->
    {Round, MoR, FunTerm, ETS} = mr_state:get_phase(MRState),
    TmpETS = mr_state:get(MRState, phase_res),
    ets:delete_all_objects(TmpETS),
    case ets:info(ETS, size) of
        0 ->
            ?TRACE("mr_~p on ~p: no data for this phase...phase complete ~p~n",
                   [JobId, self(), Round]),
            case mr_state:is_last_phase(MRState) of
                false ->
                    %% io:format("no data for phase...done...~p informs master~n", [self()]),
                    Master = mr_state:get(MRState, master),
                    comm:send(Master, {mr, phase_completed, MyRange});
                _ ->
                    %% io:format("last phase and no data ~p~n", [Round]),
                    Master = mr_state:get(MRState, master),
                    comm:send(Master, {mr, job_completed, MyRange}),
                    Client = mr_state:get(MRState, client),
                    comm:send(Client, {mr_results, [], MyRange, JobId})
            end;
        _Load ->
            ?TRACE("mr_~p on ~p: starting to work on phase ~p~n", [JobId,
                                                                   self(), Round]),
            Reply = comm:reply_as(comm:this(), 4, {mr, phase_result, JobId, '_',
                                                   MyRange}),
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
