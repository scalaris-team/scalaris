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

%% -define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE(X, Y), ok).

-export([
        on/2,
        work_on_phase/3,
        neighborhood_succ_crash_filter/3,
        neighborhood_succ_crash/5
        ]).

-include("scalaris.hrl").
-include("client_types.hrl").

-type(bulk_message() :: {mr, job, mr_state:jobid(), MasterId::?RT:key(),
                         Client::comm:mypid(),
                         mr_state:job_description(), mr_state:data()} |
                        {mr, next_phase_data, mr_state:jobid(), comm:mypid(),
                         mr_state:data()}).

-type(message() :: {bulk_distribute, uid:global_uid(), intervals:interval(),
                    bulk_message(), Parents::[comm:mypid(),...]} |
                   {mr, phase_results, mr_state:jobid(), comm:message(),
                    intervals:interval()} |
                   {mr, next_phase_data_ack, {mr_state:jobid(), reference(),
                                              intervals:interval()},
                    intervals:interval()} |
                   {mr, next_phase, mr_state:jobid()} |
                   {mr, terminate_job, mr_state:jobid()}).

-spec on(message(), dht_node_state:state()) -> dht_node_state:state().
on({bulk_distribute, _Id, Interval,
    {mr, job, JobId, MasterId, Client, Job, InitalData}, _Parents}, State) ->
    %% this message starts the worker supervisor and adds a job specific state
    %% to the dht node
    rm_loop:subscribe(self(), {"mr_succ_fd", JobId, MasterId, Client},
                      fun neighborhood_succ_crash_filter/3,
                      fun neighborhood_succ_crash/5,
                      inf),
    MRState = case dht_node_state:get_mr_state(State, JobId) of
        error ->
            ?TRACE("~p mr_~s: received job init for ~p~n~p~n",
                   [self(), JobId, Interval, InitalData]),
            mr_state:new(JobId, Client, MasterId, InitalData, Job,
                       Interval);
        {ok, ExState} ->
            ?TRACE("~p mr_~s: second init for ~p~n~p~n",
                   [self(), JobId, Interval, InitalData]),
            mr_state:add_data_to_phase(ExState, InitalData, Interval, 1)
    end,
    %% send acc to master
    send_to_master(MRState, {mr_master, JobId, phase_completed, 0, Interval}),
    dht_node_state:set_mr_state(State, JobId, MRState);

on({mr, phase_result, JobId, {work_done, Data}, Range, Round}, State) ->
    %% processing of phase results from worker.
    %% distribute data and start sync phase
    %% ?TRACE("~p mr_~s: received phase results (round ~p) for interval ~p:~n~p...~ndistributing...~n",
    %%        [self(), JobId, Round, Range, Data]),
    {ok, MRState} = dht_node_state:get_mr_state(State, JobId),
    NewMRState = mr_state:interval_processed(MRState, Range, Round),
    case mr_state:is_last_phase(MRState, Round) of
        false ->
            Ref = uid:get_global_uid(),
            bulkowner:issue_bulk_distribute(Ref, dht_node,
                                            5, {mr, next_phase_data, JobId,
                                                Range, '_', Round}, Data);
        _ ->
            ?TRACE("~p jobs last phase done...sending to client ~p~n~p~n",
                   [self(), Data, Range]),
            send_to_master(MRState, {mr_master, JobId, job_completed, Range}),
            send_to_client(MRState, {mr_results,
                               [{K, V} || {_HK, K, V} <- Data],
                               Range, JobId})
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, phase_result, JobId, {worker_died, Reason}, Range, _Round}, State) ->
    %% processing of a failed worker result.
    %% for now abort the job
    ?TRACE("runtime error in phase ~p...terminating job~n", [Round]),
    {ok, MRState} = dht_node_state:get_mr_state(State, JobId),
    send_to_master(MRState, {mr_master, JobId, job_error, Range}),
    send_to_client(MRState, {mr_results, {error, Reason}, Range, JobId}),
    State;

on({bulk_distribute, _Id, Interval,
   {mr, next_phase_data, JobId, AckRange, Data, Round}, _Parents}, State) ->
    %% processing of data for next phase.
    %% save data and send ack
    ?TRACE("~p mr_~s: received next phase data (round ~p) interval ~p: ~p~n",
           [self(), JobId, Round, Interval, Data]),
    {ok, MRState} = dht_node_state:get_mr_state(State, JobId),
    NewMRState = mr_state:add_data_to_phase(MRState, Data, Interval, Round + 1),
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
    {ok, MRState} = dht_node_state:get_mr_state(State, JobId),
    NewMRState = mr_state:set_acked(MRState,
                                    AckInterval),
    NewMRState2 = case mr_state:is_acked_complete(NewMRState) of
        true ->
            send_to_master(NewMRState, {mr_master, JobId,
                                      phase_completed, Round, DeliveryInterval}),
            ?TRACE("Phase ~p complete...~p informing master at ~p~n", [Round, self(),
                                                                    mr_state:get(NewMRState,
                                                                                master_id)]),
            mr_state:reset_acked(NewMRState);
        false ->
            ?TRACE("~p is still waiting for phase ~p to complete~n", [self(),
                                                                      Round]),
            NewMRState
    end,
    dht_node_state:set_mr_state(State, JobId, NewMRState2);

on({mr, next_phase, JobId, Round, _DeliveryInterval}, State) ->
    %% master started next round.
    ?TRACE("~p master initiated phase ~p in ~p~n~p",
              [self(), Round, JobId, DeliveryInterval]),
    {ok, MRState} = dht_node_state:get_mr_state(State, JobId),
    NewMRState = work_on_phase(JobId, MRState, Round),
    dht_node_state:set_mr_state(State, JobId, NewMRState);

on({mr, terminate_job, JobId, _DeliveryInterval}, State) ->
    %% master wants to terminate job.
        ?TRACE("~p got terminate message from master for job ~p~n",
               [self(), JobId]),
        case dht_node_state:get_mr_state(State, JobId) of
            {ok, MRState} ->
                Master = mr_state:get(MRState, master_id),
                Client = mr_state:get(MRState, client),
                rm_loop:unsubscribe(self(), {"mr_succ_fd", JobId, Master, Client}),
                _ = mr_state:clean_up(MRState),
                dht_node_state:delete_mr_state(State, JobId);
            error ->
                log:log(debug,
                        "Received terminate message for non-existing mr-job...ignoring!"),
                State
        end;

on(_Msg, State) ->
    ?TRACE("~p mr: unknown message ~p~n", [comm:this(), _Msg]),
    State.

-spec work_on_phase(mr_state:jobid(), mr_state:state(), pos_integer()) ->
    mr_state:state().
work_on_phase(JobId, State, Round) ->
    {Round, FunTerm, ETS, Open, _Working} = mr_state:get_phase(State, Round),
    case intervals:is_empty(Open) of
        false ->
            case db_ets:get_load(ETS) of
                0 ->
                    ?TRACE("~p mr_~s: no data for this phase...phase complete ~p~n",
                           [self(), JobId, Round]),
                    case mr_state:is_last_phase(State, Round) of
                        false ->
                            %% io:format("no data for phase...done...~p informs master~n", [self()]),
                            send_to_master(State,
                                           {mr_master, JobId, phase_completed, Round, Open}),
                            mr_state:interval_empty(State, Open, Round);
                        _ ->
                            %% io:format("last phase and no data ~p~n", [Round]),
                            send_to_master(State, {mr_master, JobId, job_completed, Open}),

                            send_to_client(State, {mr_results, [], Open, JobId}),
                            mr_state:interval_empty(State, Open, Round)
                    end;
                _Load ->
                    ?TRACE("~p mr_~s: starting to work on phase ~p
                        sending work (~p)~n~p
                               to ~p~n", [self(), JobId, Round, Open,
                                          ets:tab2list(ETS),
                                          pid_groups:get_my(wpool)]),
                    Reply = comm:reply_as(comm:this(), 4, {mr, phase_result, JobId, '_',
                                                           Open, Round}),
                    comm:send_local(pid_groups:get_my(wpool),
                                    {do_work, Reply, {FunTerm, ETS, Open}}),
                    mr_state:interval_processing(State, Open, Round)
                end;
        true ->
            %% nothing to do
            State
    end.

-spec neighborhood_succ_crash_filter(Old::nodelist:neighborhood(),
                                     New::nodelist:neighborhood(),
                                     rm_loop:reason()) -> boolean().
neighborhood_succ_crash_filter(Old, New, {node_crashed, _Pid}) ->
    %% only looking for succ changes
    nodelist:succ(Old) /= nodelist:succ(New);
neighborhood_succ_crash_filter(_Old, _New, _) -> false.

-spec neighborhood_succ_crash(comm:mypid(), {string(), mr_state:jobid(),
                                             ?RT:key(), comm:mypid()},
                              Old::nodelist:neighborhood(),
                              New::nodelist:neighborhood(),
                              Reason::rm_loop:reason()) -> ok.
neighborhood_succ_crash(Pid, {"mr_succ_fd", JobId, MasterId, Client}, _Old, _New, _Reason) ->
    io:format("~p: succ crashed...informing master~n",
              [Pid]),
    api_dht_raw:unreliable_lookup(MasterId,
                                  {mr_master, JobId,
                                   job_error,
                                   intervals:empty()}),
    comm:send(Client, {mr_results, {error, node_died}, intervals:empty(), JobId}).

-spec send_to_master(mr_state:state(), mr_master:message()) -> ok.
send_to_master(State, Msg) ->
    Key = mr_state:get(State, master_id),
    api_dht_raw:unreliable_lookup(Key, Msg).

-spec send_to_client(mr_state:state(), tuple()) -> ok.
send_to_client(State, Msg) ->
    Client = mr_state:get(State, client),
    comm:send(Client, Msg).
