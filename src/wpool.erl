%% @copyright 2007-2015 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc worker pool implementation.
%%      This gen_component handles arbitrary workloads for other processes. It
%%      can be send a do_work message with a job specification and will return
%%      the results of the computation or an error.
%%      Jobs will run concurrently in seperate processes. The maximum number of
%%      concurrent workers can be configured via the wpool_maxw setting in
%%      scalaris.[local.]config.
%%      when the maximum number of jobs is reached new jobs are queued and run
%%      as soon as a running job finishes.
%%
%% @end
%% @version $Id$
-module(wpool).
-author('fajerski@zib.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).

-behaviour(gen_component).

-export([
        start_link/2
        , on/2
        , init/1
        ]).

-include("scalaris.hrl").

-export_type([job/0]).

-include("gen_component.hrl").

-type(mr_job() :: {mr_state:fun_term(),
                   Data::db_ets:db(), Interval::intervals:interval()}).

%% -type(generic_job() :: {erlanon | jsanon, binary(), [tuple()]}).

-type(job() :: mr_job()).

-type(message() :: {do_work, Source::comm:mypid(), job()} |
                   %% TODO find better spec for Info
                   {'DOWN', reference(), process, pid(), Info::any()} |
                   {data, pid(), [tuple()]}).

%% running jobs are defined by the pid the worker process has and the source of
%% the job
-type(avail_workers() :: [comm:mypid()]).
-type(working() :: [{Worker::comm:mypid(), Source::comm:mypid()}]).

%% a waiting job is defined by the source and the job spec
-type(waiting_jobs() :: [{comm:mypid(), job()}]).

-type(state() :: {avail_workers(), working(), waiting_jobs()}).

-spec init([]) -> state().
init([]) ->
    {[start_worker(X) || X <- lists:seq(1, config:read(wpool_maxw))],
     [],
     []}.

-spec start_link(pid_groups:groupname(), tuple()) -> {ok, pid()}.
start_link(DHTNodeGroup, _Options) ->
    ?TRACE("wpool: starting on node ~p~n", [DHTNodeGroup]),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, wpool}]).

-spec on(message(), state()) -> state().
%% new job arrives...either start it right away or queue it
on({do_work, Source, Workload}, {[], Working, Waiting}) ->
    {[], Working, lists:append(Waiting, [{Source, Workload}])};
on({do_work, Source, Workload}, {[Pid | R], Working, Waiting}) ->
    comm:send_local(Pid, {work, self(), Workload}),
    {R, [{Pid, Source} | Working], Waiting};

%% worker threw an exception. send exception info back to origin
on({'DOWN', exc_subs, wpool_worker, Pid, Reason}, {Avail, Working, Waiting}) ->
    %% normal termination
    ?TRACE("worker ~p threw exception ~p~n", [Pid, Reason]),
    {value, {Pid, Source}, NewWorking} = lists:keytake(Pid, 1, Working),
    comm:send(Source, {worker_died, Reason}),
    worker_finished({Avail, NewWorking, Waiting}, Pid);

%% worker sends results; forward to source
%% TODO hsould be better if worker sends directly to source and just informs
%% wpool that it is available for new work
on({data, Pid, Data}, {Avail, Working, Waiting}) ->
    ?TRACE("wpool: received data from ~p:~n~p~n",
            [Pid, Data]),
    %% send results to source
    {value, {_Pid, Source}, NewWorking} = lists:keytake(Pid, 1, Working),
    comm:send(Source, {work_done, Data}),
    worker_finished({Avail, NewWorking, Waiting}, Pid);

on(Msg, State) ->
    io:format("~200p~nwpool: unknown message~n", [Msg]),
    State.

%% @doc start a worker
%%     starts worker under the supervisor sup_wpool
-spec start_worker(pos_integer()) -> pid().
start_worker(ID) ->
    Sup = pid_groups:get_my(sup_wpool),
    case supervisor:start_child(Sup, {{wpool_w, ID}, {wpool_worker, start_link,
                                                [{exception_subscription,
                                                  [comm:this()]}]}, permanent,
                                            brutal_kill, worker, []}) of
        {ok, Pid} ->
            Pid;
        {ok, Pid, _Info} ->
            Pid
    end.

-spec worker_finished(state(), comm:mypid()) -> state().
worker_finished({Avail, Working, []}, Pid) ->
    {[Pid | Avail], Working, []};
worker_finished({Avail, Working, [{Source, Workload} | MoreWaiting]}, Pid) ->
    comm:send_local(Pid, {work, self(), Workload}),
    {Avail, [{Pid, Source} | Working], MoreWaiting}.
