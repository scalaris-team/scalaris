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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc worker pool implementation.
%%      This gen_component handles arbitrary workloads for other processes. It
%%      can be send a do_work message with a job specification and will return
%%      the results of the computation or an error.
%%      Jobs will run concurrently in seperate processes. The maximum number of
%%      concurrent jobs must be configured via the wpool_maxw setting in
%%      scaralis.[local.]config.
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
-export([init_worker/2, work/2]).

-include("scalaris.hrl").

-type(message() :: {do_work, Source::comm:my_pid(), job()} |
                   %% TODO find better spec for Info
                   {'DOWN', reference(), process, pid(), Info::any()} |
                   {data, pid(), [tuple()]}).

-type(mr_job() :: {Round::pos_integer(),
                   map | reduce,
                   {erlanon | jsanon, binary()},
                   Keep::boolean(),
                   Data::[tuple()]}).

-type(generic_job() :: {erlanon | jsanon, binary(), [tuple()]}).

-type(job() :: mr_job() | generic_job()).

%% running jobs are defined by the pid the worker process has and the source of
%% the job
-type(active_jobs() :: [{pid(), comm:mypid()}]).

%% a waiting job is defined by the source and the job spec
-type(waiting_jobs() :: [{comm:mypid(), job()}]).

-type(state() :: {MaxWorkers::pos_integer(), active_jobs(), waiting_jobs()}).

-spec init([]) -> state().
init([]) ->
    {config:read(wpool_maxw),[], []}.

-spec start_link(pid_groups:groupname(), tuple()) -> {ok, pid()}.
start_link(DHTNodeGroup, _Options) ->
    ?TRACE("wpool: starting on node ~p~n", [DHTNodeGroup]),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, wpool}]).

-spec on(message(), state()) -> state().
%% new job arrives...either start it right away or queue it
on({do_work, Source, Workload}, {Max, Working, Waiting}) when
        length(Working) >= Max ->
    {Max, Working, lists:append(Waiting, [{Source, Workload}])};
on({do_work, Source, Workload}, State) ->
    start_worker(Source, Workload, State);

%% worker terminated; clear it from the working list and do error reporting if
%% necessarry
on({'DOWN', _Ref, process, Pid, Reason}, State) ->
    ?TRACE("worker finished with reason ~p~n", [Reason]),
    %% TODO in case of error send some report back
    cleanup_worker(Pid, State);

%% worker sends results; forward to source
on({data, Pid, Data}, {_Max, Working, _Waiting} = State) ->
    ?TRACE("wpool: received data from ~p:~n~p...~n",
            [Pid, lists:sublist(Data, 4)]),
    %% send results to source
    {_Pid, Source} = lists:keyfind(Pid, 1, Working),
    comm:send(Source, {work_done, Data}),
    State;

on(Msg, State) ->
    ?TRACE("~200p~nwpool: unknown message~n", [Msg]),
    State.

%% starts worker under the supervisor sup_wpool and also sets up a monitor. The
%% supvervisor does not restart the worker in case of failure. Its main purpose
%% is to shut workers down when scalaris is shuting down.
%% The monitor (between wpool and the worker) is used mainly for error
%% reporting. A link could also be used, but wpool would have to call
%% ``process_flag(trap_exit, true)'' for every link.
-spec start_worker(comm:mypid(), job(), state()) -> state().
start_worker(Source, Workload, State) ->
    Sup = pid_groups:get_my(sup_wpool),
    case supervisor:start_child(Sup, {worker, {wpool, init_worker,
                                                [pid_groups:my_groupname(),
                                                 Workload]}, temporary,
                                            brutal_kill, worker, []}) of
        {ok, Pid} ->
            monitor_worker(Pid, Source, State);
        {ok, Pid, _Info} ->
            monitor_worker(Pid, Source, State);
        X ->
            ?TRACE("start child failed ~p~n", [X]),
            %% handle failures
            State
    end.

%% monitor worker and put it into the Working queue
-spec monitor_worker(pid(), comm:my_pid(), state()) -> state().
monitor_worker(Pid, Source, {Max, Working, Waiting}) ->
    monitor(process, Pid),
    {Max, [{Pid, Source} | Working], Waiting}.

%% remove worker from Working queue and start a waiting job if present
-spec cleanup_worker(pid(), state()) -> state().
cleanup_worker(Pid, {Max, Working, Waiting}) ->
    NewWorking = lists:keydelete(Pid, 1, Working),
    case length(Waiting) of
        0 ->
            {Max, NewWorking, Waiting};
        _ ->
            [{Source, Workload} | Rest] = Waiting,
            start_worker(Source, Workload, {Max, NewWorking, Rest})
    end.

%% actual worker functions
-spec init_worker(pid_groups:groupname(), job()) -> {ok, pid()}.
init_worker(DHTNodeGroup, Workload) ->
    Pid = spawn_link(?MODULE, work, [DHTNodeGroup, Workload]),
    {ok, Pid}.

%% do the actual work
%% TODO join pid_group as worker_RANDOMID so more than one worker can be active
-spec work(pid_groups:groupname(), job()) -> ok.
work(DHTNodeGroup, {_Round, map, {erlanon, FunBin}, _Keep, Data}) ->
    pid_groups:join_as(DHTNodeGroup, worker),
    %% ?TRACE("worker: should apply ~p to ~p~n", [FunBin, Data]),
    Fun = binary_to_term(FunBin, [safe]),
    return(lists:flatten([Fun(X) || X <- Data]));
work(DHTNodeGroup, {_Round, reduce, {erlanon, FunBin}, _Keep, Data}) ->
    pid_groups:join_as(DHTNodeGroup, worker),
    Fun = binary_to_term(FunBin, [safe]),
    return(Fun(Data)).

%% send results back to wpool
-spec return(any()) -> ok.
return(Data) ->
    MyPool = pid_groups:get_my(wpool),
    comm:send_local(MyPool, {data, self(), Data}).
