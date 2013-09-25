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
%% @doc Supervisor for map reduce processes per job
%%      It runs in two flavors: either it is sup_mr_job_JOBID, runs as a child
%%      of the nodes sup_mr and supervises the master process (transient, one_for_one)
%%      or
%%      it runs as sup_mr_worker_JOBID und the sup_mr_job_JOBID supervisor and
%%      supervises the worker processes (transient, rest_fo_one)
%% @end
%% @version $Id$
-module(sup_mr_job).
-author('fajerski@informatik.hu-berlin.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), io:format(X, Y)).

-behaviour(supervisor).

-export([start_link/1, init/1]).

-include("scalaris.hrl").

-type(master() :: {pid_groups:groupname(), Options::any()}).

-spec start_link(any())
        -> {ok, Pid::pid()} | ignore |
               {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(Options) ->
    case supervisor:start_link(?MODULE, Options) of
        {ok, Pid} -> {ok, Pid};
        X         -> X
    end.

-spec init(master()) ->
    {ok, {{one_for_one, MaxRetries::pos_integer(),
           PeriodInSeconds::pos_integer()},
          [ProcessDescr::supervisor:child_spec()]}}.
init({DHTNodeGroup, Options}) ->
    ?TRACE("sup_mr_job: ~p~nstarting master supervisor and master~n", [comm:this()]),
    pid_groups:join_as(DHTNodeGroup, "sup_mr_job_" ++ element(1, Options)),
    {ok, {{one_for_one, 10, 1}, children(DHTNodeGroup, Options)}}.

-spec children(pid_groups:groupname(), {nonempty_string(), comm:mypid(), any(), ?RT:key()}) ->
                    [ProcessDescr::supervisor:child_spec()].
children(DHTNodeGroup, Options) ->
    [{"mr_master_" ++ element(1, Options), {mr_master, start_link,
                                            [DHTNodeGroup, Options]}, transient,
      brutal_kill, worker, []}].
