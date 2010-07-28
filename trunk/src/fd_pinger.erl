%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Christian Hennig <hennig@zib.de>
%% @doc    Ping process for fd.erl.
%% @end
%% @version $Id$
-module(fd_pinger).
-author('hennig@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").

-export([init/1, on/2, start_link/1, check_config/0]).

-type(state() :: {module(), comm:mypid(), Count::non_neg_integer()}).
-type(message() ::
    {stop} |
    {pong} |
    {timeout, OldCount::non_neg_integer()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a fd_pinger instance
-spec start_link([module() | comm:mypid()]) -> {ok, pid()}.
start_link([Module, Pid]) ->
   gen_component:start_link(?MODULE, [Module, Pid], []).

-spec init([module() | comm:mypid()]) -> state().
init([Module, Pid]) ->
    log:log(info,"[ fd_pinger ~p ] starting Node", [self()]),
    comm:send(Pid, {ping, comm:this()}),
    comm:send_local_after(failureDetectorInterval(), self(), {timeout, 0}),
    {Module, Pid, 0}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({stop}, {_Module, _Pid, _Count}) ->
    kill;
on({pong}, {Module, Pid, Count}) ->
    {Module, Pid, Count+1};
on({timeout, OldCount}, {Module, Pid, Count}) ->
    case OldCount < Count of 
        true -> 
            comm:send(Pid, {ping, comm:this()}),
            comm:send_local_after(failureDetectorInterval(), self(), {timeout, Count}),
            {Module, Pid, Count};
        false ->    
           report_crash(Pid, Module),
           kill
    end.

%% @doc Checks whether config parameters of the gossip process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(failure_detector_interval) and
    config:is_greater_than(failure_detector_interval, 0).

%% @doc Reports the crash of the given pid to the given module.
%% @private
-spec report_crash(comm:mypid(), module()) -> ok.
report_crash(Pid, Module) ->
    log:log(warn,"[ FD ] ~p crashed",[Pid]),
    comm:send_local(Module, {crash, Pid}).

%% @doc The interval between two failure detection runs.
-spec failureDetectorInterval() -> pos_integer().
failureDetectorInterval() ->
    config:read(failure_detector_interval).
