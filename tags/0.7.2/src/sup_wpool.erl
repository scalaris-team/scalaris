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
%% @doc    supervisor for wpool workers
%% @end
%% @version $Id$
-module(sup_wpool).
-author('fajerski@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, init/1]).
-export([supspec/1]).

-spec start_link(pid_groups:groupname())
        -> {ok, Pid::pid(), pid_groups:groupname()} | ignore |
               {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(DHTNodeGroup) ->
    case supervisor:start_link(?MODULE, DHTNodeGroup) of
        {ok, Pid} -> {ok, Pid, DHTNodeGroup};
        X         -> X
    end.

-spec init(pid_groups:groupname()) ->
                  {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
init(DHTNodeGroup) ->
    pid_groups:join_as(DHTNodeGroup, sup_wpool),
    supspec([DHTNodeGroup]).

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.
