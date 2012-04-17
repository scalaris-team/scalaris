%  @copyright 2012 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @doc    Supervisor for each DHT node that is responsible for keeping
%%         processes running that run for themselves.
%%
%%         If one of the supervised processes fails, only the failed process
%%         will be re-started!
%% @end
%% @version $Id$
-module(sup_dht_node2).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, start_link/0, init/1]).

-spec start_link([tuple()])
        -> {ok, Pid::pid(), pid_groups:groupname()} | ignore |
               {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(Options) ->
    DHTNodeGroup = pid_groups:new("dht_node2_"),
    case supervisor:start_link(?MODULE, {DHTNodeGroup, Options}) of
        {ok, Pid} -> {ok, Pid, DHTNodeGroup};
        X         -> X
    end.

-spec start_link()
        -> {ok, Pid::pid(), pid_groups:groupname()} | ignore |
               {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link() ->
    start_link([]).

-spec init({pid_groups:groupname(), [tuple()]})
        -> {ok, {{one_for_one, MaxRetries::pos_integer(), PeriodInSeconds::pos_integer()},
                 [ProcessDescr::supervisor:child_spec()]}}.
init({DHTNodeGroup, Options}) ->
    pid_groups:join_as(DHTNodeGroup, ?MODULE),
    mgmt_server:connect(),

    _Router =
        util:sup_worker_desc(routing_table, router, start_link,
                             [DHTNodeGroup]),

    DataNode =
        util:sup_worker_desc(data_node, data_node, start_link,
                             [DHTNodeGroup, Options]),
%% order in the following list is the start order
    {ok, {{one_for_one, 10, 1},
          [
           DataNode
          %Router
          ]}}.
