%% @copyright 2008-2012 Zuse Institute Berlin

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
%% @version $Id$
-module(sup_comm_layer).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([supspec/1, childs/1]).

-spec start_link() -> {ok, Pid::pid()} | ignore |
                      {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec init([]) -> {ok, {{one_for_all, MaxRetries::pos_integer(),
                                      PeriodInSeconds::pos_integer()},
                         [ProcessDescr::supervisor:child_spec()]}}.
init(X) ->
    CommLayerGroup = "comm_layer",
    pid_groups:join_as(CommLayerGroup, ?MODULE),
    supspec(X).

supspec(_) ->
    {ok, {{one_for_all, 10, 1}, []}}.

childs(_) ->
    CommLayerGroup = "comm_layer",
    CommServer =
        util:sup_worker_desc(comm_server, comm_server, start_link,
                             [CommLayerGroup]),
    CommAcceptor =
        util:sup_worker_desc(comm_acceptor, comm_acceptor, start_link,
                             [CommLayerGroup]),
    CommLogger =
        util:sup_worker_desc(comm_logger, comm_logger, start_link),
    [
     CommServer,
     CommLogger,
     CommAcceptor
    ].
