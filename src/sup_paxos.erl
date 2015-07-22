%  @copyright 2011, 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>

%% @doc Supervisor to instantiate paxos processes (proposer, acceptor,
%%      learner). Each process that uses paxos should instantiate
%%      its own set of paxos processes to avoid clashing of paxos ids.
%%
%%      To name the processes inside a pid_group, a naming prefix can
%%      be passed %% to the start_link/2 call. The supervisor will
%%      append '_proposer', '_acceptor', and '_learner' to the given
%%      prefix for the individual processes. If no prefix is given,
%%      the processes register as 'paxos_proposer', 'paxos_acceptor',
%%      and 'paxos_learner' directly.
%% @end
%% @version $Id$
-module(sup_paxos).
-author('schintke@zib.de').
-vsn('$Id$').
-behaviour(supervisor).
-export([start_link/1, init/1]).
-export([supspec/1, childs/1]).

-spec start_link({pid_groups:groupname(), Options::[tuple()]}) ->
                        {ok, Pid::pid()} | ignore |
                        {error, Error::{already_started, Pid::pid()} |
                                       shutdown | term()}.
start_link({PidGroup, Options}) ->
    supervisor:start_link(?MODULE, [{PidGroup, Options}]).

%% userdevguide-begin sup_dht_node_core:init
-spec init([{pid_groups:groupname(), Options::[tuple()]}]) ->
                  {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()},
                        [ProcessDescr::supervisor:child_spec()]}}.
init([{PidGroup, Options}] = X) ->
    SupervisorName =
        case lists:keyfind(sup_paxos_parent, 1, Options) of
            {sup_paxos_parent, Parent} ->
                {Parent, ?MODULE};
            false -> ?MODULE
        end,
    pid_groups:join_as(PidGroup, SupervisorName),
    supspec(X).

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.

-spec childs([{pid_groups:groupname(), Options::[tuple()]}]) ->
                    [ProcessDescr::supervisor:child_spec()].
childs([{PidGroup, Options}]) ->
    {ProposerName, AcceptorName, LearnerName} =
        case lists:keyfind(sup_paxos_parent, 1, Options) of
            {sup_paxos_parent, Parent} ->
                {{Parent, proposer},
                 {Parent, acceptor},
                 {Parent, learner}};
            false ->
                {paxos_proposer, paxos_acceptor, paxos_learner}
        end,
    Proposer = sup:worker_desc(proposer, proposer,
                                    start_link, [PidGroup, ProposerName]),
    Acceptor = sup:worker_desc(acceptor, acceptor,
                                    start_link, [PidGroup, AcceptorName]),
    Learner = sup:worker_desc(learner, learner,
                                   start_link, [PidGroup, LearnerName]),
    [Proposer, Acceptor, Learner].
