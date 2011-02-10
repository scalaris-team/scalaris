%  @copyright 2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @version $Id:$
-module(sup_paxos).
-author('schintke@zib.de').
-vsn('$Id: $').
-behaviour(supervisor).
-export([start_link/2, init/1]).

-spec start_link(pid_groups:groupname(), Options::[tuple()]) ->
                        {ok, Pid::pid()} | ignore |
                        {error, Error::{already_started, Pid::pid()} |
                                       shutdown | term()}.
start_link(PidGroup, Options) ->
    supervisor:start_link(?MODULE, {PidGroup, Options}).

%% userdevguide-begin sup_dht_node_core:init
-spec init({pid_groups:groupname(), Options::[tuple()]}) ->
                  {ok, {{one_for_one, MaxRetries::pos_integer(),
                         PeriodInSeconds::pos_integer()},
                        [ProcessDescr::any()]}}.
init({PidGroup, Options}) ->
    {SupervisorName, ProposerName, AcceptorName, LearnerName} =
        case lists:keyfind(sup_paxos_prefix, 1, Options) of
            {sup_paxos_prefix, Prefix} ->
                PrefixS = atom_to_list(Prefix),
                {list_to_atom(PrefixS ++ atom_to_list(?MODULE)),
                 list_to_atom(PrefixS ++ atom_to_list('_proposer')),
                 list_to_atom(PrefixS ++ atom_to_list('_acceptor')),
                 list_to_atom(PrefixS ++ atom_to_list('_learner'))};
            false -> {?MODULE, paxos_proposer, paxos_acceptor, paxos_learner}
        end,
    pid_groups:join_as(PidGroup, SupervisorName),
    Proposer = util:sup_worker_desc(proposer, proposer,
                                    start_link, [PidGroup, ProposerName]),
    Acceptor = util:sup_worker_desc(acceptor, acceptor,
                                    start_link, [PidGroup, AcceptorName]),
    Learner = util:sup_worker_desc(learner, learner,
                                   start_link, [PidGroup, LearnerName]),
    {ok, {{one_for_one, 10, 1}, [Proposer, Acceptor, Learner]}}.
