% @copyright 2009-2011 Zuse Institute Berlin,
%            2009-2010 onScale solutions GmbH

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
%% @doc Part of a generic Paxos-Consensus implementation
%%           The role of a learner.
%% @end
%% @version $Id$
-module(learner).
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_paxosid/5, start_paxosid_local/5]).
-export([stop_paxosids/2]).

-export([start_link/2]).
-export([on/2, init/1]).
-export([check_config/0]).

-type state() :: atom(). % table name

-spec msg_decide(comm:mypid(), any(), any(), any()) -> ok.
msg_decide(Client, ClientCookie, PaxosID, Val) ->
    comm:send(Client, {learner_decide, ClientCookie, PaxosID, Val}).

-spec start_paxosid(comm:mypid(), any(), pos_integer(), comm:mypid(), any())
                   -> ok.
start_paxosid(Learner, PaxosID, Majority, ProcessToInform, ClientCookie) ->
    comm:send(Learner, {learner_initialize, PaxosID, Majority,
                           ProcessToInform, ClientCookie}).

-spec start_paxosid_local(pid(), any(), pos_integer(), comm:mypid(), any())
                         -> ok.
start_paxosid_local(Learner, PaxosID, Majority, ProcessToInform, ClientCookie) ->
    comm:send_local(Learner, {learner_initialize, PaxosID, Majority,
                              ProcessToInform, ClientCookie}).

-spec stop_paxosids(comm:mypid(), [any()]) -> ok.
stop_paxosids(Learner, ListOfPaxosIDs) ->
    comm:send(Learner, {learner_deleteids, ListOfPaxosIDs}).

%% startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname(), pid_groups:pidname()) -> {ok, pid()}.
start_link(DHTNodeGroup, PidName) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, PidName}]).

%% initialize: return initial state.
-spec init([]) -> atom().
init([]) ->
    ?TRACE("Starting learner for DHT node: ~p~n", [pid_groups:my_groupname()]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = list_to_atom(pid_groups:my_groupname() ++ "_learner"),
    %%pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    TableName = pdb:new(?MODULE, [set, protected]),
    _State = TableName.

-spec on(comm:message(), state()) -> state().
on({learner_initialize, PaxosID, Majority, ProcessToInform, ClientCookie},
   ETSTableName = State) ->
    ?TRACE("learner:initialize for paxos id: ~p~n", [PaxosID]),
    case pdb:get(PaxosID, ETSTableName) of
        undefined -> pdb:set(learner_state:new(PaxosID, Majority,
                                               ProcessToInform, ClientCookie),
                            ETSTableName);
        StateForID ->
            %% set Majority and ProcessInfo and check whether finished already
            case Majority =:= learner_state:get_majority(StateForID)
                andalso ProcessToInform =:= learner_state:get_process_to_inform(StateForID)
            of
                true ->
                    log:log(error,
                            "duplicate learner initialize for id ~p",
                            [PaxosID]);
                false ->
                    TmpState = learner_state:set_majority(StateForID, Majority),
                    Tmp2State = learner_state:set_process_to_inform(TmpState, ProcessToInform),
                    NewState = learner_state:set_client_cookie(Tmp2State, ClientCookie),
                    pdb:set(NewState, ETSTableName),
                    case (Majority =< learner_state:get_accepted_count(NewState)) of
                        true -> decide(PaxosID, NewState);
                        false -> ok
                    end
            end
    end,
    State;

on({acceptor_accepted, PaxosID, Round, Value}, ETSTableName = State) ->
    ?TRACE("learner:accepted for paxosid '~p' and round '~p' value '~p'~n",
           [PaxosID, Round, Value]),
    MyState = case pdb:get(PaxosID, ETSTableName) of
                  undefined ->
                      msg_delay:send_local(
                        config:read(learner_noinit_timeout) div 1000, self(),
                        {learner_deleteid_if_still_no_client, PaxosID}),
                      learner_state:new(PaxosID, 128, none, no_cookie);
                  StateForID -> StateForID
              end,
    case learner_state:add_accepted_msg(MyState, Round, Value) of
        {majority_accepted, NewState} ->
            decide(PaxosID, NewState),
            pdb:set(NewState, ETSTableName);
        {ok, NewState} -> pdb:set(NewState, ETSTableName);
        dropped -> ok
    end,
    State;

on({learner_deleteids, ListOfPaxosIDs}, ETSTableName = State) ->
    _ = [pdb:delete(Id, ETSTableName) || Id <- ListOfPaxosIDs],
    State;

on({learner_deleteid_if_still_no_client, PaxosID}, ETSTableName = State) ->
    case pdb:get(PaxosID, ETSTableName) of
        undefined -> ok;
        StateForId ->
            case learner_state:get_process_to_inform(StateForId) of
                none ->
                    %% io:format("Deleting unhosted learner id~n"),
                    pdb:delete(PaxosID, ETSTableName);
                _ -> ok
            end
    end,
    State;

on(_, _State) ->
    unknown_event.

decide(PaxosID, State) ->
    ?TRACE("learner:decide for paxosid '~p' in round '~p' and value '~p'~n",
           [PaxosID, learner_state:get_round(State), learner_state:get_value(State)]),
    case learner_state:get_process_to_inform(State) of
        none -> ok; % will be informed later when learner is initialized
        Pid -> msg_decide(Pid, learner_state:get_client_cookie(State),
                          PaxosID, learner_state:get_value(State))
    end.

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(learner_noinit_timeout) and
    config:cfg_is_greater_than_equal(learner_noinit_timeout, 1000) and
    config:cfg_is_greater_than(learner_noinit_timeout, tx_timeout).

