% @copyright 2009-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%                 onScale solutions GmbH
% @end

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc Cheap message delay.
%%      Instead of using send_after, which is slow in Erlang, as it
%%      performs a system call, this module allows for a weaker
%%      message delay.
%%      You can specify the minimum message delay in seconds and the
%%      component will send the message sometime afterwards.
%%      Only local messages inside a VM are supported.
%%      Internally it triggers itself periodically to schedule sending.
%% @end
%% @version $Id$
-module(msg_delay).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).

-include("scalaris.hrl").

%% public interface for transaction validation using Paxos-Commit.
-export([send_local/3]).

%% functions for gen_component module and supervisor callbacks
-export([start_link/1, start_link/2]).
-export([on/2, init/1]).

% accepted messages of the msg_delay process
-type(message() ::
    {msg_delay_req, Seconds::number(), Dest::comm:erl_local_pid(), Msg::comm:message()} |
    {msg_delay_periodic}).

% internal state
-type(state()::{TableName::atom(), Round::non_neg_integer()}).

-spec send_local(Seconds::number(), Dest::comm:erl_local_pid(), Msg::comm:message()) -> ok.
send_local(Seconds, Dest, Msg) ->
    Delayer = process_dictionary:get_group_member(msg_delay),
    comm:send_local(Delayer, {msg_delay_req, Seconds, Dest, Msg}).

%% be startable via supervisor, use gen_component
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    start_link(InstanceId, []).

-spec start_link(instanceid(), [any()]) -> {ok, pid()}.
start_link(InstanceId, Options) ->
    gen_component:start_link(?MODULE,
                             [InstanceId, Options],
                             [{register, InstanceId, msg_delay}]).

%% initialize: return initial state.
-spec init([instanceid() | [any()]]) -> state().
init([InstanceID, _Options]) ->
    ?TRACE("msg_delay:init ~p~n", [InstanceID]),
    TableName =
        list_to_atom(lists:flatten(
                       io_lib:format("~p_msg_delay", [InstanceID]))),
    %% use random table name provided by ets to *not* generate an atom
    %% TableName = pdb:new(?MODULE, [set, private]),
    pdb:new(TableName, [set, protected, named_table]),
    comm:send_local(self(), {msg_delay_periodic}),
    _State = {TableName, _Round = 0}.

-spec on(message(), state()) -> state().
on({msg_delay_req, Seconds, Dest, Msg}, {TableName, Counter} = State) ->
    ?TRACE("msg_delay:on(msg_delay...)~n", []),
    Future = trunc(Counter + Seconds),
    case pdb:get(Future, TableName) of
        undefined ->
            pdb:set({Future, [{Dest, Msg}]}, TableName);
        {_, Queue} ->
            pdb:set({Future, [{Dest, Msg}| Queue]}, TableName)
    end,
    State;

%% periodic trigger
on({msg_delay_periodic} = Trigger, {TableName, Counter} = _State) ->
    ?TRACE("tx_tm_rtm:on(msg_delay_periodic)~n", []),
    case pdb:get(Counter, TableName) of
        undefined -> ok;
        {_, Queue} ->
            [ comm:send_local(Dest, Msg) || {Dest, Msg} <- Queue ],
            pdb:delete(Counter, TableName)
    end,
    comm:send_local_after(1000, self(), Trigger),
    {TableName, Counter + 1}.
