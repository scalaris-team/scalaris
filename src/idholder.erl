%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    idholder.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Stores the key for the dht_node process.
%%%
%%%  This process stores the identifier of the dht_node. If the dht_node is 
%%%  restarted his identifier will survive in this process. We could use 
%%%  this e.g. when doing load-blancing.
%%% @end
%%% Created : 24 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(idholder).

-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2, set_id/2, get_id/0, reinit/0,
         check_config/0]).

-type(message() ::
    {reinit} |
    {get_id, PID::pid()} |
    {set_id, NewKey::?RT:key(), Count::non_neg_integer()}).
-type(state() :: {Key::?RT:key(), Count::non_neg_integer()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Sets the key of the dht_node including the counter that states how often
%%      a DHT node's ID changed (the version of the ID).
-spec set_id(NewKey::?RT:key(), Count::non_neg_integer()) -> ok.
set_id(Key, Count) ->
    cs_send:send_local(get_pid(), {set_id, Key, Count}).

%% @doc Reads the key of the dht_node; sends a 'idholder_get_id_response'
%%      message in response.
-spec get_id() -> ok.
get_id() ->
    cs_send:send_local(get_pid(), {get_id, self()}).

%% @doc Starts the idholder process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    gen_component:start_link(?MODULE, [], [{register, InstanceId, idholder}]).

%% @doc Initialises the idholder with a random key and a counter of 0.
-spec init([]) -> state().
init(_Arg) ->
    {get_initial_key(config:read(key_creator)), 0}.

%% @doc Resets the key to a random key and a counter of 0.
%%      Warning: This effectively states that a newly created DHT node is
%%      unrelated to the old one and should only be used if the old DHT node is
%%      stopped.
reinit() ->
    cs_send:send_local(get_pid(), {reinit}).

%% @doc Checks whether config parameters of the idholder process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_in(key_creator, [random, random_with_bit_mask]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}")
        end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Server process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state() | unknown_event.
on({reinit}, _State) ->
    {get_initial_key(config:read(key_creator)), 0};

on({get_id, PID}, {Key, Count} = State) ->
    cs_send:send_local(PID, {idholder_get_id_response, Key, Count}),
    State;

on({set_id, NewKey, Count}, _State) ->
    {NewKey, Count};

on(_, _State) ->
    unknown_event.

%% @doc Gets the pid of the idholder process in the same group as the calling
%%      process. 
-spec get_pid() -> pid() | failed.
get_pid() ->
    process_dictionary:get_group_member(idholder).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Key creation algorithms
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_initial_key(random | random_with_bit_mask) -> ?RT:key().
get_initial_key(random) ->
    ?RT:getRandomNodeId();
get_initial_key(random_with_bit_mask) ->
    {Mask1, Mask2} = config:read(key_creator_bitmask),
    (get_initial_key(random) band Mask2) bor Mask1.
