% @copyright 2013-2016 Zuse Institute Berlin,
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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc Triggers periodically a merge operation for a Bitcask DB.
%% @end
%% @version $Id$
-module(db_bitcask_merge_extension).
-author('skrzypczak@zib.de').

-include("scalaris.hrl").

-define(TRACE(X), ?TRACE(X, [])).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-define(MERGE_INTERVAL, config:read(bitcask_merge_interval)).
-define(MERGE_OFFSET, config:read(bitcask_merge_offset)).

-export([check_config/0]).
-export([init/1, init/2]).
-export([on/2]).

%% This component uses a trigger mechanism to become active from time
%% to time. It is a dht_node_extension and therefore embedded in the
%% dht_node process.

-spec init(_Options::any()) -> ok.
init(_Options) ->
    % ignore dht_node_extension init since this module needs a
    % bitcask DB reference. db_bitcask.erl calls init/2 upon opening
    % a bitcask store.
    ok.

%% @doc Sends a periodic trigger to itself
-spec init(BitcaskHandle::reference(), Dir::string()) -> ok.
init(BitcaskHandle, Dir) ->
    ?TRACE("bitcask_merge: init for DB directory ~p~n", [Dir]),
    send_trigger(merge_offset, BitcaskHandle, Dir, ?MERGE_OFFSET),
    ok.

-spec on(comm:message(), State::dht_node_state:state()) -> dht_node_state:state().
on({merge_offset, BitcaskHandle, Dir}, State) ->
    %% calculate initial offset to prevent that all nodes are merging at the
    %% same time.
    mgmt_server:node_list(),
    Nodes = receive
                ?SCALARIS_RECV(
                    {get_list_response, List},
                    lists:usort([Pid || {_, _, Pid} <- List])
                )
            after 2000 ->
                [self()]
            end,

    Slot = get_idx(self(), Nodes),
    SlotLength = ?MERGE_INTERVAL / length(Nodes),
    Offset = trunc(SlotLength * Slot),
    ?TRACE("PIDs: ~p~n~nSlot choosen: ~p~n", [Nodes, Slot]),
    send_trigger(merge_trigger, BitcaskHandle, Dir, Offset),

    State;

on({merge_trigger, BitcaskHandle, Dir}, State) ->
    %% merges if necessary

    _ = case erlang:get(BitcaskHandle) of
        undefined ->
            %% handle was closed, no more merge operations
            %% possible.
            ?TRACE("bitcask_merge: Bitcask handle closed for ~p~n", [Dir]),
            ok;
        _ -> 
            %% bitcask:needs_merge/1 cleans up open file descriptors left
            %% over from the last successful merge. Deleted files stay
            %% open until then...
            _ = case bitcask:needs_merge(BitcaskHandle) of
                {true, Files} ->
                    ?TRACE("bitcask_merge: starting merge in dir ~p~n", [Dir]),
                    bitcask_merge_worker:merge(Dir, [], Files);
                false ->
                    ok
            end,
            send_trigger(merge_trigger, BitcaskHandle, Dir, ?MERGE_INTERVAL)
    end,
    State.

%% @doc Sends a trigger message to itself
-spec send_trigger(Message::atom(), BitcaskHandle::reference(),
    Dir::string(), Delay::integer()) -> ok.
send_trigger(Message, BitcaskHandle, Dir, Delay) ->
    msg_delay:send_trigger(Delay,
                           dht_node_extensions:wrap(?MODULE, {Message,
                                                              BitcaskHandle,
                                                              Dir})),
    ok.

%% @doc Helper to find the index of an element in a list.
%% Returns length(List)+1 if List does not contain E.
-spec get_idx(E::any(), List::any()) -> integer().
get_idx(E, List) -> get_idx(E, List, 1).

-spec get_idx(E::any(), List::any(), Idx::integer()) -> integer().
get_idx(_, [], Idx) -> Idx;
get_idx(E, [E | _], Idx) -> Idx;
get_idx(E, [_ | T], Idx) -> get_idx(E, T, Idx+1).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(bitcask_merge_interval) and
    config:cfg_is_integer(bitcask_merge_offset).
