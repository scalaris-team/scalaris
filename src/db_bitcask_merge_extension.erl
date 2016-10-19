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

-define(MERGE_INTERVAL, 10 * 60). % in seconds.

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
    trigger_merge(BitcaskHandle, Dir, ?MERGE_INTERVAL),
    ok.

-spec on(comm:message(), State::dht_node_state:state()) -> dht_node_state:state().
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
            trigger_merge(BitcaskHandle, Dir, ?MERGE_INTERVAL)
    end,
    State.

%% @doc Sends a merge_trigger message to itself
-spec trigger_merge(BitcaskHandle::reference(),
    Dir::string(), Delay::integer()) -> ok.
trigger_merge(BitcaskHandle, Dir, Delay) ->
    msg_delay:send_trigger(Delay,
                           dht_node_extensions:wrap(?MODULE, {merge_trigger,
                                                              BitcaskHandle,
                                                              Dir})),
    ok.
