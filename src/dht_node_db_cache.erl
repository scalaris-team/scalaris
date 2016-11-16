%  @copyright 2013-2015 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Caches some DB values that are not relevant for consistence.
%% @end
%% @version $Id$
-module(dht_node_db_cache).

-author('kruber@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2]).

-include("gen_component.hrl").

-type(message() ::
    {get_split_key, DB::db_dht:db(), CurRange::intervals:interval(),
     Begin::?RT:key(), End::?RT:key(),
     TargetLoad::pos_integer(), Direction::forward | backward, SourcePid::comm:erl_local_pid()} |
    {get_split_key_wrapper, SourcePid::comm:erl_local_pid(), DB::db_dht:db(),
     CurRange::intervals:interval(), {get_split_key_response, Val::{?RT:key(),
     TakenLoad::non_neg_integer()}}} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

-type state() :: [{DB::db_dht:db(), Range::intervals:interval(), Expires::erlang_timestamp(),
                   Key::get_split_key, Val::{?RT:key(), TakenLoad::non_neg_integer()}}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a db_dht cache process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

%% @doc Initialises the module with an uninitialized state.
-spec init([]) -> state().
init([]) ->
    [].
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({get_split_key, DB, CurRange, Begin, End, TargetLoad, Direction, SourcePid}, State) ->
    Now = util:timestamp2us(os:timestamp()),
    case lists:keyfind(get_split_key, 4, State) of
        {DB, CurRange, Expires, get_split_key, Val} when Now < Expires ->
            comm:send_local(SourcePid, {get_split_key_response, Val}),
            State;
        _ ->
            Sender = comm:reply_as(self(), 5,
                                   {get_split_key_wrapper, SourcePid, DB, CurRange, '_'}),
            comm:send_local(pid_groups:get_my(dht_node),
                            {get_split_key, DB, Begin, End,
                             TargetLoad, Direction, Sender}),
            State
    end;

on({get_split_key_wrapper, SourcePid, DB, CurRange, {get_split_key_response, Val}}, State) ->
    comm:send_local(SourcePid, {get_split_key_response, Val}),
    Now = util:timestamp2us(os:timestamp()),
    case lists:keyfind(get_split_key, 4, State) of
        {DB, CurRange, Expires, get_split_key, Val} when Now < Expires ->
            State;
        _ ->
            CachedVal = {DB, CurRange, Now + 10 * 1000000, % 10s
                         get_split_key, Val},
            lists:keystore(get_split_key, 4, State, CachedVal)
    end;

on({web_debug_info, Requestor}, State) ->
    KeyValueList =
        [{"Cached values:", ""} |
             [{webhelpers:safe_html_string(
                 "~.0p (Expires: ~.0p UTC)",
                 [Key, calendar:now_to_universal_time(util:us2timestamp(Expires))]),
               webhelpers:safe_html_string("~.0p", [Val])}
             || {_DB, _Range, Expires, Key, Val} <- State]],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.
