%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc    dht_node helper process for monitoring
%% @end
%% @version $Id$
-module(dht_node_monitor).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-behaviour(gen_component).

-export([start_link/2, on/2, init/1]).

% accepted messages of dht_node_monitor processes
-type message() :: {Key::lookup_hops, Value::pos_integer()} |
                    {Msg::db_op_init, Value::?RT:key(), Range::intervals:interval()} |
                    {Key::db_op, Value::?RT:key()}.
-type state() :: {LookupHops::rrd:rrd(),
                  DBOps::rrd:rrd() | uninit}.

%% @doc message handler
-spec on(message(), state()) -> state().
on({lookup_hops, Hops}, {OldLookupHops, OldDBOps}) ->
    NewLookupHops = rrd:add_now(Hops, OldLookupHops),
    monitor:check_report(dht_node, 'lookup_hops', OldLookupHops, NewLookupHops),
    {NewLookupHops, OldDBOps};

on({db_op_init, Id}, {OldLookupHops, _OldDBOps}) ->
    NewDBOps = lb_active:init_db_rrd(Id),
    {OldLookupHops, NewDBOps};

on({db_op, Key}, {OldLookupHops, OldDBOps}) ->
    NewDBOps = lb_active:update_db_rrd(Key, OldDBOps),
    {OldLookupHops, NewDBOps}.

%% @doc initialisation
-spec init(Options::[tuple()]) -> state().
init(_Options) ->
    % 1m monitoring interval, only keep newest
    LookupHops = rrd:create(60 * 1000000, 1, {timing, count}),
    %% initialized by dht_node with handler db_op_init
    {LookupHops, uninit}.

%% @doc spawns a dht_node_monitor, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, Options, [{pid_groups_join_as, DHTNodeGroup, dht_node_monitor}]).