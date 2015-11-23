%  @copyright 2007-2015 Zuse Institute Berlin

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

-include("gen_component.hrl").

% accepted messages of dht_node_monitor processes
-type message() :: {Key::lookup_hops, Value::pos_integer()} |
                    {Msg::db_histogram_init, Value::?RT:key(), Range::intervals:interval()} |
                    {Key::db_op, Value::?RT:key()}.
-type state() :: {LookupHops::rrd:rrd(),
                  DBOps::rrd:rrd(), %% counts the number of db operations in a fixed interval
                  DBHistogram::rrd:rrd() | uninit %% histogram of db operations which gets resetted on pred or node id change
                 }.

%% @doc message handler
-spec on(message(), state()) -> state().
on({lookup_hops, Hops}, {OldLookupHops, OldDBOps, OldDBHistogram}) ->
    NewLookupHops = rrd:add_now(Hops, OldLookupHops),
    monitor:check_report(dht_node, 'lookup_hops', OldLookupHops, NewLookupHops),
    {NewLookupHops, OldDBOps, OldDBHistogram};

on({db_histogram_init, Id}, {OldLookupHops, OldDBOps, _OldDBHistogram}) ->
    NewDBHistogram = lb_stats:init_db_histogram(Id),
    {OldLookupHops, OldDBOps, NewDBHistogram};

on({db_report}, {OldLookupHops, OldDBOps, OldDBHistogram}) ->
    case lb_active:requests_balance() of
        true ->
            NewDBHistogram = rrd:add_now(no_op, OldDBHistogram),
            monitor:check_report(lb_active, db_histogram, OldDBHistogram, NewDBHistogram);
        _ ->
            NewDBHistogram = OldDBHistogram
    end,
    NewDBOps = rrd:add_now(0, OldDBOps),
    monitor:check_report(lb_active, db_ops, OldDBOps, NewDBOps),
    {OldLookupHops, NewDBOps, NewDBHistogram};

on({db_op, Key}, {OldLookupHops, OldDBOps, OldDBHistogram}) ->
    NewDBOps = rrd:add_now(1, OldDBOps),
    NewDBHistogram = lb_stats:update_db_histogram(Key, OldDBHistogram),
    {OldLookupHops, NewDBOps, NewDBHistogram}.

%% @doc initialisation
-spec init(Options::[tuple()]) -> state().
init(_Options) ->
    % 1m monitoring interval, only keep newest
    LookupHops = rrd:create(60 * 1000000, 1, {timing, count}),
    DBOps = rrd:create(config:read(lb_active_monitor_resolution) * 1000, 1, counter),
    %% initialized by dht_node with handler db_op_init
    DBHistogram = uninit,
    {LookupHops, DBOps, DBHistogram}.

%% @doc spawns a dht_node_monitor, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, Options, [{pid_groups_join_as, DHTNodeGroup, dht_node_monitor}]).
