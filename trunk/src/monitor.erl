%  @copyright 2011 Zuse Institute Berlin

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
%% @doc    Monitors data from different processes, e.g. for performance
%%         evaluations.
%% @end
%% @version $Id$
-module(monitor).
-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("scalaris.hrl").

% monitor process functions
-export([start_link/1, init/1, on/2, check_config/0]).

% functions (temporarily) storing monitoring values in the calling process:
-export([proc_set_value/3, proc_get_value/2, proc_exists_value/2,
         proc_check_timeslot/2, proc_check_all_timeslot/0]).

% functions sending monitoring values directly to the monitor process
-export([monitor_set_value/3, client_monitor_set_value/3]).

-type key() :: string().
-type internal_key() :: {'$monitor$', Process::atom(), Key::string()}.
-type table_index() :: {Process::atom(), Key::key()}.

-type state() :: Table::tid() | atom().
-type message() ::
    {report_rrd, Process::atom(), Key::key(), OldValue::rrd:rrd(), Value::rrd:rrd()} |
    {report_single, Process::atom(), Key::key(),
     NewValue_or_UpdateFun::term() | fun((Old::Value | undefined) -> New::Value)} |
    {check_timeslots} |
    {get_rrd, Process::atom(), Key::key(), SourcePid::comm:mypid()} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.

%% @doc Converts the given Key to avoid conflicts in erlang:put/get.
-spec to_internal_key(Process::atom(), Key::key()) -> internal_key().
to_internal_key(Process, Key) -> {'$monitor$', Process, Key}.

-spec check_report(Process::atom(), Key::key(), Old::Value, New::Value) -> ok.
check_report(Process, Key, OldValue, NewValue) ->
    case OldValue =:= undefined orelse NewValue =:= undefined of
        true -> ok;
        _ ->
            % check whether to report to the monitor
            % (always report if a new time slot was started)
            SlotOld = rrd:get_slot_start(0, OldValue),
            SlotNew = rrd:get_slot_start(0, NewValue),
            case SlotNew of
                SlotOld -> ok; %nothing to do
                _  -> % new slot -> report to monitor:
                    proc_report_to_my_monitor(Process, Key, OldValue, NewValue)
            end
    end.

%% @doc Keep track of the available keys by adding Key to the list of keys
%%      stored at '$monitor$:$keys$'.
-spec proc_add_to_keys_avail(OldValue::term() | undefined, Process::atom(), Key::key()) -> ok.
proc_add_to_keys_avail(undefined, Process, Key) ->
    AvailKey = '$monitor$:$keys$',
    OldKeys = case erlang:get(AvailKey) of
                  undefined -> [];
                  L -> L
              end,
    erlang:put(AvailKey, [{Process, Key} | OldKeys]),
    ok;
proc_add_to_keys_avail(_OldValue, _Process, _Key) ->
    ok.

%% @doc Sets the value at Key inside the current process.
%%      Either specify a new value or an update function which generates the
%%      new value from the old one.
%%      If a new time slot is started by updating the value, then the rrd()
%%      record is send to the monitor process.
-spec proc_set_value(Process::atom(), Key::key(),
                     NewValue_or_UpdateFun::term() | fun((Old::Value | undefined) -> New::Value)) -> ok.
proc_set_value(Process, Key, UpdateFun) when is_function(UpdateFun, 1) ->
    InternalKey = to_internal_key(Process, Key),
    OldValue = erlang:get(InternalKey),
    NewValue = UpdateFun(OldValue),
    proc_add_to_keys_avail(OldValue, Process, Key),
    check_report(Process, Key, OldValue, NewValue),
    erlang:put(InternalKey, NewValue);
proc_set_value(Process, Key, NewValue) ->
    InternalKey = to_internal_key(Process, Key),
    OldValue = erlang:put(InternalKey, NewValue),
    proc_add_to_keys_avail(OldValue, Process, Key),
    check_report(Process, Key, OldValue, NewValue).

%% @doc Sets the value at Key inside the monitor process of the current group.
%%      Either specify a new value or an update function which generates the
%%      new value from the old one.
-spec monitor_set_value(Process::atom(), Key::key(),
                        NewValue_or_UpdateFun::term() | fun((Old::Value | undefined) -> New::Value)) -> ok.
monitor_set_value(Process, Key, NewValue_or_UpdateFun) ->
    MyMonitor = pid_groups:get_my(monitor),
    comm:send_local(MyMonitor, {report_single, Process, Key, NewValue_or_UpdateFun}).

%% @doc Advances the stored timeslots of the value at Key inside the current
%%      process (if necessary) to the current time.
%%      If a new time slot is started by updating the value, then the rrd()
%%      record is send to the monitor process.
-spec proc_check_timeslot(Process::atom(), Key::key()) -> ok.
proc_check_timeslot(Process, Key) ->
    InternalKey = to_internal_key(Process, Key),
    OldValue = erlang:get(InternalKey),
    case OldValue of
        undefined -> ok;
        _ ->
            NewValue = rrd:check_timeslot_now(OldValue),
            check_report(Process, Key, OldValue, NewValue),
            erlang:put(InternalKey, NewValue)
    end.

%% @doc Advances the stored timeslots of the value at Key inside the current
%%      process (if necessary) to the current time.
%%      If a new time slot is started by updating the value, then the rrd()
%%      record is send to the monitor process.
-spec proc_check_all_timeslot() -> ok.
proc_check_all_timeslot() ->
    case erlang:get('$monitor$:$keys$') of
        undefined -> ok;
        AvailableKeys ->
            _ = [proc_check_timeslot(Process, Key) || {Process, Key} <- AvailableKeys],
            ok
    end.

%% @doc Sets the value at Key inside the monitor process of the "clients_group".
%%      Either specify a new value or an update function which generates the
%%      new value from the old one.
-spec client_monitor_set_value(Process::atom(), Key::key(),
                        NewValue_or_UpdateFun::term() | fun((Old::Value | undefined) -> New::Value)) -> ok.
client_monitor_set_value(Process, Key, NewValue_or_UpdateFun) ->
    MyMonitor = pid_groups:pid_of("clients_group", monitor),
    comm:send_local(MyMonitor, {report_single, Process, Key, NewValue_or_UpdateFun}).

%% @doc Checks whether a value exists at Key.
-spec proc_exists_value(Process::atom(), Key::key()) -> boolean().
proc_exists_value(Process, Key) ->
    erlang:get(to_internal_key(Process, Key)) =/= undefined.

%% @doc Gets the value stored at Key. The key must exist, otherwise no rrd()
%%      structure is returned!
-spec proc_get_value(Process::atom(), Key::key()) -> rrd:rrd().
proc_get_value(Process, Key) ->
    erlang:get(to_internal_key(Process, Key)).

%% @doc Reports the given value to the process' monitor process.
-spec proc_report_to_my_monitor(Process::atom(), Key::key(), OldValue::rrd:rrd(), Value::rrd:rrd()) -> ok.
proc_report_to_my_monitor(Process, Key, OldValue, Value) ->
    MyMonitor = pid_groups:get_my(monitor),
    % note: it may happen that the new value created a new slot which already
    % discarded all logged data from the previous (unreported) time slot
    % -> send OldValue, too
    comm:send_local(MyMonitor, {report_rrd, Process, Key, OldValue, Value}).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({report_rrd, Process, Key, OldValue, _NewValue}, Table) ->
    % note: reporting is always done when a new time slot is created
    % -> use the values from the old value
    TableIndex = {Process, Key},
    MyData = case ets:lookup(Table, TableIndex) of
                 [{TableIndex, X}] -> X;
                 [] ->
                     SlotLength = rrd:get_slot_length(OldValue),
                     OldTime = rrd:get_current_time(OldValue),
                     rrd:create(SlotLength, get_timeslots_to_keep(),
                                rrd:get_type(OldValue),
                                erlang:max(OldTime, OldTime - SlotLength))
             end,
    NewData = rrd:add_nonexisting_timeslots(MyData, OldValue),
    ets:insert(Table, {TableIndex, NewData}),
    Table;

on({report_single, Process, Key, NewValue_or_UpdateFun}, Table) ->
    proc_set_value(Process, Key, NewValue_or_UpdateFun),
    Table;

on({check_timeslots}, Table) ->
    proc_check_all_timeslot(),
    comm:send_local_after(get_check_timeslots_interval(), self(), {check_timeslots}),
    Table;

on({get_rrd, Process, Key, SourcePid}, Table) ->
    TableIndex = {Process, Key},
    MyData = case ets:lookup(Table, TableIndex) of
                 [{TableIndex, X}] -> X;
                 [] -> undefined
             end,
    comm:send(SourcePid, {get_rrd_response, MyData}),
    Table;

on({web_debug_info, Requestor}, Table) ->
    Keys = get_all_keys(Table),
    GroupedLast5 = [begin
                        KeyData5 = get_last_n(Table, Key, 5),
                        web_debug_info_merge_values(Key, KeyData5)
                    end || Key <- Keys],
    comm:send_local(Requestor, {web_debug_info_reply, [{"last 5 records per key:", ""} | GroupedLast5]}),
    Table.

-spec get_all_keys(Table::tid() | atom()) -> [table_index()].
get_all_keys(Table) ->
    lists:usort(ets:select(Table, [{ {'$1', '$2'},
                                     [],     % guard
                                     ['$1']} % result
                                  ])).

% @doc Reduces the rrd() data to N time slots (the key _must_ exist in the table!).
-spec get_last_n(Table::tid() | atom(), Key::table_index(), N::pos_integer())
        -> Value::rrd:rrd().
get_last_n(Table, Key, N) ->
    [{Key, Data}] = ets:lookup(Table, Key),
    rrd:reduce_timeslots(N, Data).

-spec web_debug_info_dump_fun(rrd:rrd(), From::util:time(), To::util:time(), Value::term())
        -> {From::util:time_utc(), To::util:time_utc(), Diff_in_s::non_neg_integer(), ValueStr::string()}.
web_debug_info_dump_fun(DB, From_, To_, Value) ->
    From = calendar:now_to_universal_time(From_),
    To = calendar:now_to_universal_time(To_),
    Diff_in_s = timer:now_diff(To_, From_) div 1000000,
    ValueStr =
        case rrd:get_type(DB) of
            timing  ->
                {Sum, Sum2, Count, Min, Max, Hist} = Value,
                AvgPerS = Count / Diff_in_s,
                Avg = Sum / Count, Avg2 = Sum2 / Count,
                Stddev = math:sqrt(Avg2 - (Avg * Avg)),
                io_lib:format("&nbsp;&nbsp;count: ~B (avg: ~.2f / s), avg: ~.2f ms, min: ~.2f ms, max: ~.2f ms, stddev: ~.2f ms<br />&nbsp;&nbsp;hist:~p",
                              [Count, AvgPerS, Avg / 1000, Min / 1000, Max / 1000, Stddev / 1000, histogram:get_data(Hist)]);
            counter ->
                io_lib:format("&nbsp;&nbsp;sum: ~p (avg: ~.2f / s)", [Value, Value / Diff_in_s]);
            event ->
                io_lib:format("&nbsp;&nbsp;events: ~p (avg: ~.2f / s)", [Value, length(Value) / Diff_in_s]);
            _       ->
                io_lib:format("&nbsp;&nbsp;~p", [Value])
        end,
    {From, To, Diff_in_s, lists:flatten(ValueStr)}.

-spec web_debug_info_merge_values(table_index(), rrd:rrd())
            -> {Key::string(), LastNValues::string()}.
web_debug_info_merge_values(Key, Data) ->
    ValuesLastN =
        [lists:flatten(io_lib:format("~p - ~p UTC (~p s):<br/>~s~n",
                                     [From, To, Diff_in_s, ValueStr]))
         || {From, To, Diff_in_s, ValueStr} <- rrd:dump_with(Data, fun web_debug_info_dump_fun/4)],
    {lists:flatten(io_lib:format("~p", [Key])), string:join(ValuesLastN, "<br />")}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the monitor process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, null,
                             [{pid_groups_join_as, DHTNodeGroup, monitor}]).

%% @doc Initialises the module with an empty state.
-spec init(null) -> state().
init(null) ->
    comm:send_local_after(get_check_timeslots_interval(), self(), {check_timeslots}),
    TableName = pid_groups:my_groupname() ++ ":monitor",
    ets:new(list_to_atom(TableName), [ordered_set, protected]).

%% @doc Checks whether config parameters of the monitor process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(monitor_timeslots_to_keep) and
    config:is_greater_than(monitor_timeslots_to_keep, 0).

-spec get_timeslots_to_keep() -> pos_integer().
get_timeslots_to_keep() ->
    config:read(monitor_timeslots_to_keep).

-spec get_check_timeslots_interval() -> 10000.
get_check_timeslots_interval() ->
    10 * 1000. % every 10s
