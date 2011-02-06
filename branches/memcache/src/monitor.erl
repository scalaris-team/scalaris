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

-export([start_link/1, init/1, on/2, check_config/0]).
-export([proc_init/1, proc_set_value/3, proc_get_value/2,
         proc_inc_value/2, proc_inc_value/3,
         proc_report_to_my_monitor/1,
         proc_get_report_interval/0]).

-type key() :: string().
-type internal_key() :: {'$monitor$', Key::string()}.
-type table_index() :: {ProcTable::atom(), Key::key()}.

-type state() :: Table::tid() | atom().
-type message() ::
    {proc_report, ProcTable::pdb:tableid(), LastReport::util:time(),
     CurrentReport::util:time(), KVList::[{key(), term()}]} |
    {purge_old_data} |
    {web_debug_info, Requestor::comm:erl_local_pid()}.


%% @doc Initialises a process' monitor DB. Needs to be called prior to the
%%      other proc_* methods! 
%%      Beware: there is only one monitor table available per process if pdb
%%      uses erlang:put/get!
-spec proc_init(Process::atom()) -> pdb:tableid().
proc_init(Process) ->
    % note: make table name unique if named_table is used
    Table = pdb:new(Process, [ordered_set, protected]),
    pdb:set({'$monitor$:$last_report$', erlang:now()}, Table),
    Table.

%% @doc Converts the given Key avoid conflicts in case erlang:put/get is used
%%      by pdb.
-spec to_internal_key(Key::key()) -> internal_key().
to_internal_key(Key) -> {'$monitor$', Key}.

%% @doc Sets the value at Key.
-spec proc_set_value(Table::pdb:tableid(), Key::key(), Value::term()) -> ok.
proc_set_value(Table, Key, Value) ->
    InternalKey = to_internal_key(Key),
    pdb:set({InternalKey, Value}, Table),
    proc_add_to_keys_avail(Table, Key).

%% @doc Keep track of the available keys by adding Key to the list of keys
%%      stored at '$monitor$:$keys$'.
-spec proc_add_to_keys_avail(Table::pdb:tableid(), Key::key()) -> ok.
proc_add_to_keys_avail(Table, Key) ->
    case pdb:get('$monitor$:$keys$', Table) of
        undefined -> pdb:set({'$monitor$:$keys$', [Key]}, Table);
        {'$monitor$:$keys$', Keys} when is_list(Keys) ->
            pdb:set({'$monitor$:$keys$', [Key | Keys]}, Table)
    end.

%% @doc Gets the value stored at Key.
-spec proc_get_value(Table::pdb:tableid(), Key::key()) -> term() | undefined.
proc_get_value(Table, Key) -> proc_get_value_(Table, to_internal_key(Key)).

%% @doc Gets the value stored at Key.
-spec proc_get_value_(Table::pdb:tableid(), Key::internal_key()) -> term() | undefined.
proc_get_value_(Table, InternalKey) ->
    case pdb:get(InternalKey, Table) of
        undefined    -> undefined;
        {InternalKey, Value} -> Value
    end.

%% @doc Increases the value stored at Key by 1. If there is no such value, 1
%%      is stored.
-spec proc_inc_value(Table::pdb:tableid(), Key::key()) -> ok.
proc_inc_value(Table, Key) -> proc_inc_value(Table, Key, 1).

%% @doc Increases the value stored at Key by Inc. If there is no such value,
%%      Inc is stored. Note: Inc may be negative in order to decrease values.
%%      If the stored value is no number, unsupported is returned and nothing
%%      is changed.
-spec proc_inc_value(Table::pdb:tableid(), Key::key(), Inc::number()) -> ok | unsupported.
proc_inc_value(Table, Key, Inc) ->
    InternalKey = to_internal_key(Key),
    case pdb:get(InternalKey, Table) of
        undefined ->
            pdb:set({InternalKey, Inc}, Table),
            proc_add_to_keys_avail(Table, Key);
        {InternalKey, X} when is_number(X) ->
            pdb:set({InternalKey, X + Inc}, Table);
        _ -> unsupported
    end.

%% @doc Reports all collected Key/Value pairs to the process' monitor process
%%      and resets the values and the list of stored keys.
-spec proc_report_to_my_monitor(Table::pdb:tableid()) -> ok.
proc_report_to_my_monitor(Table) ->
    MyMonitor = pid_groups:get_my(monitor),
    % note: since proc_init/1 was called before, this value is present and valid:
    LastReport = erlang:element(2, pdb:get('$monitor$:$last_report$', Table)),
    Now = erlang:now(),
    case pdb:get('$monitor$:$keys$', Table) of
        undefined ->
            comm:send_local(MyMonitor, {proc_report, Table, LastReport, Now, []});
        {'$monitor$:$keys$', Keys} when is_list(Keys) ->
            UKeys = lists:usort(Keys), % may not be unique
            KVList = [begin
                          InternalKey = to_internal_key(Key),
                          Value = proc_get_value_(Table, InternalKey),
                          pdb:delete(InternalKey, Table),
                          {Key, Value}
                      end || Key <- UKeys],
            pdb:delete('$monitor$:$keys$', Table),
            comm:send_local(MyMonitor, {proc_report, Table, LastReport, Now, KVList})
    end,
    pdb:set({'$monitor$:$last_report$', Now}, Table),
    ok.

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({proc_report, ProcTable, LastReport, CurrentReport, KVList}, Table) ->
    Data = lists:flatten(
             [{{ProcTable, Key}, LastReport, CurrentReport, Value} || {Key, Value} <- KVList]),
    ets:insert(Table, Data),
    Table;

on({purge_old_data}, Table) ->
    Keys = get_all_keys(Table),
    _ = [begin
             Data = ets:lookup(Table, Key),
             L = erlang:length(Data),
             MaxPerKey = get_max_values_per_key(),
             case L =< MaxPerKey of
                 true -> ok;
                 _ ->
                     TailStart = L - get_max_values_per_key(),
                     NewData = lists:nthtail(TailStart, Data),
                     ets:delete(Table, Key),
                     ets:insert(Table, NewData)
             end
         end || Key <- Keys],
    msg_delay:send_local(get_purge_old_data_interval(), self(), {purge_old_data}),
    Table;

on({web_debug_info, Requestor}, Table) ->
    Keys = get_all_keys(Table),
    GroupedLast5 = [begin
                        KeyDataN = get_last_n(Table, Key, 5),
                        web_debug_info_merge_values(Key, KeyDataN)
                    end || Key <- Keys],
    comm:send_local(Requestor, {web_debug_info_reply, GroupedLast5}),
    Table.

-spec get_all_keys(Table::tid() | atom()) -> [{ProcTable::atom(), Key::key()}].
get_all_keys(Table) ->
    lists:usort(ets:select(Table, [{ {'$1', '$2', '$3', '$4'},
                                     [],     % guard
                                     ['$1']} % result
                                  ])).

-spec get_last_n(Table::tid() | atom(), table_index(), N::pos_integer())
        -> [{table_index(), LastReport::util:time(), CurrentReport::util:time(), Value::term()}].
get_last_n(Table, Key, N) ->
    Data = ets:lookup(Table, Key),
    L = erlang:length(Data),
    lists:nthtail(erlang:max(0, L - N), Data).

-spec web_debug_info_merge_values(table_index(),
        [{table_index(), LastReport::util:time(), CurrentReport::util:time(), Value::term()}])
            -> {Key::string(), LastNValues::string()}.
web_debug_info_merge_values(Key, Data) ->
    ValuesLastN = 
        [begin
             Avg = case is_number(Value) of
                       false -> "n/a";
                       _ -> (Value / timer:now_diff(Time, PrevTime)) * 1000000
                   end,
             lists:flatten(
               io_lib:format(
                 "~p UTC: ~p (avg: ~p / s)",
                 [calendar:now_to_universal_time(Time), Value, Avg]))
         end
        || {_, PrevTime, Time, Value} <- Data],
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
    TableName = string:concat(pid_groups:my_groupname(), ":monitor"),
    msg_delay:send_local(get_purge_old_data_interval(), self(), {purge_old_data}),
    ets:new(list_to_atom(TableName), [bag, protected]).

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(monitor_proc_report_interval) and
    config:is_greater_than(monitor_proc_report_interval, 0) and

    config:is_integer(monitor_purge_old_data_interval) and
    config:is_greater_than(monitor_purge_old_data_interval, 0) and

    config:is_integer(monitor_max_values_per_key) and
    config:is_greater_than(monitor_max_values_per_key, 0).

-spec proc_get_report_interval() -> Seconds::pos_integer().
proc_get_report_interval() ->
    config:read(monitor_proc_report_interval).

-spec get_purge_old_data_interval() -> Seconds::pos_integer().
get_purge_old_data_interval() ->
    config:read(monitor_purge_old_data_interval).

-spec get_max_values_per_key() -> pos_integer().
get_max_values_per_key() ->
    config:read(monitor_max_values_per_key).
