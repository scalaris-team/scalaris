% @copyright 2013 Zuse Institute Berlin

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

%% @author Ufuk Celebi <celebi@zib.de>
%% @doc Auto-scaling service. 
%%      {autoscale, true} in scalaris.local.cfg will enable this service
%%
%%      Alarms can be configured through
%%      {autoscale_alarms, [{alarm, AlarmName, AlarmHandler, TimeCheckInterval,
%%                                  MinValue, MaxValue, VMsToRemove, VMsToAdd,
%%                                  active|inactive, [Options]}, ...]}.
%%      Example:
%%      {autoscale_alarms, [{alarm, load_alarm, load_avg, 10, 30, 10, 40, 1, 1, inactive, []},
%%                          {alarm, churn, random_churn, 10, 0, 0, 50, 3, 5, inactive, []},
%%                          {alarm, lat, latency_avg, 60, 0, 100, 250, 2, 2, inactive, []}]}.
%%
%%      The cloud module which is called to remove or add VMs can be configured using
%%      {autoscale_cloud_module, CloudModule}.
%%      Available modules are: cloud_local and cloud_ssh which implement cloud_beh
%% @end
%% @version $Id$
-module(autoscale).
-author('celebi@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-define(CLOUD, (config:read(autoscale_cloud_module))).

-export([start_link/1, init/1, on/2]).
-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type(alarm_handler() ::
      load_avg |
      latency_avg |
      random_churn).

-record(alarm,
    {
        name             = ?required(alarm, name) :: atom(),
        handler          = ?required(alarm, handler) :: alarm_handler(),
        period_secs      = ?required(alarm, period_in_s) :: pos_integer(),
        cooldown_secs    = 0 :: non_neg_integer(),
        lower_limit      = ?MINUS_INFINITY :: number(),
        upper_limit      = ?PLUS_INFINITY :: number(),
        scale_down_by    = 1 :: non_neg_integer(),
        scale_up_by      = 1 :: non_neg_integer(),
        state             = active :: active | inactive,
        opt_fields       = [] :: [{atom(), any()}]
    }).

-type(state() ::
    {IsLeader::boolean(), Alarms::dict()} |
    unknown_event).

-type(breach_state() ::
    breach_lower |
    breach_upper |
    ok).

-type(message() ::
    {get_state_response, MyRange::intervals:interval()} |
    {check_alarm, Name::atom()} |
    {toggle_alarm, Name::atom()} |
    {deactivate_alarms, Name::atom()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(DHTNodeGroup::pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, null,
                             [{pid_groups_join_as, DHTNodeGroup, autoscale}]).

-spec init(null) -> state().
init(null) ->
    ?CLOUD:init(),
    % dict with alarm name as key and {alarm record, epoch} as value 
    Alarms = dict:from_list(lists:map(fun(A) -> {A#alarm.name, {A, 0}} end, read_alarms_from_config())),
    % check leadership and subscribe to direct neighborhood changes
    check_leadership(),
    rm_loop:subscribe(self(), autoscale,
                      fun rm_loop:subscribe_dneighbor_change_filter/3,
                      fun autoscale:check_leadership/4,
                      inf),
    % initial state: not leader and alarms from config
    {_IsLeader = false, Alarms}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% message handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% leadership maintenance %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().
on({get_state_response, MyRange}, {IsLeader, Alarms}) ->
    %% @fix: dbrange vs myrange?
    IsNewLeader = intervals:in(?RT:hash_key("0"), MyRange),
    NewAlarms = 
        case {IsLeader, IsNewLeader} of
            {false, true} ->
                dict:map(fun(Name, {Alarm, Epoch}) ->
                             case Alarm#alarm.state =:= active of                                     
                                 true ->
                                     continue_alarm(Name, Alarm#alarm.period_secs,
                                                    self(), Epoch+1);
                                 false ->
                                     ok
                             end,
                             {Alarm, Epoch+1}
                         end, Alarms);
            {_, _}        -> Alarms
        end,
    {IsNewLeader, NewAlarms};

%% alarm maintenance %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({check_alarm, Name, AlarmEpoch}, {IsLeader, Alarms}) -> 
    {Alarm, Epoch} =
        case dict:find(Name, Alarms) of
            {ok, Value} -> Value;
            error       -> {unknown_alarm, 0}
        end,
    case (Alarm =/= unknown_alarm) andalso (Alarm#alarm.state =:= active) andalso
             IsLeader andalso (Epoch =:= AlarmEpoch) of
        true ->
            % call alarm handler and react to breach_state 
            _ = case check_alarm(Alarm#alarm.handler, Alarm) of
                    unknown_alarm_handler ->
                        ok;
                    breach_lower ->
                        continue_alarm(Name,
                            erlang:max(Alarm#alarm.period_secs, Alarm#alarm.cooldown_secs),
                            self(), Epoch),
                        ?CLOUD:remove_vms(Alarm#alarm.scale_down_by);
                    breach_upper ->
                        continue_alarm(Name,
                            erlang:max(Alarm#alarm.period_secs, Alarm#alarm.cooldown_secs),
                            self(), Epoch),
                        ?CLOUD:add_vms(Alarm#alarm.scale_up_by);
                    ok ->
                        continue_alarm(Name, Alarm#alarm.period_secs, self(), Epoch)
                end,
            ok;
        false ->
            ok
    end,
    {IsLeader, Alarms};
on({toggle_alarm, Name}, {_IsLeader, Alarms}) ->
    NewAlarms = 
        case dict:find(Name, Alarms) of
            error                -> Alarms;
            {ok, {Alarm, Epoch}} ->
                case Alarm#alarm.state of
                    active ->
                        update_alarm(Alarms, Name, [{state, inactive}]);
                    inactive ->
                        % update state and epoch in dict
                        continue_alarm(Name, Alarm#alarm.period_secs, self(), Epoch+1),
                        update_alarm(Alarms, Name, [{state, active}, {epoch, Epoch+1}])
                end
        end,
    {_IsLeader, NewAlarms};
on({deactivate_alarms}, {_IsLeader, Alarms}) ->
    AlarmNames = dict:fetch_keys(Alarms),
    NewAlarms = lists:foldl(
                    fun(Name, AlarmsAcc) ->
                        update_alarm(AlarmsAcc, Name, {state, inactive})
                    end, Alarms, AlarmNames),
    {_IsLeader, NewAlarms};
on(_, _) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% alarm handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_alarm(alarm_handler(), #alarm{}) -> breach_state() | unknown_alarm_handler.
check_alarm(load_avg, Alarm) ->
    AvgLoad = statistics:get_average_load(statistics:get_ring_details()),
    get_breach_state(AvgLoad, Alarm);
check_alarm(latency_avg, Alarm) ->
    Monitor = pid_groups:find_a(monitor_perf),
    {_CountD, _CountPerSD, AvgMsD, _MinMsD, _MaxMsD, _StddevMsD, _HistMsD} =
        case statistics:getTimingMonitorStats(Monitor, [{api_tx, 'req_list'}], tuple) of
            []                           -> {[], [], [], [], [], [], []};
            [{api_tx, 'req_list', Data}] -> Data
        end,
    History = get_opt_field(Alarm, history, 1),
    [Hd|Tail] = lists:map(fun({_, V}) -> get_breach_state(V, Alarm) end,
                          lists:sublist(AvgMsD, History)),
    ?IIF(lists:all(fun(X) -> X =:= Hd end, Tail), Hd, ok);
check_alarm(random_churn, _Alarm) ->
    case randoms:rand_uniform(1,4) of
        1 -> breach_upper;
        2 -> breach_lower;
        3 -> ok
    end;
check_alarm(_, _) ->
    unknown_alarm_handler.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% alarm helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec continue_alarm(atom(), pos_integer(), pid(), non_neg_integer()) -> ok.
continue_alarm(Name, Secs, Pid, Epoch) ->
    %% io:format("Alarm '~p' for ~p delayed by ~p seconds (Epoch ~p).~n", [Name, Pid, Secs, Epoch]),
    msg_delay:send_local(Secs, Pid, {check_alarm, Name, Epoch}).

update_alarm(Alarms, Name, {Field, NewValue}) ->
    update_alarm(Alarms, Name, [{Field, NewValue}]);
update_alarm(Alarms, Name, [{Field, NewValue}|Tail]) ->
    NewAlarms =
        case Field =:= epoch of
            true ->
                % update epoch
                dict:update(Name, fun({_Alarm, _Epoch}) -> {_Alarm, NewValue} end, Alarms);
            false ->
                % update record
                FieldIndex =
                    case Field of
                        period_secs   -> 4;
                        cooldown_secs -> 5;
                        lower_limit   -> 6;
                        upper_limit   -> 7;
                        scale_down_by -> 8;
                        scale_up_by   -> 9;
                        state         -> 10;
                        _             -> -1
                    end,
                dict:update(Name,
                            fun({Alarm, Epoch}) ->
                                {erlang:setelement(FieldIndex, Alarm, NewValue), Epoch}
                            end,
                            Alarms)
        end,
    update_alarm(NewAlarms, Name, Tail);
update_alarm(Alarms, _Name, []) ->
    Alarms.

-spec get_opt_field(#alarm{}, atom(), any()) -> false | any().
get_opt_field(Alarm, Field, Default) ->
    case lists:keyfind(Field, 1, Alarm#alarm.opt_fields) of
        false          -> Default;
        {Field, Value} -> Value
    end.

-spec get_breach_state(term(), #alarm{}) -> breach_state().
get_breach_state(Value, Alarm) ->
    ?IIF(Value < Alarm#alarm.lower_limit, breach_lower,
         ?IIF(Value > Alarm#alarm.upper_limit, breach_upper, ok)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% misc helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_leadership() -> ok.
check_leadership() ->
    DhtNode = pid_groups:get_my(dht_node),
    comm:send_local(DhtNode, {get_state, comm:this(), my_range}).

-spec check_leadership(pid(), autoscale, nodelist:neighborhood(), nodelist:neighborhood()) -> ok.
check_leadership(Pid, autoscale, _, _) ->
    GrpName = pid_groups:group_of(Pid),
    DhtNode = pid_groups:pid_of(GrpName, dht_node),
    comm:send_local(DhtNode, {get_state, comm:make_global(Pid), my_range}).

-spec get_timestamp_secs() -> pos_integer().
get_timestamp_secs() ->
    get_timestamp_secs(erlang:now()).

-spec get_timestamp_secs(erlang:timestamp()) -> pos_integer().
get_timestamp_secs({MegaSecs, Secs, _}) ->
    MegaSecs*1000000+Secs.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec read_alarms_from_config() -> [#alarm{}].
read_alarms_from_config() ->
    case Alarms = config:read(autoscale_alarms) of
        failed -> [];
        _      -> Alarms
    end.
