% @copyright 2013-2015 Zuse Institute Berlin

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
%%
%%      {autoscale, true} in scalaris.local.cfg will enable this service.
%%
%%      The main task of the autoscale is to manage a set of installed alarms
%%      by calling their alarm handlers periodically. Every alarm handler can
%%      request to add or remove VMs (see alarm_handler/2).
%%
%%      Alarms are read from the config as a list of alarm records:
%%      {autoscale_alarms, [{alarm, Name (req.), Options (opt.),
%%                                  Interval (opt.), Cooldown (opt.),
%%                                  State (opt.)}]}.
%%
%%      Every provided tuple corresponds to the alarm record type:
%%        - Name:     the name of the alarm handler (should be unique)
%%        - Options:  options are alarm handler specific (see alarm_handler/2)
%%        - Interval: interval between alarm checks in seconds
%%        - Cooldown: interval after a scale request
%%        - State:    active or inactive
%%
%%      Example:
%%      {autoscale_alarms, [{alarm, rand_churn, [{lower_limit, -3}, {upper_limit, 10}], 60, 0, inactive},
%%                          {alarm, lat_avg, [], 60, 120, inactive}]}.
%%
%%      In this example, the rand_churn alarm handler is called every 60
%%      seconds and configured with the options lower_limit and upper_limit.
%%      The lat_avg alarm handler is called every 60 seconds and falls
%%      back to the default options (set in the alarm handler), because no
%%      options are provided. If the lat_avg handler requested to add or remove
%%      VMs, the handler will be called after 120 seconds in order to allow the
%%      changes to take effect (cooldown).
%%
%%      VMs requests are handled by the cloud module. It can be configured with
%%        {autoscale_cloud_module, CloudModule}.
%%
%%      Possible modules are: cloud_local, cloud_ssh, and cloud_cps. For both
%%      cloud_local and cloud_ssh, scale requests are pushed by autoscale, for
%%      cloud_cps, autoscale expects pulling of requests.
%%
%%      cloud_cps is a dummy and not actually implemented as a module. Instead,
%%      the manager of the ConPaaS scalaris service needs to pull the current
%%      request via the JSON API.
%% @end
%% @version $Id$
-module(autoscale).
-author('celebi@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("record_helpers.hrl").
-include("scalaris.hrl").

%% gen_component functions
-export([start_link/1, init/1, on/2]).
%% rm_loop interaction
-export([send_my_range_req/4]).
%% misc (check_config for api_autoscale)
-export([check_config/0]).

%% rm subscription
-export([rm_exec/5]).

-define(TRACE1(X), ?TRACE(X, [])).
%% -define(TRACE(X,Y), io:format("as: " ++ X ++ "~n",Y)).
-define(TRACE(_X,_Y), ok).

-define(CLOUD, (config:read(autoscale_cloud_module))).
-define(AUTOSCALE_TX_KEY, "d9c966df633f8b1577eacff013166db95917a7002999b6fbb").
-define(DEFAULT_LOCK_TIMEOUT, 60).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type key_value_list() :: [{Key :: atom(), Value :: term()}].

-record(alarm, {name     = ?required(alarm, name) :: atom(),
                options  = []                     :: key_value_list(),
                interval = 60                     :: pos_integer(),
                cooldown = 90                     :: non_neg_integer(),
                state    = inactive               :: active | inactive }).

-record(scale_req, {req   = 0        :: integer(),
                    lock  = unlocked :: locked | unlocked,
                    mode  = push     :: push | pull}).

-type alarm()     :: #alarm{}.
-type alarms()    :: [alarm()].
-type scale_req() :: #scale_req{}.
-type triggers()  :: [{Name :: atom(), Trigger :: reference() | ok}].

-type(api_message() ::
          {pull_scale_req, Pid :: pid()} |
          {unlock_scale_req, Pid :: pid()} |
          {toggle_alarm, Name :: atom(), Pid :: pid()} |
          {activate_alarms, Pid :: pid()} |
          {deactivate_alarms, Pid :: pid()}).

-type(message() ::
          {get_state_response, MyRange :: intervals:interval()} |
          {check_alarm, Name :: atom()} |
          {push_scale_req} |
          api_message()).

-type state() :: {IsLeader :: boolean(),
                  Alarms   :: alarms(),
                  ScaleReq :: scale_req(),
                  Triggers :: triggers()}.

-export_type([key_value_list/0]).

-include("gen_component.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Alarm handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Check average latency measured by monitor_perf and request scaling if
%%      average latency is out of provided bounds.
%%      Options:
%%        lower_limit_ms: lowest avg. lat. to tolerate before scaling down
%%        upper_limit_ms: highest avg. lat. to tolerate before scaling up
%%        vms_to_remove: number of vms to add, when avg. lat. &lt; lower_limit_ms
%%        vms_to_add: number of vms to add, when avg. lat. &gt; upper_limit_ms
%% @todo add history option (-> don't check only latest avg lat)
alarm_handler(lat_avg, Options) ->
    LoMs        = get_alarm_option(Options, lower_limit_ms, 1000),
    HiMs        = get_alarm_option(Options, upper_limit_ms, 2000),
    VmsToRemove = get_alarm_option(Options, vms_to_remove, 1),
    VmsToAdd    = get_alarm_option(Options, vms_to_add, 1),

    Monitor = pid_groups:pid_of(basic_services, monitor),
    {_CountD, _CountPerSD, AvgMsD, _MinMsD, _MaxMsD, _StddevMsD, _HistMsD} =
        case statistics:getTimingMonitorStats(Monitor, [{api_tx, 'agg_req_list'}], tuple) of
            []                           -> {[], [], [], [], [], [], []};
            [{api_tx, 'agg_req_list', Data}] -> Data
        end,
    [{_, LatestAvgMsD}|_] = AvgMsD,

    log(lat_avg, LatestAvgMsD),

    if
        LatestAvgMsD < LoMs ->
            -VmsToRemove;
        LatestAvgMsD > HiMs ->
            +VmsToAdd;
        true ->
            0
    end;

%% @doc Pick a number UAR and add or remove VMs by that amount.
%%      Options:
%%        lower_limit: max. number of vms to remove
%%        upper_limit: max. number of vms to add
alarm_handler(rand_churn, Options) ->
    Lo = get_alarm_option(Options, lower_limit, -5),
    Hi = get_alarm_option(Options, upper_limit, 5),
    Churn = randoms:rand_uniform(Lo, Hi+1),

    log(rand_churn, Churn),

    Churn.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Msg loop: main functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(message(), state()) -> state().

%% @doc Set new leader state at startup and ring changes.
on({get_state_response, MyRange}, {IsLeader, Alarms, ScaleReq, Triggers}) ->
    IsNewLeader = intervals:in(?RT:hash_key("0"), MyRange),

    {NewAlarms, NewScaleReq, NewTriggers} =
        case {IsLeader, IsNewLeader} of
            {false, true} ->
                {tx_merge_alarms(Alarms),
                 ScaleReq#scale_req{lock = unlocked, req = 0},
                 % init triggers for alarms
                 lists:map(
                   fun(Alarm) ->
                           {Alarm#alarm.name,
                            next(Alarm#alarm.name, Alarm#alarm.interval)}
                   end, Alarms)};
            {true, false} ->
                {Alarms,
                 ScaleReq#scale_req{lock = unlocked, req = 0},
                 % cancel all triggers
                 lists:map(fun({Name, Trigger}) -> {Name, cancel(Trigger)} end,
                           Triggers)};
            {_, _}        ->
                {Alarms, ScaleReq, Triggers}
        end,

    ?TRACE("leadership: ~p", [{IsLeader, IsNewLeader}]),
    {IsNewLeader, NewAlarms, NewScaleReq, NewTriggers};

%% @doc Check alarm Name for new scale request.
on({check_alarm, Name}, {IsLeader, Alarms, ScaleReq, Triggers})
  when IsLeader andalso ScaleReq#scale_req.lock =:= unlocked  ->
    {ok, Alarm} = get_alarm(Name, Alarms),

    % log current number of vms
    log_vms(),

    % call alarm handler
    NewReq =
        case Alarm#alarm.state of
            active   -> alarm_handler(Name, Alarm#alarm.options);
            inactive -> ScaleReq#scale_req.req
        end,

    % delay for next check (cooldown if scale req /= 0)
    NewDelay =
        case NewReq == 0 of
            true  -> Alarm#alarm.interval;
            false -> erlang:max(Alarm#alarm.interval, Alarm#alarm.cooldown)
        end,

    % push request to cloud module (except cloud_cps)
    case ScaleReq#scale_req.mode of
        push -> comm:send_local(self(), {push_scale_req});
        pull -> ok
    end,

    ?TRACE("check_alarm: ~p, ~p new vms requested, ~p s delayed",
           [Name, NewReq, NewDelay]),
    {IsLeader, Alarms, ScaleReq#scale_req{req = NewReq},
     next(Name, NewDelay, Triggers)};

on({check_alarm, Name}, {IsLeader, Alarms, ScaleReq, Triggers})
  when IsLeader andalso ScaleReq#scale_req.lock =:= locked  ->
    {ok, Alarm} = get_alarm(Name, Alarms),

    ?TRACE("check_alarm: ~p, scale_req is locked", [Name]),
    {IsLeader, Alarms, ScaleReq, next(Name, Alarm#alarm.interval, Triggers)};

on({push_scale_req}, {IsLeader, _Alarms, ScaleReq, _Triggers})
  when IsLeader andalso ScaleReq#scale_req.lock =:= unlocked ->

    Req = ScaleReq#scale_req.req,
    if
        Req > 0  -> ?CLOUD:add_vms(Req);
        Req < 0  -> ?CLOUD:remove_vms(Req*-1);
        Req == 0 -> ok
    end,

    ?TRACE("push_scale_req: ~p", [ScaleReq]),
    {IsLeader, _Alarms, ScaleReq#scale_req{req = 0}, _Triggers};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Msg loop: API msgs -> scale req polling API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({pull_scale_req, Pid}, State = {_IsLeader, _Alarms, ScaleReq, _Triggers}) ->
    comm:send(Pid, {scale_req_resp, {ok, ScaleReq#scale_req.req}}),
    ?TRACE("pull_scale_req,~nscale_req: ~p", [ScaleReq]),
    State;

on({lock_scale_req, Pid}, State = {_IsLeader, _Alarms, ScaleReq, _Triggers})
  when ScaleReq#scale_req.lock =:= locked ->
    comm:send(Pid, {scale_req_resp, {error, locked}}),
    ?TRACE("lock_scale_req,~nscale_req: ~p", [ScaleReq]),
    State;
on({lock_scale_req, Pid}, {_IsLeader, _Alarms, ScaleReq, Triggers})
  when ScaleReq#scale_req.lock =:= unlocked ->
    comm:send(Pid, {scale_req_resp, ok}),
    % start timeout @todo add timeout to cofig
    Trigger = comm:send_local_after(get_lock_timeout()*1000, self(),
                                    {unlock_scale_req_timeout}),
    ?TRACE("lock_scale_req,~nscale_req: ~p", [ScaleReq]),
    {_IsLeader, _Alarms, ScaleReq#scale_req{lock = locked},
                         Triggers ++ [{timeout, Trigger}]};

on({unlock_scale_req, Pid}, {_IsLeader, _Alarms, ScaleReq, Triggers})
  when ScaleReq#scale_req.lock =:= locked ->
    comm:send(Pid, {scale_req_resp, ok}),
    % cancel timeout
    {value, {timeout, Trigger}, NewTriggers} = lists:keytake(
                                                 timeout, 1, Triggers),
    cancel(Trigger),
    ?TRACE("unlock_scale_req,~nscale_req: ~p", [ScaleReq]),
    {_IsLeader, _Alarms, ScaleReq#scale_req{lock = unlocked, req  = 0},
                         NewTriggers};
on({unlock_scale_req, Pid}, State = {_IsLeader, _Alarms, ScaleReq, _Triggers})
  when ScaleReq#scale_req.lock =:= unlocked ->
    comm:send(Pid, {scale_req_resp, {error, not_locked}}),
    ?TRACE("unlock_scale_req,~nscale_req: ~p", [ScaleReq]),
    State;

on({unlock_scale_req_timeout}, {_IsLeader, _Alarms, ScaleReq, Triggers}) ->
    ?TRACE1("unlock_scale_req_timeout"),
    {_IsLeader, _Alarms, ScaleReq#scale_req{lock = unlocked},
                         lists:keydelete(timeout, 1, Triggers)};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Msg loop: API msgs -> alarm state API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({toggle_alarm, Name, Pid}, {_IsLeader, Alarms, _ScaleReq, _Triggers}) ->
    NewAlarms =
       case get_alarm(Name, Alarms) of
            {error, unknown_alarm} ->
                comm:send(Pid, {toggle_alarm_resp, {error, unknown_alarm}}),
                Alarms;
            {ok, Alarm} ->
                Toggled = case Alarm#alarm.state of
                              active   -> Alarm#alarm{state = inactive};
                              inactive -> Alarm#alarm{state = active}
                          end,
                case tx_update_alarms([Toggled], [Alarm]) of
                    ok   ->
                        comm:send(Pid, {toggle_alarm_resp,
                                   {ok, {new_state, Toggled#alarm.state}}}),
                        update_alarm(Toggled, Alarms);
                    fail ->
                        comm:send(Pid, {toggle_alarm_resp, {error, tx_fail}}),
                        Alarms
                end
        end,
    ?TRACE("alarms: ~p, ~nnewalarms: ~p", [Alarms, NewAlarms]),
    {_IsLeader, NewAlarms, _ScaleReq, _Triggers};

on({activate_alarms, Pid}, {_IsLeader, Alarms, _ScaleReq, _Triggers}) ->
    AllActive = lists:map(fun(Alarm) -> Alarm#alarm{state = active} end,
                          Alarms),
    NewAlarms =
        case tx_update_alarms(AllActive, Alarms) of
            ok ->
                comm:send(Pid, {activate_alarms_resp, ok}),
                AllActive;
            fail ->
                comm:send(Pid, {activate_alarms_resp, {error, tx_fail}}),
                Alarms
        end,
    {_IsLeader, NewAlarms, _ScaleReq, _Triggers};

on({deactivate_alarms, Pid}, {_IsLeader, Alarms, _ScaleReq, _Triggers}) ->
    AllInactive = lists:map(fun(Alarm) -> Alarm#alarm{state = inactive} end,
                            Alarms),
    NewAlarms =
        case tx_update_alarms(AllInactive, Alarms) of
            ok ->
                comm:send(Pid, {deactivate_alarms_resp, ok}),
                AllInactive;
            fail ->
                comm:send(Pid, {deactivate_alarms_resp, {error, tx_fail}}),
                Alarms
        end,
    {_IsLeader, NewAlarms, _ScaleReq, _Triggers};

on({update_alarm, Name, NewOptions, Pid},
   {_IsLeader, Alarms, _ScaleReq, _Triggers}) ->
    NewAlarms =
        case get_alarm(Name, Alarms) of
            {error, unknown_alarm} ->
                comm:send(Pid, {update_alarm_resp, {error, unknown_alarm}}),
                Alarms;
            {ok, Alarm} ->
                UpdatedOptions = lists:foldl(
                                   fun(Option = {Key, _Value}, Acc) ->
                                           lists:keystore(Key, 1, Acc, Option)
                                   end, Alarm#alarm.options, NewOptions),
                UpdatedAlarm = Alarm#alarm{options=UpdatedOptions},
                case tx_update_alarms(UpdatedAlarm, Alarm) of
                    ok   ->
                        comm:send(Pid, {update_alarm_resp,
                                   {ok, {new_options, UpdatedOptions}}}),
                        update_alarm(UpdatedAlarm, Alarms);
                    fail ->
                        comm:send(Pid, {update_alarm_resp, {error, tx_fail}}),
                        Alarms
                end
        end,
    {_IsLeader, NewAlarms, _ScaleReq, _Triggers};

on(_Msg, State) ->
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Triggers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Check alarm Name in Delay seconds.
-spec next(Name :: atom(), Delay :: pos_integer()) -> reference().
next(Name, Delay) ->
    comm:send_local_after(Delay*1000, self(), {check_alarm, Name}).

%% @doc Check alarm Name from trigger list in Delay seconds.
-spec next(Name :: atom(), Delay :: pos_integer(), Triggers :: triggers()) ->
          triggers().
next(Name, Delay, Triggers) ->
    lists:keyreplace(Name, 1, Triggers, {Name, next(Name, Delay)}).

%% @doc Cancel timer of trigger.
-spec cancel(Trigger :: reference() | ok) -> ok.
cancel(Trigger) ->
    _ = erlang:cancel_timer(Trigger),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Alarm helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Get alarm by name.
-spec get_alarm(Name :: atom(), Alarms :: alarms()) -> {ok, alarm()} |
                                                       {error, unknown_alarm}.
get_alarm(Name, Alarms) ->
    case Alarm = lists:keyfind(Name, 2, Alarms) of
        false  -> {error, unknown_alarm};
        _Found -> {ok, Alarm}
    end.

%% @doc Get value for specified option key from key-value list. Return provided
%%      default value, if key does not exist.
-spec get_alarm_option(Options :: key_value_list(), Key :: atom(),
                       Default :: any()) -> any().
get_alarm_option(Options, FieldKey, Default) ->
    case lists:keyfind(FieldKey, 1, Options) of
        {FieldKey, Value} -> Value;
        false             -> Default
    end.

-spec update_alarm(UpdatedAlarm :: #alarm{}, Alarms :: alarms()) -> alarms().
update_alarm(UpdatedAlarm, Alarms) ->
    lists:keystore(UpdatedAlarm#alarm.name, 2, Alarms, UpdatedAlarm).

%% @doc Log key-value-pair at autoscale_server.
-spec log(Key :: atom(), Value :: term()) -> ok.
log(Key, Value) ->
    case autoscale_server:check_config() andalso ?CLOUD =/= cloud_cps of
        true  ->
            autoscale_server:log(Key, Value);
        false ->
            ok
    end.

%% @doc Log number of VMs at autoscale_server.
-spec log_vms() -> ok.
log_vms() ->
    case autoscale_server:check_config() andalso ?CLOUD =/= cloud_cps of
        true  ->
            autoscale_server:log(vms, ?CLOUD:get_number_of_vms());
        false ->
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Request my_range from dht_node (called by rm_loop at direct neighborhood
%%      changes).
-spec send_my_range_req(Pid :: pid(), Tag :: ?MODULE,
                        _OldN :: nodelist:neighborhood(),
                        _NewN :: nodelist:neighborhood()) -> ok.
send_my_range_req(Pid, ?MODULE, _OldN, _NewN) ->
    DhtNodeOfPid = pid_groups:pid_of(pid_groups:group_of(Pid), dht_node),
    comm:send_local(DhtNodeOfPid, {get_state, comm:make_global(Pid), my_range}).

%% @doc Write alarm updates to dht. Alarms in ToAdd are added to the ring and
%%      alarms in ToRem are removed. Usually, ToAdd should contain the new
%%      version of the alarm and ToRem the old version (which could possibly
%%      have been written to the dht before).
%%
%%      When a node becomes leader, she merges her local alarms with alarms from
%%      the ring (alarms on ring overwrite local alarms). See tx_merge_alarms/2.
-spec tx_update_alarms(ToAdd::alarms(), ToRem::alarms()) -> ok | fail;
                      (ToAdd::alarm(), ToRem::alarm()) -> ok | fail.
tx_update_alarms(ToAdd, ToRem)
  when erlang:is_tuple(ToAdd) andalso erlang:is_tuple(ToRem) ->
    tx_update_alarms([ToAdd], [ToRem]);
tx_update_alarms(ToAdd, ToRem) ->
    {TLog, _Res} = api_tx:add_del_on_list(
                     api_tx:new_tlog(), ?AUTOSCALE_TX_KEY, ToAdd, ToRem),
    case api_tx:commit(TLog) of
        {ok} ->
            ok;
        {fail, abort, _ClientKeys} ->
            fail
    end.

%% @doc Merge local alarms with updated alarms from dht.
-spec tx_merge_alarms(Alarms :: alarms()) -> alarms().
tx_merge_alarms(Alarms) ->
    case api_tx:read(?AUTOSCALE_TX_KEY) of
        {ok, Updates} ->
            lists:foldl(
              fun(Alarm, Acc) ->
                      lists:keyreplace(Alarm#alarm.name, 2, Acc, Alarm)
              end, Alarms, Updates);
        {fail, not_found} ->
            Alarms
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(DHTNodeGroup :: pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, autoscale}]).

-spec init([]) -> state().
init([]) ->
    % initial my_range request to dht_node
    comm:send_local(pid_groups:get_my(dht_node),
                    {get_state, comm:this(), my_range}),

    % check my_range at direct neighborhood changes
    rm_loop:subscribe(
      self(), ?MODULE, fun rm_loop:subscribe_dneighbor_change_filter/3,
      fun ?MODULE:rm_exec/5, inf),

    % intial state
    {_IsLeader = false,
     _Alarms   = get_alarms(),
     _ScaleReq =
         case ?CLOUD =:= cloud_cps of
             true  -> #scale_req{mode = pull};
             false ->
                 ?CLOUD:init(),
                 #scale_req{mode = push}
         end,
     _Triggers = []}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_alarms() -> [#alarm{}].
get_alarms() ->
    case check_config() of
        true  ->
            % @todo add convenience for cfg
            config:read(autoscale_alarms);
        false ->
            []
    end.

%% @doc Get timeout for pull lock (in seconds) from config.
-spec get_lock_timeout() -> pos_integer().
get_lock_timeout() ->
    case config:read(autoscale_lock_timeout) of
        Timeout when erlang:is_integer(Timeout) andalso Timeout > 0 ->
            Timeout;
        _ -> ?DEFAULT_LOCK_TIMEOUT
    end.

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_list(autoscale_alarms) andalso
    config:cfg_exists(autoscale_cloud_module) andalso
    config:cfg_is_list(autoscale_alarms,
                       fun(X) -> erlang:is_record(X, alarm) end,
                       "alarm record tuple").

-spec rm_exec(pid(), Tag::?MODULE,
              OldNeighbors::nodelist:neighborhood(),
              NewNeighbors::nodelist:neighborhood(),
              Reason::rm_loop:reason()) -> ok.
rm_exec(Pid, ?MODULE, _OldN, _NewN, _Reason) ->
    DhtNodeOfPid = pid_groups:pid_of(pid_groups:group_of(Pid),
                                     dht_node),
    comm:send_local(DhtNodeOfPid,
                    {get_state, comm:make_global(Pid), my_range}).
