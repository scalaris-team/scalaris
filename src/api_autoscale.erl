% @copyright 2013-2016 Zuse Institute Berlin

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
%% @doc Simple auto-scaling service API.
%% @end
-module(api_autoscale).
-author('celebi@zib.de').

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type autoscale_error_resp() :: {error, resp_timeout | autoscale_false}.

%% Misc API
-export([check_config/0]).
%% Polling API
-export([pull_scale_req/0, lock_scale_req/0, unlock_scale_req/0]).
%% Alarm state API
-export([toggle_alarm/1, activate_alarms/0, deactivate_alarms/0]).
%% Autoscale server API
-export([write_plot_data/0, reset_plot_data/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Checks whether config parameters exist and are valid (requires a pull
%%      configuration, i.e. cloud_cps as cloud_module).
-spec check_config() -> boolean().
check_config() ->
    autoscale:check_config() andalso is_in_pull_mode().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Polling API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Pull current scale request from autoscale leader. If the request is
%%      not 0, further requests should be locked with lock_scale_req/0 until a
%%      unlock_scale_req/0 call has been made. Autoscale defines a timeout,
%%      after which the lock will be automatically freed, i.e. the caller has
%%      $timeout seconds to satisfy the request and notify autoscale by
%%      unlock_scale_req/0.
-spec pull_scale_req() -> {ok, Req :: integer()} | autoscale_error_resp().
pull_scale_req() ->
    case autoscale:check_config() andalso is_in_pull_mode() of
        true  -> send_to_leader_wait_resp({pull_scale_req, comm:this()},
                                          scale_req_resp, 5);
        false -> {error, autoscale_false}
    end.

-spec lock_scale_req() -> ok | {error, locked} | autoscale_error_resp().
lock_scale_req() ->
    case autoscale:check_config() andalso is_in_pull_mode() of
        true  -> send_to_leader_wait_resp({lock_scale_req, comm:this()},
                                          scale_req_resp, 5);
        false -> {error, autoscale_false}
    end.

-spec unlock_scale_req() -> ok | {error, not_locked} | autoscale_error_resp().
unlock_scale_req() ->
    case autoscale:check_config() andalso is_in_pull_mode() of
        true  -> send_to_leader_wait_resp({unlock_scale_req, comm:this()},
                                          scale_req_resp, 5);
        false -> {error, autoscale_false}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Alarm state API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Toggle state of alarm Name from active to inactive and vice versa.
-spec toggle_alarm(Name :: atom()) -> {ok, {new_state, NewState :: active | inactive}} |
                                      {error, unknown_alarm | tx_fail} |
                                      autoscale_error_resp().
toggle_alarm(Name) ->
    case autoscale:check_config() of
        true  -> send_to_leader_wait_resp({toggle_alarm, Name, comm:this()},
                                          toggle_alarm_resp, 5);
        false -> {error, autoscale_false}
    end.

%% @doc Set all alarms to active.
-spec activate_alarms() -> ok | {error, tx_fail} | autoscale_error_resp().
activate_alarms () ->
    case autoscale:check_config() of
        true  -> send_to_leader_wait_resp({activate_alarms, comm:this()},
                                          activate_alarms_resp, 5);
        false -> {error, autoscale_false}
    end.

%% @doc Set all alarms to inactive.
-spec deactivate_alarms() -> ok | {error, tx_fail} | autoscale_error_resp().
deactivate_alarms () ->
    case autoscale:check_config() of
        true  -> send_to_leader_wait_resp({deactivate_alarms, comm:this()},
                                          deactivate_alarms_resp, 5);
        false -> {error, autoscale_false}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Autoscale server API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec write_plot_data() -> ok | {error, mgmt_server_false | autoscale_server_false}.
write_plot_data() ->
    ?IIF(config:read(autoscale_server),
        case MgmtServer = config:read(mgmt_server) of
            failed -> {error, mgmt_server_false};
            _      ->
                comm:send(MgmtServer, {write_to_file}, [{group_member, autoscale_server}])
        end,
        {error, autoscale_server_false}).

-spec reset_plot_data() -> ok | {error, mgmt_server_false | autoscale_server_false}.
reset_plot_data() ->
    ?IIF(config:read(autoscale_server),
        case MgmtServer = config:read(mgmt_server) of
            failed -> {error, mgmt_server_false};
            _      ->
                comm:send(MgmtServer, {reset}, [{group_member, autoscale_server}])
        end,
        {error, autoscale_server_false}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Send to autoscale leader.
-spec send_to_leader(Msg :: comm:message()) -> ok.
send_to_leader(Msg) ->
    api_dht_raw:unreliable_lookup(?RT:hash_key("0"),
                                  {?send_to_group_member, autoscale, Msg}).

%% @doc Send to autoscale leader and wait for response.
-spec send_to_leader_wait_resp(Msg :: comm:message(),
                               RespTag :: comm:msg_tag(),
                               Timeout :: pos_integer()) ->
          Resp :: term() | {error, resp_timeout}.
send_to_leader_wait_resp(Msg, RespTag, Timeout) ->
    send_to_leader(Msg),
    receive
        {RespTag, Resp} -> Resp
    after
        Timeout*1000    -> {error, resp_timeout}
    end.

%% @doc Check whether scale requests are pulled or pushed to cloud_module.
-spec is_in_pull_mode() -> boolean().
is_in_pull_mode() ->
    config:read(autoscale_cloud_module) =:= cloud_cps.
