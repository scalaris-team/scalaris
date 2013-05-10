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
%% @doc Simple auto-scaling service API.
%% @end
%% @version $Id$
-module(api_autoscale).
-author('celebi@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-compile(export_all).

%%==============================================================================
%% Polling API
%%==============================================================================

%% @doc Pull current scale request from autoscale leader. If the request is
%%      successful, further requests are blocked until a unlock_scale_req/0 call
%%      has been made. Autoscale defines a timeout, after which the lock will be
%%      automatically freed, i.e. the caller has $timeout seconds to satisfy
%%      the request and notify autoscale by unlock_scale_req/0. 
-spec pull_scale_req() -> {ok, Req :: integer()} | 
                          {error, locked} |                          
                          {error, resp_timeout}. 
pull_scale_req() ->
    send_to_leader_wait_resp(
      {pull_scale_req, comm:this()}, scale_req_resp, 5).

-spec unlock_scale_req() -> ok |
                            {error, not_locked} |
                            {error, resp_timeout}.
unlock_scale_req() ->
    send_to_leader_wait_resp(
      {unlock_scale_req, comm:this()}, scale_req_resp, 5).

%%==============================================================================
%% Alarm state API
%%==============================================================================

%% @doc Toggle state of alarm Name from active to inactive and vice versa.
-spec toggle_alarm(Name :: atom()) -> {ok, {new_state, NewState :: active | inactive}} |
                                      {error, unknown_alarm} |
                                      {error, tx_fail} |
                                      {error, resp_timeout}.
toggle_alarm(Name) ->
    send_to_leader_wait_resp(
      {toggle_alarm, Name, comm:this()}, toggle_alarm_resp, 5).

%% @doc Set all alarms to active.
-spec activate_alarms() -> ok |
                           {error, tx_fail} |
                           {error, resp_timeout}.
activate_alarms () ->
    send_to_leader_wait_resp(
      {activate_alarms, comm:this()}, activate_alarms_resp, 5).

%% @doc Set all alarms to inactive.
-spec deactivate_alarms() -> ok |
                           {error, tx_fail} |
                           {error, resp_timeout}.
deactivate_alarms () ->
    send_to_leader_wait_resp(
      {deactivate_alarms, comm:this()}, deactivate_alarms_resp, 5).

%%==============================================================================
%% Autoscale server API
%%==============================================================================

write_plot_data() ->
    ?IIF(config:read(autoscale_server),
        case MgmtServer = config:read(mgmt_server) of
            failed -> {error, mgmt_server_false};
            _      ->
                comm:send(MgmtServer, {?send_to_group_member, autoscale_server,
                                       {write_to_file}})
        end,
        {error, autoscale_server_false}).

reset_plot_data() ->
    ?IIF(config:read(autoscale_server),
        case MgmtServer = config:read(mgmt_server) of
            failed -> {error, mgmt_server_false};
            _      ->
                comm:send(MgmtServer, {?send_to_group_member, autoscale_server,
                                       {reset}})
        end,
        {error, autoscale_server_false}).

%%==============================================================================
%% Misc
%%==============================================================================

%% @doc Send to autoscale leader.
-spec send_to_leader(Msg :: comm:msg()) -> ok. 
send_to_leader(Msg) ->
    api_dht_raw:unreliable_lookup(?RT:hash_key("0"),
                                  {?send_to_group_member, autoscale, Msg}).

%% @doc Send to autoscale leader and wait for response. 
-spec send_to_leader_wait_resp(Msg :: comm:message(),
                               RespTag :: comm:msg_tag(),
                               Timeout :: pos_integer()) ->
          {RespTag :: comm:msg_tag(), Resp :: term()} | {error, resp_timeout}. 
send_to_leader_wait_resp(Msg, RespTag, Timeout) ->
    send_to_leader(Msg),
    receive
        {RespTag, Resp} -> Resp
    after
        Timeout*1000    -> {error, resp_timeout}
    end.