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

toggle_alarm(Name) ->
    api_dht_raw:unreliable_lookup(?RT:hash_key("0"), {send_to_group_member, autoscale,
                                                      {toggle_alarm, Name}}).
toggle_alarms(Names) ->
    lists:foreach(fun toggle_alarm/1, Names).

deactivate_alarms () ->
    api_dht_raw:unreliable_lookup(?RT:hash_key("0"), {send_to_group_member, autoscale,
                                                      {deactivate_alarms}}).
