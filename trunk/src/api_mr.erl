%  @copyright 2011-2013 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    API functions for the Map-Reduce system
%% @end
%% @version $Id$
-module(api_mr).
-author('fajerski@zib.de').
-vsn('$Id$').

-define(TRACE(X, Y), io:format(X, Y)).

-export([start_job/1]).

-include("scalaris.hrl").

-spec start_job(mr:job_description()) -> [any()].
start_job(Job) ->
    Id = randoms:getRandomString(),
    comm:send_local(pid_groups:find_a(dht_node), 
                    {mr, init, comm:this(), Id, Job}),
    wait_for_results([], intervals:empty(), Id).

-spec wait_for_results([any()], intervals:interval(), nonempty_string()) -> [any()].
wait_for_results(Data, Interval, Id) ->
    {NewData, NewInterval} = receive
        ?SCALARIS_RECV({mr_results, PartData, PartInterval, Id},
                       {PartData ++ Data, intervals:union(PartInterval, Interval)})
    end,
    ?TRACE("mr_api: received data for job ~p: ~p~n", [Id, hd(NewData)]),
    case intervals:is_all(NewInterval) of
        true ->
            lists:sort(NewData);
        _ ->
            wait_for_results(NewData, NewInterval, Id)
    end.
