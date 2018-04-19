%% @copyright 2018 Zuse Institute Berlin

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

%% @author Thorsten Schütt <schuett@zib.de>
%% @doc API for querying properties of rings.
%%
-module(api_ring).
-author('schuett@zib.de').

-export([get_ring_size/1, wait_for_ring_size/2]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec get_ring_size(TimeOut::integer()) -> failed | integer().
get_ring_size(TimeOut) ->
    Id = uid:get_global_uid(),
    bulkowner:issue_bulk_owner(Id, intervals:all(),
                               {deliver_ping, comm:this(), {get_ring_size, Id}}),
    get_ring_size_internal(intervals:empty(), 0, TimeOut, Id).

get_ring_size_internal(Interval, Count, TimeOut, Id) ->
    receive
        {pong, AnInterval, Id, {get_ring_size, Id}} ->
            Interval2 = intervals:union(Interval, AnInterval),
            case intervals:is_all(Interval2) of
                true ->
                    Count + 1;
                false ->
                    get_ring_size_internal(Interval2, Count + 1, TimeOut, Id)
            end;
        X ->
            io:format("~w~n", [X])
    after
        TimeOut ->
            failed
    end.

-spec wait_for_ring_size(Size::integer(), TimeOut::integer()) -> ok.
wait_for_ring_size(Size, TimeOut) ->
    case get_ring_size(TimeOut) of
        Size ->
            ok;
        _ ->
            wait_for_ring_size(Size, TimeOut)
    end.
