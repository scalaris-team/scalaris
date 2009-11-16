%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : cs_replica_stabilization.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bulk owner operation TODO
%%%
%%% Created :  18 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_replica_stabilization).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("../include/scalaris.hrl").

-export([recreate_replicas/1, createReplicatedIntervals/2]).

%% @doc recreates the replicas of the given key range
%% @spec recreate_replicas({string(), string()}) -> pid()
recreate_replicas({_From, _To}) ->
%    InstanceId = erlang:get(instance_id),
%    spawn(fun () -> 
%		  erlang:put(instance_id, InstanceId),
% 		  Intervals = createReplicatedIntervals(From, To),
% 		  io:format("{~p, ~p}: ~p~n", [From, To, Intervals]),
% 		  start_fetchers(1, Intervals),
% 		  recreate_replicas_loop(length(Intervals), gb_sets:empty(), []) 
% 	  end),
    ok.


%% recreate_replicas_loop(Intervals, Done, Data) ->
%%     receive
%% 	{fetched_data, Index, FetchedData} ->
%% 	    case gb_sets:is_member(Index, Done) of
%% 		false ->
%% 		    case gb_sets:size(Done) + 1 == Intervals of
%% 			true ->
%% 			    update_db(Data);
%% 			false ->
%% 			    recreate_replicas_loop(Intervals, gb_sets:add(Index, Done), [FetchedData | Data])
%% 		    end;
%% 		true ->
%% 		    recreate_replicas_loop(Intervals, Done, Data)
%% 	    end
%%     end.

%%====================================================================
%% fetch functions
%%====================================================================  

% @spec start_fetchers(int(), [intervals:interval()]) -> ok
start_fetchers(_Index, []) ->
    ok;
start_fetchers(Index, [Interval | Tail]) ->
    Me = self(),
    gen_component:start(?MODULE, {Me, Index, Interval},[], Me),
    start_fetchers(Index + 1, Tail).
    

init({Owner, Index, Interval}) ->
    bulkowner:issue_bulk_owner(Interval,{bulk_read_with_version, cs_send:this()}),
    cs_send:send_after(5000, self(), {timeout}),
    {Owner, Index, Interval, [], []}.



on({timeout},{Owner, Index, Interval, Done, FetchedData}) ->
	    cs_send:send_after(5000, self(), {timeout}),
	    {Owner, Index, Interval, Done, FetchedData};
on({bulk_read_with_version_response, Interval, NewData},{Owner, Index, Interval, Done, FetchedData}) ->
	    Done2 = [Interval | Done],
	    case done(Interval, Done2) of
		false ->
		    {Owner, Index, Interval, Done2,[FetchedData| NewData]};
		true ->
		    cs_send:send_local(Owner , {fetched_data, Index, FetchedData}),
		    kill
	    end;
on(_, _State) ->
    unknown_event.

% @spec done(intervals:interval(), [intervals:interval()]) -> bool()
done(Interval, Done) ->
    intervals:is_covered(Interval, Done).

%%====================================================================
%% update database functions (TODO)
%%====================================================================  

%% @TODO
% @spec update_db([{Key::term(), Value::term(), Version::int(), WriteLock::bool(), ReadLock::int()}]) -> ok
update_db(Data) ->
    ok.

%%====================================================================
%% replica management
%%====================================================================  

% @spec createReplicatedIntervals(term(), term()) -> [intervals:interval()]
createReplicatedIntervals(From, To) ->
    FromReplicas = ?RT:get_keys_for_replicas(From),
    ToReplicas   = ?RT:get_keys_for_replicas(To),
    Zipped = lists:zip(FromReplicas, ToReplicas),
    lists:map(fun intervals:make/1, Zipped).

