%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : bulkowner.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bulk owner operation TODO
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$

%% @doc This implements the bulk owner algorithm.
%% @reference Ali Ghodsi, <em> Distributed k-ary System: Algorithms for Distributed Hash Tables</em>, PhD Thesis, page 129.
-module(bulkowner).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("../include/scalaris.hrl").

-export([issue_bulk_owner/2, start_bulk_owner/2, bulk_owner/3]).

%% @doc start a bulk owner operation.
%%      sends the message to all nodes in the given interval
%% @spec issue_bulk_owner(intervals:interval(), term()) -> ok
issue_bulk_owner(I, Msg) ->
    {ok, DHTNode} = process_dictionary:find_dht_node(),
    cs_send:send_local(DHTNode , {start_bulk_owner, I, Msg}).

start_bulk_owner(I, Msg) ->
    cs_send:send_local(self() , {bulk_owner, I, Msg}).

%% @doc main routine. It spans a broadcast tree over the nodes in I
%% @spec bulk_owner(State::dht_node_state:state(), I::intervals:interval(), 
%%                 Msg::term()) -> ok
bulk_owner(State, I, Msg) ->
    Range = intervals:normalize(intervals:cut(I, dht_node_state:next_interval(State))),
    case intervals:is_empty(Range) of
	true ->
	    ok;
	false ->
	    cs_send:send(dht_node_state:succ_pid(State), {bulkowner_deliver, Range, Msg})
    end,
    U = ?RT:to_dict(State),
    case intervals:is_covered(I, Range) of
	true ->
	    ok;
	false ->
	    bulk_owner_iter(State, U, 1, I, Msg)
    end.

% @spec bulk_owner_iter(State::dht_node_state:state(), U::dict:dictionary(), 
%       Index::int(), I::intervals:interval(), Msg::term()) -> ok
bulk_owner_iter(State, U, Index, I, Msg) ->
    case dict:find(Index, U) of
	{ok, U_of_Index} ->
	    U_of_IndexM1 = dict:fetch(Index - 1, U),
	    Range = intervals:normalize(intervals:cut(I, intervals:new(node:id(U_of_IndexM1), node:id(U_of_Index)))),
	    %ct:pal("iter: ~p ~p ~p ~n", [I, intervals:new(node:id(U_of_IndexM1), node:id(U_of_Index)), Range]),
	    case intervals:is_empty(Range) of
		false ->
		    cs_send:send(node:pidX(U_of_IndexM1), {bulk_owner, Range, Msg});
		true ->
		    ok
	    end,
	    bulk_owner_iter(State, U, Index + 1, I, Msg);
	error ->
	    ok
    end.
