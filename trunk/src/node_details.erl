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
%%% File    : node_details.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Node summary for statistics
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(node_details).

-author('schuett@zib.de').
-vsn('$Id$ ').

% constructors
-export([new/0, new/7]).

% getters
-export([predlist/1,
         pred/1,
         me/1,
         my_range/1,
         succ/1,
         succlist/1,
         load/1,
         hostname/1,
         rt_size/1,
         message_log/1,
         memory/1]).

% setters
-export([set_predlist/2,
         set_pred/2,
         set_me/2,
         set_my_range/2,
         set_succ/2,
         set_succlist/2,
         set_load/2,
         set_hostname/2,
         set_rt_size/2,
         set_message_log/2,
         set_memory/2]).

-include("../include/scalaris.hrl").

-type(predlist() :: [node:node_type()]).
-type(node_type() :: node:node_type()).
-type(succlist() :: [node:node_type()]).
-type(load() :: integer()).
-type(hostname() :: string()).
-type(rt_size() :: integer()).
-type(message_log() :: any()).
-type(memory() :: non_neg_integer()).
-type(node_range() :: {any(), any()}). % two node IDs

-record(node_details, {predlist    :: predlist(),
                       node        :: node_type(),
                       succlist    :: succlist(),
                       load        :: load(),
                       hostname    :: hostname(),
                       rt_size     :: rt_size(),
                       memory      :: memory()}).

-type(node_details() ::
    #node_details{} |
    [{predlist, predlist()} |
     {pred, node_type()} |
     {node, node_type()} |
     {my_range, node_range()} |
     {succ, node_type()} |
     {succlist, succlist()} |
     {load, load()} |
     {hostname, hostname()} |
     {rt_size, rt_size()} |
     {message_log, message_log()} |
     {memory, memory()}]).

-spec new() -> node_details().
new() -> [].

-spec new(predlist(), node_type(), succlist(), load(), hostname(), rt_size(), memory()) -> node_details().
new(Pred, Node, SuccList, Load, Hostname, RTSize, Memory) ->
    #node_details{
     predlist = Pred,
     node = Node,
     succlist = SuccList,
     load = Load,
     hostname = Hostname,
     rt_size = RTSize,
     memory = Memory
    }.

-spec predlist(node_details()) -> predlist() | unknown.
predlist(NodeDetails) -> get_info(NodeDetails, predlist).

-spec set_predlist(node_details(), predlist()) -> node_details().
set_predlist(NodeDetails, PredList) -> set_info(NodeDetails, predlist, PredList).

-spec pred(node_details()) -> node_type() | unknown.
pred(NodeDetails) -> get_info(NodeDetails, pred).

-spec set_pred(node_details(), node_type()) -> node_details().
set_pred(NodeDetails, Pred) -> set_info(NodeDetails, pred, Pred).

-spec me(node_details()) -> node:node_type() | unknown.
me(NodeDetails) -> get_info(NodeDetails, node).

-spec set_me(node_details(), node_type()) -> node_details().
set_me(NodeDetails, Me) -> set_info(NodeDetails, node, Me).

-spec my_range(node_details()) -> node_range() | unknown.
my_range(NodeDetails) -> get_info(NodeDetails, my_range).

-spec set_my_range(node_details(), node_range()) -> node_details().
set_my_range(NodeDetails, Range) -> set_info(NodeDetails, my_range, Range).

-spec succ(node_details()) -> node_type() | unknown.
succ(NodeDetails) -> get_info(NodeDetails, succ).

-spec set_succ(node_details(), node_type()) -> node_details().
set_succ(NodeDetails, Succ) -> set_info(NodeDetails, succ, Succ).

-spec succlist(node_details()) -> succlist() | unknown.
succlist(NodeDetails) -> get_info(NodeDetails, succlist).

-spec set_succlist(node_details(), succlist()) -> node_details().
set_succlist(NodeDetails, SuccList) -> set_info(NodeDetails, succlist, SuccList).

-spec load(node_details()) -> load() | unknown.
load(NodeDetails) -> get_info(NodeDetails, load).

-spec set_load(node_details(), load()) -> node_details().
set_load(NodeDetails, Load) -> set_info(NodeDetails, load, Load).

-spec hostname(node_details()) -> hostname() | unknown.
hostname(NodeDetails) -> get_info(NodeDetails, hostname).

-spec set_hostname(node_details(), hostname()) -> node_details().
set_hostname(NodeDetails, HostName) -> set_info(NodeDetails, hostname, HostName).

-spec rt_size(node_details()) -> rt_size() | unknown.
rt_size(NodeDetails) -> get_info(NodeDetails, rt_size).

-spec set_rt_size(node_details(), rt_size()) -> node_details().
set_rt_size(NodeDetails, RTSize) -> set_info(NodeDetails, rt_size, RTSize).

-spec message_log(node_details()) -> message_log() | unknown.
message_log(NodeDetails) -> get_info(NodeDetails, message_log).

-spec set_message_log(node_details(), message_log()) -> node_details().
set_message_log(NodeDetails, Log) -> set_info(NodeDetails, message_log, Log).

-spec memory(node_details()) -> memory() | unknown.
memory(NodeDetails) -> get_info(NodeDetails, memory).

-spec set_memory(node_details(), memory()) -> node_details().
set_memory(NodeDetails, Memory) -> set_info(NodeDetails, memory, Memory).

-spec set_info(node_details(), predlist, predlist()) -> node_details()
             ; (node_details(), pred, node_type()) -> node_details()
             ; (node_details(), node, node_type()) -> node_details()
             ; (node_details(), my_range, node_range()) -> node_details()
             ; (node_details(), succ, node_type()) -> node_details()
             ; (node_details(), succlist, succlist()) -> node_details()
             ; (node_details(), load, load()) -> node_details()
             ; (node_details(), hostname, hostname()) -> node_details()
             ; (node_details(), rt_size, rt_size()) -> node_details()
             ; (node_details(), message_log, message_log()) -> node_details()
             ; (node_details(), memory, memory()) -> node_details().
set_info(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
  hostname=HostName, rt_size=RTSize, memory=Memory} = NodeDetails, Key, Value) ->
	case Key of
		predlist -> NodeDetails#node_details{predlist = Value};
		node -> NodeDetails#node_details{node = Value};
		succlist -> NodeDetails#node_details{succlist = Value};
		load -> NodeDetails#node_details{load = Value};
		hostname -> NodeDetails#node_details{hostname = Value};
		rt_size -> NodeDetails#node_details{rt_size = Value};
		memory -> NodeDetails#node_details{memory = Value};
		_ -> [{Key, Value},
			 {predlist, PredList},
			 {node, Me},
			 {succlist, SuccList},
			 {load, Load},
			 {hostname, HostName},
			 {rt_size, RTSize},
			 {memory, Memory}]
	end;
set_info(NodeDetails, Key, Value) when is_list(NodeDetails) ->
	[{Key, Value} | NodeDetails].

-spec get_info(node_details(), predlist) -> predlist() | unknown
             ; (node_details(), pred) -> node_type() | unknown
             ; (node_details(), node) -> node_type() | unknown
             ; (node_details(), my_range) -> node_range() | unknown
             ; (node_details(), succ) -> node_type() | unknown
             ; (node_details(), succlist) -> succlist() | unknown
             ; (node_details(), load) -> load() | unknown
             ; (node_details(), hostname) -> hostname() | unknown
             ; (node_details(), rt_size) -> rt_size() | unknown
             ; (node_details(), message_log) -> message_log() | unknown
             ; (node_details(), memory) -> memory() | unknown.
get_info(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails, Key) ->
	case Key of
		predlist -> PredList;
		node -> Me;
		succlist -> SuccList;
		load -> Load;
		hostname -> HostName;
		rt_size -> RTSize;
		memory -> Memory;
		_ -> unknown
	end;
get_info(NodeDetails, Key) when is_list(NodeDetails) ->
	Result = lists:keysearch(Key, 1, NodeDetails),
	case Result of
		{value, {Key, Value}} -> Value;
		false -> unknown
    end.
