%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    node_details.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Node summary for statistics
%%% @end
%%% Created : 7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(node_details).

-author('schuett@zib.de').
-vsn('$Id$ ').

% constructors
-export([new/0, new/7]).

% getters
-export([get/2]).

% setters
-export([set/3]).

-include("scalaris.hrl").

-type(nodelist() :: [node:node_type()]).
-type(node_type() :: node:node_type()).
-type(load() :: integer()).
-type(hostname() :: string()).
-type(rt_size() :: integer()).
-type(message_log() :: any()).
-type(memory() :: non_neg_integer()).
-type(my_range() :: dht_node_state:my_range()). % two node IDs

-type(node_details_name() :: predlist | pred | node | my_range | succ |
                             succlist | load | hostname | rt_size |
                             message_log | memory).

-record(node_details, {predlist    :: nodelist(),
                       node        :: node_type(),
                       succlist    :: nodelist(),
                       load        :: load(),
                       hostname    :: hostname(),
                       rt_size     :: rt_size(),
                       memory      :: memory()}).
-type(node_details_record() :: #node_details{}).

-type(node_details_list() ::
    [{predlist, nodelist()} |
     {pred, node_type()} |
     {node, node_type()} |
     {my_range, my_range()} |
     {succ, node_type()} |
     {succlist, nodelist()} |
     {load, load()} |
     {hostname, hostname()} |
     {rt_size, rt_size()} |
     {message_log, message_log()} |
     {memory, memory()}]).

-type(node_details() :: node_details_record() | node_details_list()).

-spec new() -> node_details_list().
new() -> [].

-spec new(nodelist(), node_type(), nodelist(), load(), hostname(), rt_size(), memory()) -> node_details_record().
new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory) ->
    #node_details{
     predlist = PredList,
     node = Node,
     succlist = SuccList,
     load = Load,
     hostname = Hostname,
     rt_size = RTSize,
     memory = Memory
    }.

-spec record_to_list(node_details_record()) -> node_details_list().
record_to_list(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails) ->
    [{predlist, PredList},
     {node, Me},
     {succlist, SuccList},
     {load, Load},
     {hostname, HostName},
     {rt_size, RTSize},
     {memory, Memory}].

-spec set(node_details(), predlist, nodelist()) -> node_details();
          (node_details(), pred, node_type()) -> node_details();
          (node_details(), node, node_type()) -> node_details();
          (node_details(), my_range, my_range()) -> node_details();
          (node_details(), succ, node_type()) -> node_details();
          (node_details(), succlist, nodelist()) -> node_details();
          (node_details(), load, load()) -> node_details();
          (node_details(), hostname, hostname()) -> node_details();
          (node_details(), rt_size, rt_size()) -> node_details();
          (node_details(), message_log, message_log()) -> node_details();
          (node_details(), memory, memory()) -> node_details().
set(NodeDetails, Key, Value) when is_record(NodeDetails, node_details) ->
    case Key of
        % record members:
        predlist -> NodeDetails#node_details{predlist = Value};
        node -> NodeDetails#node_details{node = Value};
        succlist -> NodeDetails#node_details{succlist = Value};
        load -> NodeDetails#node_details{load = Value};
        hostname -> NodeDetails#node_details{hostname = Value};
        rt_size -> NodeDetails#node_details{rt_size = Value};
        memory -> NodeDetails#node_details{memory = Value};
        % list members:
        pred -> [{Key, Value} | record_to_list(NodeDetails)];
        my_range -> [{Key, Value} | record_to_list(NodeDetails)];
        succ -> [{Key, Value} | record_to_list(NodeDetails)];
        message_log -> [{Key, Value} | record_to_list(NodeDetails)]
    end;
set(NodeDetails, Key, Value) when is_list(NodeDetails) ->
    lists:keystore(Key, 1, NodeDetails, {Key, Value}).

-spec get(node_details_record(), predlist) -> nodelist();
          (node_details_list(), predlist) -> nodelist() | unknown;
          (node_details(), pred) -> node_type() | unknown;
          (node_details_record(), node) -> node_type();
          (node_details_list(), node) -> node_type() | unknown;
          (node_details(), my_range) -> my_range() | unknown;
          (node_details(), succ) -> node_type() | unknown;
          (node_details_record(), succlist) -> nodelist();
          (node_details_list(), succlist) -> nodelist() | unknown;
          (node_details_record(), load) -> load();
          (node_details_list(), load) -> load() | unknown;
          (node_details_record(), hostname) -> hostname();
          (node_details_list(), hostname) -> hostname() | unknown;
          (node_details_record(), rt_size) -> rt_size();
          (node_details_list(), rt_size) -> rt_size() | unknown;
          (node_details(), message_log) -> message_log() | unknown;
          (node_details_record(), memory) -> memory();
          (node_details_list(), memory) -> memory() | unknown.
get(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
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
get(NodeDetails, Key) when is_list(NodeDetails) ->
    Result = lists:keysearch(Key, 1, NodeDetails),
    case Result of
        {value, {Key, Value}} -> Value;
        false -> unknown
    end.
