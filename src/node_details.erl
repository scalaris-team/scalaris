%  @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Node summary for statistics
%% @end
%% @version $Id$
-module(node_details).
-author('schuett@zib.de').
-vsn('$Id$').

-export([new/0, new/9,
         contains/2, get/2, set/3]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export_type([node_details/0, node_details_name/0,
              load/0, load2/0, load3/0,
              hostname/0, rt_size/0, message_log/0, memory/0]).

-type(load() :: integer()).
-type(load2() :: lb_stats:load()).
-type(load3() :: lb_stats:load()).
-type(hostname() :: string()).
-type(rt_size() :: integer()).
-type(message_log() :: any()).
-type(memory() :: non_neg_integer()).

-type(node_details_name() :: predlist | pred | node | my_range | succ |
                             succlist | load | hostname | rt_size |
                             message_log | memory | new_key | db | is_leaving).

-record(node_details, {predlist = ?required(node_details, predlist) :: nodelist:non_empty_snodelist(),
                       node     = ?required(node_details, node)     :: node:node_type(),
                       succlist = ?required(node_details, succlist) :: nodelist:non_empty_snodelist(),
                       load     = ?required(node_details, load)     :: load(),
                       load2    = ?required(node_details, load2)    :: load2(),
                       load3    = ?required(node_details, load3)    :: load3(),
                       hostname = ?required(node_details, hostname) :: hostname(),
                       rt_size  = ?required(node_details, rt_size)  :: rt_size(),
                       memory   = ?required(node_details, memory)   :: memory()}).
-type(node_details_record() :: #node_details{}).

-type(node_details_list() ::
    [{predlist, nodelist:non_empty_snodelist()} |
     {node, node:node_type()} |
     {my_range, intervals:interval()} |
     {succlist, nodelist:non_empty_snodelist()} |
     {load, load()} |
     {load2, load2()} |
     {load3, load3()} |
     {hostname, hostname()} |
     {rt_size, rt_size()} |
     {message_log, message_log()} |
     {memory, memory()} |
     {new_key, ?RT:key()} |
     {db, db_dht:db()} | % only useful locally for read access!
     {is_leaving, boolean()}]).

-opaque(node_details() :: node_details_record() | node_details_list()).

%% @doc Creates an empty node details object.
-spec new() -> node_details().
new() -> [].

%% @doc Creates a new node details object with the given data.
-spec new(PredList::nodelist:non_empty_snodelist(), Node::node:node_type(), SuccList::nodelist:non_empty_snodelist(), Load::load(), Load2::load2(), Load3::load3(), Hostname::hostname(), RTSize::rt_size(), Memory::memory()) -> node_details().
new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory) ->
    #node_details{
     predlist = PredList,
     node = Node,
     succlist = SuccList,
     load = Load,
     load2 = Load2,
     load3 = Load3,
     hostname = Hostname,
     rt_size = RTSize,
     memory = Memory
    }.

%% @doc Converts a node details record into a list (only for internal use!).
-spec to_list(node_details_record()) -> node_details_list().
to_list(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load, load2=Load2, load3=Load3,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails) ->
    [{predlist, PredList},
     {node, Me},
     {succlist, SuccList},
     {load, Load},
     {load2, Load2},
     {load3, Load3},
     {hostname, HostName},
     {rt_size, RTSize},
     {memory, Memory}].

%% @doc Adds the given data to the node list object.
%%      Beware: Setting pred/succ will overwrite predlist/succlist!
%%      (pred and succ are only shortcuts for hd(predlist)/hd(succlist))
-spec set(node_details(), predlist, nodelist:non_empty_snodelist()) -> node_details();
          (node_details(), pred, node:node_type()) -> node_details();
          (node_details(), node, node:node_type()) -> node_details();
          (node_details(), my_range, intervals:interval()) -> node_details();
          (node_details(), is_leaving, boolean()) -> node_details();
          (node_details(), succ, node:node_type()) -> node_details();
          (node_details(), succlist, nodelist:non_empty_snodelist()) -> node_details();
          (node_details(), db, db_dht:db()) -> node_details();
          (node_details(), load, load()) -> node_details();
          (node_details(), load2, load2()) -> node_details();
          (node_details(), load3, load3()) -> node_details();
          (node_details(), hostname, hostname()) -> node_details();
          (node_details(), rt_size, rt_size()) -> node_details();
          (node_details(), message_log, message_log()) -> node_details();
          (node_details(), memory, memory()) -> node_details();
          (node_details(), new_key, ?RT:key()) -> node_details().
set(NodeDetails, Key, Value) when is_record(NodeDetails, node_details) ->
    case Key of
        % record members:
        predlist -> NodeDetails#node_details{predlist = Value};
        node -> NodeDetails#node_details{node = Value};
        succlist -> NodeDetails#node_details{succlist = Value};
        load -> NodeDetails#node_details{load = Value};
        load2 -> NodeDetails#node_details{load2 = Value};
        load3 -> NodeDetails#node_details{load3 = Value};
        hostname -> NodeDetails#node_details{hostname = Value};
        rt_size -> NodeDetails#node_details{rt_size = Value};
        memory -> NodeDetails#node_details{memory = Value};
        succ -> NodeDetails#node_details{succlist = [Value]};
        pred -> NodeDetails#node_details{predlist = [Value]};
        % list members:
        my_range -> [{Key, Value} | to_list(NodeDetails)];
        is_leaving -> [{Key, Value} | to_list(NodeDetails)];
        message_log -> [{Key, Value} | to_list(NodeDetails)];
        new_key -> [{Key, Value} | to_list(NodeDetails)];
        db -> [{Key, Value} | to_list(NodeDetails)]
    end;
set(NodeDetails, Key, Value) when is_list(NodeDetails) ->
    case Key of
        pred -> lists:keystore(predlist, 1, NodeDetails, {predlist, [Value]});
        succ -> lists:keystore(succlist, 1, NodeDetails, {succlist, [Value]});
        _    -> lists:keystore(Key, 1, NodeDetails, {Key, Value})
    end.

%% @doc Checks whether the given data is available in a node details object.
-spec contains(node_details(), node_details_name()) -> boolean().
contains(NodeDetails, Key) when is_record(NodeDetails, node_details) ->
    lists:member(Key, [predlist, pred, node, succlist, succ, load, hostname,
                       rt_size, memory]);
contains(NodeDetails, Key) when is_list(NodeDetails) ->
    case Key of
        pred -> lists:keymember(predlist, 1, NodeDetails);
        succ -> lists:keymember(succlist, 1, NodeDetails);
        _    -> lists:keymember(Key, 1, NodeDetails)
    end.

%% @doc Gets the value of the given data in a node details object. Will throw
%%      an exception if the value can not be located.
-spec get(node_details(), predlist | succlist) -> nodelist:non_empty_snodelist();
          (node_details(), pred | node | succ) -> node:node_type();
          (node_details(), my_range) -> intervals:interval();
          (node_details(), is_leaving) -> boolean();
          (node_details(), db) -> db_dht:db();
          (node_details(), load) -> load();
          (node_details(), load2) -> load2();
          (node_details(), load3) -> load3();
          (node_details(), hostname) -> hostname();
          (node_details(), rt_size) -> rt_size();
          (node_details(), message_log) -> message_log();
          (node_details(), memory) -> memory();
          (node_details(), new_key) -> ?RT:key().
get(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load, load2=Load2, load3=Load3,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails, Key) ->
    case Key of
        predlist -> PredList;
        pred -> hd(PredList);
        node -> Me;
        succlist -> SuccList;
        succ -> hd(SuccList);
        load -> Load;
        load2 -> Load2;
        load3 -> Load3;
        hostname -> HostName;
        rt_size -> RTSize;
        memory -> Memory;
        _ -> throw('not_available')
    end;
get(NodeDetails, Key) when is_list(NodeDetails) ->
    case Key of
        pred -> hd(get_list(NodeDetails, predlist));
        succ -> hd(get_list(NodeDetails, succlist));
        _    -> get_list(NodeDetails, Key)
    end.

-spec get_list(node_details_list(), predlist | succlist) -> nodelist:non_empty_snodelist();
              (node_details_list(), pred | node | succ) -> node:node_type();
              (node_details_list(), my_range) -> intervals:interval();
              (node_details_list(), is_leaving) -> boolean();
              (node_details_list(), db) -> db_dht:db();
              (node_details_list(), load) -> load();
              (node_details_list(), hostname) -> hostname();
              (node_details_list(), rt_size) -> rt_size();
              (node_details_list(), message_log) -> message_log();
              (node_details_list(), memory) -> memory();
              (node_details_list(), new_key) -> ?RT:key().
get_list(NodeDetails, Key) ->
    case lists:keyfind(Key, 1, NodeDetails) of
        {Key, Value} -> Value;
        false -> throw('not_available')
    end.
