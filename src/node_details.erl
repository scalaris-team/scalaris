%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

-export([new/0, new/7,
         contains/2, get/2, set/3]).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([node_details/0, node_details_name/0,
              load/0, hostname/0, rt_size/0, message_log/0, memory/0]).
-endif.

-type(load() :: integer()).
-type(hostname() :: string()).
-type(rt_size() :: integer()).
-type(message_log() :: any()).
-type(memory() :: non_neg_integer()).

-type(node_details_name() :: predlist | pred | node | my_range | succ |
                             succlist | load | hostname | rt_size |
                             message_log | memory).

-record(node_details, {predlist    :: nodelist:non_empty_snodelist(),
                       node        :: node:node_type(),
                       succlist    :: nodelist:non_empty_snodelist(),
                       load        :: load(),
                       hostname    :: hostname(),
                       rt_size     :: rt_size(),
                       memory      :: memory()}).
% TODO: copy field declarations from record definition with their types into #node_details{}
%       (erlang otherwise thinks of a field type as 'unknown' | type())
%       http://www.erlang.org/doc/reference_manual/typespec.html#id2272601
%       dialyzer up to R14A can not handle these definitions though
%       http://www.erlang.org/cgi-bin/ezmlm-cgi?2:mss:1979:cbgdipmboiafbbcfaifn
%       -> be careful when using this type with the tester module!
-type(node_details_record() :: #node_details{}).

-type(node_details_list() ::
    [{predlist, nodelist:non_empty_snodelist()} |
     {node, node:node_type()} |
     {my_range, intervals:interval()} |
     {succlist, nodelist:non_empty_snodelist()} |
     {load, load()} |
     {hostname, hostname()} |
     {rt_size, rt_size()} |
     {message_log, message_log()} |
     {memory, memory()}]).

-opaque(node_details() :: node_details_record() | node_details_list()).

%% @doc Creates an empty node details object.
-spec new() -> node_details().
new() -> [].

%% @doc Creates a new node details object with the given data.
-spec new(PredList::nodelist:non_empty_snodelist(), Node::node:node_type(), SuccList::nodelist:non_empty_snodelist(), Load::load(), Hostname::hostname(), RTSize::rt_size(), Memory::memory()) -> node_details().
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

%% @doc Converts a node details record into a list (only for internal use!).
-spec to_list(node_details()) -> node_details_list().
to_list(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails) ->
    [{predlist, PredList},
     {node, Me},
     {succlist, SuccList},
     {load, Load},
     {hostname, HostName},
     {rt_size, RTSize},
     {memory, Memory}];
to_list(NodeDetails) when is_list(NodeDetails) ->
    NodeDetails.

%% @doc Adds the given data to the node list object.
%%      Beware: Setting pred/succ will overwrite predlist/succlist!
%%      (pred and succ are only shortcuts for hd(predlist)/hd(succlist))
-spec set(node_details(), predlist, nodelist:non_empty_snodelist()) -> node_details();
          (node_details(), pred, node:node_type()) -> node_details();
          (node_details(), node, node:node_type()) -> node_details();
          (node_details(), my_range, intervals:interval()) -> node_details();
          (node_details(), succ, node:node_type()) -> node_details();
          (node_details(), succlist, nodelist:non_empty_snodelist()) -> node_details();
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
        succ -> NodeDetails#node_details{succlist = [Value]};
        pred -> NodeDetails#node_details{predlist = [Value]};
        % list members:
        my_range -> [{Key, Value} | to_list(NodeDetails)];
        message_log -> [{Key, Value} | to_list(NodeDetails)]
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
          (node_details(), load) -> load();
          (node_details(), hostname) -> hostname();
          (node_details(), rt_size) -> rt_size();
          (node_details(), message_log) -> message_log();
          (node_details(), memory) -> memory().
get(#node_details{predlist=PredList, node=Me, succlist=SuccList, load=Load,
  hostname=HostName, rt_size=RTSize, memory=Memory} = _NodeDetails, Key) ->
    case Key of
        predlist -> PredList;
        pred -> hd(PredList);
        node -> Me;
        succlist -> SuccList;
        succ -> hd(SuccList);
        load -> Load;
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
              (node_details_list(), load) -> load();
              (node_details_list(), hostname) -> hostname();
              (node_details_list(), rt_size) -> rt_size();
              (node_details_list(), message_log) -> message_log();
              (node_details_list(), memory) -> memory().
get_list(NodeDetails, Key) ->
    case lists:keyfind(Key, 1, NodeDetails) of
        {Key, Value} -> Value;
        false -> throw('not_available')
    end.
