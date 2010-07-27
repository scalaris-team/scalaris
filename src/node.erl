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
%% @doc    node data structure + functions
%% @end
%% @version $Id$
-module(node).
-author('schuett@zib.de').
-vsn('$Id$').

-export([id/1, id_version/1, pidX/1,
         new/3, null/0, is_valid/1,
         equals/2, is_newer/2, newer/2, is_me/1]).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([node_type/0]).
-endif.

-record(node, {pid :: comm:mypid(), id :: ?RT:key(), id_version :: non_neg_integer()}).
-opaque(node_type() :: #node{}).

%% @doc Creates a new node.
-spec new(Pid::comm:mypid(), Id::?RT:key(), IdVersion::non_neg_integer()) -> node_type().
new(Pid, Id, IdVersion) ->
    #node{pid = Pid, id = Id, id_version = IdVersion}.

%% @doc Creates an invalid node.
null() ->
    null.

%% @doc Gets the pid of the node.
-spec pidX(node_type()) -> comm:mypid().
pidX(#node{pid=PID}) ->
    PID.

%% @doc Gets the node's ID.
-spec id(node_type()) -> ?RT:key().
id(#node{id=Id}) ->
    Id.

%% @doc Gets the version of the node's ID.
-spec id_version(node_type()) -> non_neg_integer().
id_version(#node{id_version=IdVersion}) ->
    IdVersion.

%% @doc Checks whether the given parameter is a valid node.
-spec is_valid(node_type()) -> true;
               (null | unknown) -> false.
is_valid(X) when is_record(X, node) ->
    true;
is_valid(_) ->
    false.

%% @doc Checks whether two nodes are the same, i.e. contain references to the.
%%      same process.
-spec equals(node_type() | comm:mypid() | pid(), node_type()) -> boolean();
            (node_type(), comm:mypid() | pid()) -> boolean();
            (null | unknown, node_type() | comm:mypid() | pid()) -> false;
            (node_type() | comm:mypid() | pid(), null | unknown) -> false.
equals(Node1, Node2) when ((Node1 =:= null) orelse (Node1 =:= unknown) orelse
                           (Node2 =:= null) orelse (Node2 =:= unknown)) ->
    false;
equals(Node1, Node2) when is_record(Node1, node) andalso is_record(Node2, node) ->
    pidX(Node1) =:= pidX(Node2);
equals(Pid1, Node2) when is_record(Node2, node) andalso is_pid(Pid1) ->
    comm:make_global(Pid1) =:= pidX(Node2);
equals(Pid1, Node2) when is_record(Node2, node) ->
    Pid1 =:= pidX(Node2);
equals(Node1, Pid2) when is_record(Node1, node) andalso is_pid(Pid2)->
    pidX(Node1) =:= comm:make_global(Pid2);
equals(Node1, Pid2) when is_record(Node1, node) ->
    pidX(Node1) =:= Pid2.

%% @doc Checks whether the given node is the same as the node associated with
%%      the requesting process.
-spec is_me(node_type() | null | unknown) -> boolean().
is_me(Node) ->
    equals(Node, process_dictionary:get_group_member(dht_node)).

%% @doc Determines whether Node1 is a newer instance of Node2.
%%      Note: Both nodes need to share the same PID, otherwise an exception of
%%      type 'throw' is thrown!
-spec is_newer(Node1::node_type(), Node2::node_type()) -> boolean().
is_newer(#node{pid=PID, id=Id1, id_version=IdVersion1}, #node{pid=PID, id=Id2, id_version=IdVersion2}) ->
    if
        (IdVersion1 > IdVersion2) -> true;
        (IdVersion1 =:= IdVersion2) andalso (Id1 =:= Id2) -> false;
        IdVersion1 =:= IdVersion2 ->
            throw('got two nodes with same IDversion but different ID');
        true -> false
    end.

%% @doc Determines the newer instance of two representations of the same node.
%%      Note: Both nodes need to share the same PID, otherwise an exception of
%%      type 'throw' is thrown!
-spec newer(node_type(), node_type()) -> node_type().
newer(Node1, Node2) ->
    case is_newer(Node1, Node2) of
        true -> Node1;
        _    -> Node2
    end.
