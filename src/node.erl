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
%% @doc    node data structure + functions

%% @version $Id$
-module(node).
-author('schuett@zib.de').
-vsn('$Id$').

-export([id/1, id_version/1, pidX/1, yawsPort/1,
         new/3, null/0, is_valid/1,
         same_process/2, is_newer/2, newer/2, is_me/1,
         update_id/2]).
%% intervals between nodes
-export([mk_interval_between_ids/2, mk_interval_between_nodes/2]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export_type([node_type/0]).

-record(node, {pid        = ?required(node, pid)        :: comm:mypid(),
               id         = ?required(node, id)         :: ?RT:key(),
               id_version = ?required(node, id_version) :: non_neg_integer(),
               yaws_port  = config:read(yaws_port)      :: non_neg_integer()}).
-opaque(node_type() :: #node{}).

%% @doc Creates a new node.
-spec new(Pid::comm:mypid(), Id::?RT:key(), IdVersion::non_neg_integer()) -> node_type().
new(Pid, Id, IdVersion) -> #node{pid=Pid, id=Id, id_version=IdVersion}.

%% @doc Creates an invalid node.
-spec null() -> null.
null() -> null.

%% @doc Gets the pid of the node.
-spec pidX(node_type()) -> comm:mypid().
pidX(#node{pid=PID}) -> PID.

-spec yawsPort(node_type()) -> non_neg_integer().
yawsPort(#node{yaws_port = Port}) -> Port.

%% @doc Gets the node's ID.
-spec id(node_type()) -> ?RT:key().
id(#node{id=Id}) -> Id.

%% @doc Gets the version of the node's ID.
-spec id_version(node_type()) -> non_neg_integer().
id_version(#node{id_version=IdVersion}) -> IdVersion.

%% @doc Checks whether the given parameter is a valid node.
-spec is_valid(node_type()) -> true;
               (null | unknown) -> false.
is_valid(X) when is_record(X, node) -> true;
is_valid(_) -> false.

%% @doc Checks whether two nodes are the same, i.e. contain references to the.
%%      same process.
-spec same_process(node_type() | comm:mypid() | pid(), node_type()) -> boolean();
                  (node_type(), comm:mypid() | pid()) -> boolean();
                  (null | unknown, node_type() | comm:mypid() | pid()) -> false;
                  (node_type() | comm:mypid() | pid(), null | unknown) -> false.
same_process(Node1, Node2) when is_record(Node1, node) andalso is_record(Node2, node) ->
    pidX(Node1) =:= pidX(Node2);
same_process(Node1, Node2) when (Node1 =:= null) orelse (Node1 =:= unknown) orelse
                           (Node2 =:= null) orelse (Node2 =:= unknown) ->
    false;
same_process(Pid1, Node2) when is_record(Node2, node) andalso is_pid(Pid1) ->
    comm:make_global(Pid1) =:= pidX(Node2);
same_process(Pid1, Node2) when is_record(Node2, node) ->
    Pid1 =:= pidX(Node2);
same_process(Node1, Pid2) when is_record(Node1, node) andalso is_pid(Pid2)->
    pidX(Node1) =:= comm:make_global(Pid2);
same_process(Node1, Pid2) when is_record(Node1, node) ->
    pidX(Node1) =:= Pid2.

%% @doc Checks whether the given node is the same as the node associated with
%%      the requesting process.
-spec is_me(node_type() | null | unknown) -> boolean().
is_me(Node) ->
    same_process(Node, pid_groups:get_my(dht_node)).

%% @doc Determines whether Node1 is a newer instance of Node2.
%%      Note: Both nodes need to share the same PID, otherwise an exception of
%%      type 'throw' is thrown!
-spec is_newer(Node1::node_type(), Node2::node_type()) -> boolean().
is_newer(#node{pid=PID, id=Id1, id_version=IdVersion1},
         #node{pid=PID, id=Id2, id_version=IdVersion2}) ->
    if
        (IdVersion1 > IdVersion2) -> true;
        IdVersion1 < IdVersion2 -> false;
        Id1 =:= Id2 -> false;
        true -> throw('got two nodes with same IDversion but different ID')
    end.

%% @doc Creates an interval that covers all keys a node with MyKey is
%%      responsible for if his predecessor has PredKey, i.e. (PredKey, MyKey]
-spec mk_interval_between_ids(PredKey::?RT:key(), MyKey::?RT:key())
                             -> intervals:interval().
mk_interval_between_ids(Key, Key) ->
    intervals:all();
mk_interval_between_ids(PredKey, MyKey) ->
    intervals:new('(', PredKey, MyKey, ']').

%% @doc Creates an interval that covers all keys a node is responsible for given
%%      his predecessor, i.e. (node:id(PredKey), node:id(MyKey)]
-spec mk_interval_between_nodes(Pred::node:node_type(),
                                Node::node:node_type())
                               -> intervals:interval().
mk_interval_between_nodes(Pred, Node) ->
    mk_interval_between_ids(node:id(Pred), node:id(Node)).

%% @doc Determines the newer instance of two representations of the same node.
%%      Note: Both nodes need to share the same PID, otherwise an exception of
%%      type 'throw' is thrown!
-spec newer(node_type(), node_type()) -> node_type().
newer(Node1, Node2) ->
    case is_newer(Node1, Node2) of
        true -> Node1;
        _    -> Node2
    end.

%% @doc Updates the node's id and increases the id's version accordingly. 
-spec update_id(Node::node_type(), NewId::?RT:key()) -> node_type().
update_id(Node = #node{id_version=IdVersion}, NewId) ->
    Node#node{id=NewId, id_version=IdVersion + 1}.
