%  @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Balance operation structure
%% @end
%% @version $Id$
-module(lb_op).
-author('kruber@zib.de').
-vsn('$Id$').

-export([no_op/0, slide_op/5, jump_op/7,
        is_no_op/1, is_slide/1, is_jump/1, get/2]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export_type([lb_op/0, id/0]).

-type id() :: any().

-type type() :: slide | jump.

-record(lb_op,
        {type       = ?required(lb_op, type)       :: type(),
         id         = ?required(lb_op, id)         :: id(),
         % first node involved (node that slides or moves)
         n1         = ?required(lb_op, n1)         :: node_details:node_details(),
         % successor of node1
         n1succ     = ?required(lb_op, n1succ)     :: node_details:node_details(),
         % node to move to if type == jump
         n3         = null                         :: node_details:node_details() | null,
         n1_new     = ?required(lb_op, n1_new)     :: node_details:node_details(),
         n1succ_new = ?required(lb_op, n1succ_new) :: node_details:node_details(),
         n3_new     = null                         :: node_details:node_details() | null
        }).
-opaque lb_op() :: #lb_op{} | no_op.

%% @doc Creates a no_op operation.
-spec no_op() -> lb_op().
no_op() -> no_op.

%% @doc Creates a new slide operation with the given nodes and their details
%%      after the slide.
-spec slide_op(Id::id(),
    Node::node_details:node_details(), Successor::node_details:node_details(),
    NodeNew::node_details:node_details(), SuccessorNew::node_details:node_details())
        -> lb_op().
slide_op(Id, Node, Successor, NodeNew, SuccessorNew) ->
    #lb_op{type = slide, id = Id,
           n1 = Node, n1succ = Successor,
           n1_new = NodeNew, n1succ_new = SuccessorNew}.

%% @doc Creates a new jump operation with the given nodes and their details
%%      after the jump.
-spec jump_op(Id::id(),
    NodeToMove::node_details:node_details(), NodeToMove_succ::node_details:node_details(), NodePosition::node_details:node_details(),
    NodeToMoveNew::node_details:node_details(), NodeToMove_succNew::node_details:node_details(), NodePositionNew::node_details:node_details())
        -> lb_op().
jump_op(Id, NodeToMove, NodeToMove_succ, NodePosition,
        NodeToMoveNew, NodeToMove_succNew, NodePositionNew) ->
    #lb_op{type = jump, id = Id,
           n1 = NodeToMove, n1succ = NodeToMove_succ, n3 = NodePosition,
           n1_new = NodeToMoveNew, n1succ_new = NodeToMove_succNew, n3_new = NodePositionNew}.

%% @doc Determines whether the operation is a no_op, i.e. no operation.
-spec is_no_op(Op::lb_op()) -> boolean().
is_no_op(no_op) -> true;
is_no_op(#lb_op{}) -> false.

%% @doc Determines whether the operation is a slide.
-spec is_slide(Op::lb_op()) -> boolean().
is_slide(no_op) -> false;
is_slide(Op) -> Op#lb_op.type =:= slide.

%% @doc Determines whether the operation is a jump.
-spec is_jump(Op::lb_op()) -> boolean().
is_jump(no_op) -> false;
is_jump(Op) -> Op#lb_op.type =:= jump.

%% @doc Gets the selected property from the load balancing operation.
-spec get(lb_op(), n1 | n1succ| n1_new | n1succ_new) -> node_details:node_details();
         (lb_op(), n3 | n3_new) -> node_details:node_details() | null;
         (lb_op(), id) -> id().
get(#lb_op{id=Id, n1=N1, n1succ=N1Succ, n3=N3,
           n1_new=N1New, n1succ_new=N1SuccNew, n3_new=N3New}, Key) ->
    case Key of
        id -> Id;
        n1 -> N1;
        n1succ -> N1Succ;
        n3 -> N3;
        n1_new -> N1New;
        n1succ_new -> N1SuccNew;
        n3_new -> N3New
    end.
