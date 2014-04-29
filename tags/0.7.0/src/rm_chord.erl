% @copyright 2008-2014 Zuse Institute Berlin

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
%% @doc    Chord-like ring maintenance
%% @end
%% @version $Id$
-module(rm_chord).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(rm_beh).

-opaque state() :: {Neighbors :: nodelist:neighborhood()}.

% accepted messages of an initialized rm_chord process in addition to rm_loop
-type(custom_message() ::
    {rm, get_succlist, Source_Pid::comm:mypid()} |
    {rm, {get_node_details_response, NodeDetails::node_details:node_details()}, from_succ | from_node} |
    {rm, get_succlist_response, Succ::node:node_type(), SuccsSuccList::nodelist:non_empty_snodelist()}).

-define(SEND_OPTIONS, [{channel, prio}, {?quiet}]).

% note include after the type definitions for erlang < R13B04!
-include("rm_beh.hrl").

-spec get_neighbors(state()) -> nodelist:neighborhood().
get_neighbors({Neighbors}) ->
    Neighbors.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Nothing to do.
-spec init_first() -> ok.
init_first() ->
    ok.

%% @doc Initialises the state when rm_loop receives an init_rm message.
-spec init(Me::node:node_type(), Pred::node:node_type(),
           Succ::node:node_type()) -> state().
init(Me, Pred, Succ) ->
    Neighborhood = nodelist:new_neighborhood(Pred, Me, Succ),
    get_successorlist(node:pidX(Succ)),
    {Neighborhood}.

-spec unittest_create_state(Neighbors::nodelist:neighborhood()) -> state().
unittest_create_state(Neighbors) ->
    {Neighbors}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler when the module is fully initialized.
-spec handle_custom_message(custom_message(), state())
        -> {ChangeReason::rm_loop:reason(), state()} | unknown_event.
handle_custom_message({rm, get_succlist, Source_Pid}, {Neighborhood} = State) ->
    comm:send(Source_Pid, {rm, get_succlist_response,
                           nodelist:node(Neighborhood),
                           nodelist:succs(Neighborhood)},
              ?SEND_OPTIONS),
    {{unknown}, State};

% got node_details from our successor
handle_custom_message({rm, {get_node_details_response, NodeDetails}, from_succ}, State)  ->
    SuccsPred = node_details:get(NodeDetails, pred),
    comm:send(node:pidX(SuccsPred),
              {get_node_details,
               comm:reply_as(comm:this(), 2, {rm, '_', from_node}), [node, is_leaving]},
              ?SEND_OPTIONS),
    {{unknown}, State};

% we asked another node we wanted to add for its node object -> now add it
% (if it is not in the process of leaving the system)
handle_custom_message({rm, {get_node_details_response, NodeDetails}, from_node},
   {OldNeighborhood} = State)  ->
    case node_details:get(NodeDetails, is_leaving) of
        false ->
            Node = node_details:get(NodeDetails, node),
            NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [Node],
                                                 predListLength(), succListLength()),
            OldSucc = nodelist:succ(OldNeighborhood),
            NewSucc = nodelist:succ(NewNeighborhood),
            %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
            case OldSucc =/= NewSucc of
                true ->
                    get_successorlist(node:pidX(NewSucc)),
                    rm_loop:notify_new_pred(node:pidX(NewSucc),
                                            nodelist:node(NewNeighborhood));
                false -> ok
            end,
            {{node_discovery}, {NewNeighborhood}};
        true  -> {{unknown}, State}
    end;

handle_custom_message({rm, get_succlist_response, Succ, SuccsSuccList},
   {OldNeighborhood} = State) ->

    NewNeighborhood = nodelist:add_nodes(OldNeighborhood, [Succ | SuccsSuccList],
                                         predListLength(), succListLength()),
    OldView = nodelist:to_list(OldNeighborhood),
    NewView = nodelist:to_list(NewNeighborhood),
    ViewOrd = fun(A, B) ->
                      nodelist:succ_ord_node(A, B, nodelist:node(OldNeighborhood))
              end,
    {_, _, NewNodes} = util:ssplit_unique(OldView, NewView, ViewOrd),
    contact_new_nodes(NewNodes),
    {{unknown}, State};

handle_custom_message(_, _State) -> unknown_event.

-spec trigger_action(State::state())
        -> {ChangeReason::rm_loop:reason(), state()}.
trigger_action({Neighborhood} = State) ->
    % new stabilization interval
    case nodelist:has_real_succ(Neighborhood) of
        true -> comm:send(node:pidX(nodelist:succ(Neighborhood)),
                          {get_node_details,
                           comm:reply_as(comm:this(), 2, {rm, '_', from_succ}), [pred]},
                          ?SEND_OPTIONS);
        _    -> ok
    end,
    {{unknown}, State}.

-spec new_pred(State::state(), NewPred::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
new_pred({OldNeighborhood}, NewPred) ->
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewPred,
                                        predListLength(), succListLength()),
    {{unknown}, {NewNeighborhood}}.

-spec new_succ(State::state(), NewSucc::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
new_succ({OldNeighborhood}, NewSucc) ->
    NewNeighborhood = nodelist:add_node(OldNeighborhood, NewSucc,
                                        predListLength(), succListLength()),
    {{unknown}, {NewNeighborhood}}.

%% @doc Removes the given predecessor as a result from a graceful leave only!
-spec remove_pred(State::state(), OldPred::node:node_type(),
                  PredsPred::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
remove_pred({OldNeighborhood}, OldPred, PredsPred) ->
    NewNbh1 = nodelist:remove(OldPred, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, PredsPred, predListLength(), succListLength()),
    {{graceful_leave, pred, OldPred}, {NewNbh2}}.

%% @doc Removes the given successor as a result from a graceful leave only!
-spec remove_succ(State::state(), OldSucc::node:node_type(),
                  SuccsSucc::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
remove_succ({OldNeighborhood}, OldSucc, SuccsSucc) ->
    NewNbh1 = nodelist:remove(OldSucc, OldNeighborhood),
    NewNbh2 = nodelist:add_node(NewNbh1, SuccsSucc, predListLength(), succListLength()),
    {{graceful_leave, succ, OldSucc}, {NewNbh2}}.

-spec update_node(State::state(), NewMe::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
update_node({OldNeighborhood}, NewMe) ->
    NewNeighborhood = nodelist:update_node(OldNeighborhood, NewMe),
    % inform neighbors
    trigger_action({NewNeighborhood}).

-spec contact_new_nodes(NewNodes::[node:node_type()]) -> ok.
contact_new_nodes(NewNodes) ->
    % TODO: add a local cache of contacted nodes in order not to contact them again
    ThisWithCookie = comm:reply_as(comm:this(), 2, {rm, '_', from_node}),
    case comm:is_valid(ThisWithCookie) of
        true ->
            _ = [begin
                     Msg = {get_node_details, ThisWithCookie, [node, is_leaving]},
                     comm:send(node:pidX(Node), Msg, ?SEND_OPTIONS)
                 end || Node <- NewNodes],
            ok;
        false -> ok
    end.

-spec leave(State::state()) -> ok.
leave(_State) -> ok.

% failure detector reported dead node
-spec crashed_node(State::state(), DeadPid::comm:mypid())
        -> {ChangeReason::rm_loop:reason(), state()}.
crashed_node({OldNeighborhood}, DeadPid) ->
    NewNeighborhood = nodelist:remove(DeadPid, OldNeighborhood),
    {{node_crashed, DeadPid}, {NewNeighborhood}}.

% dead-node-cache reported dead node to be alive again
-spec zombie_node(State::state(), Node::node:node_type())
        -> {ChangeReason::rm_loop:reason(), state()}.
zombie_node({OldNeighborhood}, Node) ->
    % this node could potentially be useful as it has been in our state before
    NewNeighborhood = nodelist:add_node(OldNeighborhood, Node,
                                        predListLength(), succListLength()),
    {{node_discovery}, {NewNeighborhood}}.

-spec get_web_debug_info(State::state()) -> [{string(), string()}].
get_web_debug_info(_State) -> [].

%% @doc Checks whether config parameters of the rm_chord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(stabilization_interval_base) and
    config:cfg_is_greater_than(stabilization_interval_base, 0) and

    config:cfg_is_integer(succ_list_length) and
    config:cfg_is_greater_than(succ_list_length, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @private

%% @doc Sends a message to the remote node's dht_node process asking for
%%      its list of successors.
-spec get_successorlist(comm:mypid()) -> ok.
get_successorlist(RemoteDhtNodePid) ->
    comm:send(RemoteDhtNodePid, {rm, get_succlist, comm:this()}, ?SEND_OPTIONS).

%% @doc the length of the successor list
-spec predListLength() -> pos_integer().
predListLength() -> 1.

%% @doc the length of the successor list
-spec succListLength() -> pos_integer().
succListLength() -> config:read(succ_list_length).

%% @doc the interval between two stabilization runs
-spec trigger_interval() -> pos_integer().
trigger_interval() -> config:read(stabilization_interval_base) div 1000.
