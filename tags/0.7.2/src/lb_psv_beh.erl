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
%% @doc    Passive load balancing algorithm behavior
%% @end
%% @version $Id$
-module(lb_psv_beh).
-author('kruber@zib.de').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

%% userdevguide-begin rt_beh:behaviour
-ifdef(have_callback_support).
-include("scalaris.hrl").

-callback get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
-callback get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
-callback create_join(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                      SourcePid::comm:mypid(), Conn::dht_node_join:connection())
        -> dht_node_state:state().
-callback sort_candidates(Ops::[lb_op:lb_op()]) -> [lb_op:lb_op()].
-callback process_join_msg(comm:message(), LbPsvState::term(),
                           DhtNodeState::dht_node_state:state())
        -> dht_node_state:state() | unknown_event.

-callback check_config() -> boolean().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % at the joining node, get the number of IDs to sample
     {get_number_of_samples, 1},
     % at an existing node, send the number of IDs to sample to a joining node
     {get_number_of_samples_remote, 2},
     % at an existing node, simulate a join operation
     {create_join, 4},
     % sort a list of candidates so that the best ones are at the front of the list 
     {sort_candidates, 1},
     % process join messages with signature {Msg, {join, LbPsv, LbPsvState}}
     {process_join_msg, 3},
     % common methods
     {check_config, 0}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
%% userdevguide-end rt_beh:behaviour
