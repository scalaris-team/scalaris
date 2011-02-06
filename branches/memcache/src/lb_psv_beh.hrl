% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    Common types and function specs for passive load balancing algorithm
%%         implementations.
%% @end
%% @version $Id$

-include("scalaris.hrl").

-export([get_number_of_samples/1, get_number_of_samples_remote/2,
         create_join/4, sort_candidates/1,
         process_join_msg/3,
         check_config/0]).

-spec get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
-spec get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
-spec create_join(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                  SourcePid::comm:mypid(), Conn::dht_node_join:connection())
        -> dht_node_state:state().
-spec sort_candidates(Ops::[lb_op:lb_op()]) -> [lb_op:lb_op()].

-spec check_config() -> boolean().
