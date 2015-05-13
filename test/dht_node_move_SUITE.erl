% @copyright 2010-2013 Zuse Institute Berlin

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
%% @doc    Unit tests for src/dht_node_move.erl (slide and jump operations).
%% @end
%% @version $Id$
-module(dht_node_move_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

%% no proto scheduler for this suite
-define(proto_sched(_Action), ok).
%% number of slides without timeouts
-define(NUM_SLIDES, 50).

-include("dht_node_move_SUITE.hrl").

all() ->
    [
     {group, send_to_pred},
     {group, send_to_pred_incremental},
     {group, send_to_succ},
     {group, send_to_succ_incremental},
     {group, send_to_both},
     {group, send_to_both_incremental},
     {group, slide_illegally},
     {group, jump_slide}
    ].

suite() -> [ {timetrap, {seconds, 30}} ].

-spec additional_ring_config() -> [].
additional_ring_config() ->
    [].
