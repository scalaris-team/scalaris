%  @copyright 2008-2011 Zuse Institute Berlin
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
%
%% @doc    Behavior for load modules in gossip_load
%% @end
%% @version $Id$
-module(gossip_load_beh).
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-export_type([load/0]).

-type load() :: number().

% Erlang version >= R15B
-ifdef(have_callback_support).

-callback get_load(node_details:node_details()) -> load().

-callback init_histo(node_details:node_details(), NumberOfBuckets::pos_integer())
                    -> gossip_load:histogram().

% Erlang version < R15B
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
  [
    {get_load, 1},
    {init_histo, 2}
  ];

behaviour_info(_Other) ->
  undefined.

-endif.
