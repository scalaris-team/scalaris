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
%% @doc    Common types and function specs for routing table implementations.
%% @end
%% @version $Id$

-include("client_types.hrl").

-export_type([key/0, rt/0, custom_message/0, external_rt/0]).

-export([empty_ext/1, init/0, activate/1,
         hash_key/1, get_random_node_id/0, next_hop/3, succ/2,
         init_stabilize/2, update/3,
         filter_dead_node/3, to_pid_list/1, get_size/1, get_size_ext/1,
         get_replica_keys/1, get_replica_keys/2, get_key_segment/1,
         get_key_segment/2,
         n/0, get_range/2, get_random_in_interval/1, get_random_in_interval/2,
         get_split_key/3, get_split_keys/3,
         dump/1, to_list/1, export_rt_to_dht_node/2,
         handle_custom_message_inactive/2, handle_custom_message/2,
         check/5, check/6,
         check_config/0,
         client_key_to_binary/1,
         wrap_message/5,
         unwrap_message/2
     ]).

-spec client_key_to_binary(Key::client_key()) -> binary().
client_key_to_binary(Key) when is_binary(Key) ->
    Key;
client_key_to_binary(Key) ->
    case unicode:characters_to_binary(Key) of
        {incomplete, Encoded, Rest} ->
            RestBin = unicode:characters_to_binary(Rest, latin1),
            <<Encoded/binary, RestBin/binary>>;
        {error, Encoded, [Invalid | Rest]} when Invalid >= 16#D800 andalso Invalid =< 16#DFFF ->
            % nevertheless encode the invalid unicode character in range
            % 16#D800 to 16#DFFF
            <<X1, X2:2/binary>> = unicode:characters_to_binary([Invalid - 4096]),
            % map to the "correct" binary:
            RestBin = client_key_to_binary(Rest),
            <<Encoded/binary, (X1 + 1), X2/binary, RestBin/binary>>;
        {error, Encoded, [Invalid | Rest]} when Invalid =:= 16#FFFE orelse Invalid =:= 16#FFFF ->
            % nevertheless encode the invalid unicode character
            <<X1:2/binary, X2>> = unicode:characters_to_binary([Invalid - 2]),
            % map to the "correct" binary:
            RestBin = client_key_to_binary(Rest),
            <<Encoded/binary, X1/binary, (X2 + 1), RestBin/binary>>;
        Bin -> Bin
    end.

% note: can not use wrapper methods for all methods to make dialyzer happy
% about the opaque types since it doesn't recognize the module's own opaque
% type if returned by a method outside the module's scope, e.g. node:id/1.
% If required, e.g. a method is used internally and externally, use a wrapper
% in the implementing module.
