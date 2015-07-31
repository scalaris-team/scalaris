%  @copyright 2015 Zuse Institute Berlin

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
%% @doc API for using the routing table.
%%  @end
-module(api_rt).
-author('schuett@zib.de').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([get_replication_factor/0, get_evenly_spaced_keys/1, escaped_list_of_keys/1]).

-spec get_replication_factor() -> pos_integer().
get_replication_factor() ->
    config:read(replication_factor).

-spec get_evenly_spaced_keys(N::pos_integer()) -> list(?RT:key()).
get_evenly_spaced_keys(N) ->
    case config:read(replication_factor) of
        N ->
            ?RT:get_replica_keys(?MINUS_INFINITY);
        _ ->
            [?MINUS_INFINITY | 
             ?RT:get_split_keys(?MINUS_INFINITY, ?PLUS_INFINITY, N)]
    end.

-spec escaped_list_of_keys(list(?RT:key())) ->  string().
escaped_list_of_keys(Keys) ->
    tl(lists:foldl(fun (Key, S) ->
                        case erlang:is_list(Key) of
                            true ->
                                S ++ " " ++ util:escape_quotes(Key);
                            false ->
                                S ++ " " ++ erlang:integer_to_list(Key)
                        end
                end, "", Keys)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

