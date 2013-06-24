% @copyright 2012-2013 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @doc leases.
%% @end
%% @version $Id$
-module(leases).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([is_responsible/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_responsible(dht_node_state:state(), ?RT:key()) -> boolean() | maybe.
is_responsible(State, Key) ->
    {ActiveLeaseList, _} = dht_node_state:get(State, lease_list),
    is_responsible_(ActiveLeaseList, Key).
%    case lists:any(fun (Lease) ->
%                           intervals:in(Key, l_on_cseq:get_range(Lease))
%                               andalso l_on_cseq:is_valid(Lease)
%                   end, ActiveLeaseList) of
%        true -> true;
%        false ->
%            %log:log("is_responsible failed ~p ~p", [Key, ActiveLeaseList]),
%            false
%    end.

is_responsible_([], _Key) ->
    false;
is_responsible_([Lease|List], Key) ->
    RangeMatches = intervals:in(Key, l_on_cseq:get_range(Lease)),
    IsAlive = l_on_cseq:is_valid(Lease),
    if
        IsAlive andalso RangeMatches ->
            true;
        RangeMatches ->
            case is_responsible_(List, Key) of
                true ->
                    true;
                maybe ->
                    maybe;
                false ->
                    maybe
            end;
        true ->
            is_responsible_(List, Key)
    end.
