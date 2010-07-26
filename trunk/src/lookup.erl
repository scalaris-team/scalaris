%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin


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
%% @doc Public Lookup API; Reliable Lookup.
%% @end
%% @version $Id$
-module(lookup).
-author('schuett@zib.de').
-vsn('$Id$').

-export([unreliable_lookup/2,
         unreliable_get_key/1, unreliable_get_key/3]).

-include("scalaris.hrl").

%%%-----------------------Public API----------------------------------
%% userdevguide-begin lookup:lookup
-spec unreliable_lookup(Key::?RT:key(), Msg::comm:message()) -> ok.
unreliable_lookup(Key, Msg) ->
    comm:send_local(process_dictionary:get_group_member(dht_node),
                    {lookup_aux, Key, 0, Msg}).

-spec unreliable_get_key(Key::?RT:key()) -> ok.
unreliable_get_key(Key) ->
    unreliable_lookup(Key, {get_key, comm:this(), Key}).

-spec unreliable_get_key(CollectorPid::comm:mypid(),
                         ReqId::{rdht_req_id, {pos_integer(), comm:mypid()}},
                         Key::?RT:key()) -> ok.
unreliable_get_key(CollectorPid, ReqId, Key) ->
    unreliable_lookup(Key, {get_key, CollectorPid, ReqId, Key}).
%% userdevguide-end lookup:lookup

