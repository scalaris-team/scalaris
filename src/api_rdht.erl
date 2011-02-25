%% @copyright 2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc API for inconsistent access to the replicated DHT items. This
%%      is not compatible with the api_tx functions.
%% @version $Id: cs_api_v2.erl 1325 2010-12-27 13:04:53Z kruber@zib.de $
-module(api_rdht).
-author('kruber@zib.de').
-vsn('$Id: cs_api_v2.erl 1325 2010-12-27 13:04:53Z kruber@zib.de $').

-export([delete/1]).
-include("scalaris.hrl").
-include("client_types.hrl").

-spec delete(Key::client_key())
        -> {ok, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, timeout} |
           {fail, timeout, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, node_not_found}.
delete(Key) ->
    delete(Key, 2000).

%% @doc try to delete the given key and return a list of replicas
%%      successfully deleted.  WARNING: this function can lead to
%%      inconsistencies for api_tx functions.
-spec delete(client_key(), Timeout::pos_integer())
        -> {ok, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, timeout} |
           {fail, timeout, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, node_not_found}.
delete(Key, Timeout) ->
    case pid_groups:find_a(dht_node) of
        failed       -> {fail, node_not_found};
        LocalDHTNode ->
            LocalDHTNode ! {delete, comm:this(), Key},
            receive {delete_result, Result} -> Result
            after Timeout -> {fail, timeout}
            end
    end.
