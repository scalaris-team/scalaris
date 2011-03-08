%% @copyright 2011 Zuse Institute Berlin

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
%% @version $Id$
-module(api_rdht).
-author('kruber@zib.de').
-vsn('$Id$').

-export([delete/1, delete/2]).
-include("scalaris.hrl").
-include("client_types.hrl").

-type(delete_result() ::
        {ok, ResultsOk::pos_integer(),
         ResultList::[ok | locks_set | undef]}
      | {fail, timeout, ResultsOk::pos_integer(),
         ResultList::[ok | locks_set | undef]}).

-spec delete(Key::client_key()) -> delete_result().
delete(Key) -> delete(Key, 2000).

%% @doc try to delete the given key and return a list of replicas
%%      successfully deleted.  WARNING: this function can lead to
%%      inconsistencies for api_tx functions.
-spec delete(client_key(), Timeout::pos_integer()) -> delete_result().
delete(Key, Timeout) ->
    ReplicaKeys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    _ = [ api_dht_raw:unreliable_lookup(
            Replica, {delete_key, comm:this(), Replica})
          || Replica <- ReplicaKeys],
    erlang:send_after(Timeout, self(), {timeout}),
    delete_collect_results(ReplicaKeys, []).

%% @doc collect the response for the delete requests
-spec delete_collect_results(ReplicaKeys::[?RT:key()],
                             Results::[ok | locks_set | undef])
                            -> delete_result().
delete_collect_results([], Results) ->
    OKs = length([ok || R <- Results, R =:= ok]),
    {ok, OKs, Results};
delete_collect_results(ReplicaKeys, Results) ->
    receive
        {delete_key_response, Key, Result} ->
            case lists:member(Key, ReplicaKeys) of
                true ->
                    delete_collect_results(lists:delete(Key, ReplicaKeys),
                                           [Result | Results]);
                false ->
                    delete_collect_results(ReplicaKeys, Results)
            end;
        {timeout} ->
            OKs = length([ok || R <- Results, R =:= ok]),
            {fail, timeout, OKs, Results}
    end.
