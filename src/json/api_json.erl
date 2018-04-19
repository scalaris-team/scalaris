%% @copyright 2011-2018 Zuse Institute Berlin

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
%% @doc JSON API for transactional, consistent access to replicated DHT items
-module(api_json).
-author('schintke@zib.de').

-export([handler/2]).

%% for the api_json_* modules:
-export([tuple_list_to_json/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

%% main handler for json calls
-spec handler(atom(), list()) -> any().
handler(nop, [_Value]) -> "ok";

handler(range_read, [From, To])              -> api_json_dht_raw:range_read(From, To);
handler(delete, [Key])                       -> api_json_rdht:delete(Key);
handler(delete, [Key, Timeout])              -> api_json_rdht:delete(Key, Timeout);
handler(req_list, [Param])                   -> api_json_tx:req_list(Param);
handler(req_list, [TLog, ReqList])           -> api_json_tx:req_list(TLog, ReqList);
handler(read, [Key])                         -> api_json_tx:read(Key);
handler(write, [Key, Value])                 -> api_json_tx:write(Key, Value);
handler(add_del_on_list, [Key, ToAdd, ToRemove])
                                             -> api_json_tx:add_del_on_list(Key, ToAdd, ToRemove);
handler(add_on_nr, [Key, ToAdd])             -> api_json_tx:add_on_nr(Key, ToAdd);
handler(test_and_set, [Key, OldV, NewV])     -> api_json_tx:test_and_set(Key, OldV, NewV);
handler(req_list_commit_each, [Param])       -> api_json_tx:req_list_commit_each(Param);

handler(get_node_info, [])                   -> api_json_monitor:get_node_info();
handler(get_node_performance, [])            -> api_json_monitor:get_node_performance();
handler(get_service_info, [])                -> api_json_monitor:get_service_info();
handler(get_service_performance, [])         -> api_json_monitor:get_service_performance();

handler(get_replication_factor, [])          -> api_json_rt:get_replication_factor();

handler(rbr_read, [Key])                     -> api_json_rbr:read(Key);
handler(rbr_write, [Key, Value])             -> api_json_rbr:write(Key, Value);

handler(get_ring_size, [TimeOut])            -> api_json_ring:get_ring_size(TimeOut);
handler(wait_for_ring_size, [Size, TimeOut]) -> api_json_ring:wait_for_ring_size(Size, TimeOut);


handler(AnyOp, AnyParams) ->
    io:format("Unknown request = ~s:~p(~p)~n", [?MODULE, AnyOp, AnyParams]),
    {struct, [{failure, "unknownreq"}]}.

%% @doc Recursively converts 2-tuple lists to JSON objects.
-spec tuple_list_to_json([{Key::atom(), Value::term()}]) -> {struct, [{Key::atom(), Value::term()}]}.
tuple_list_to_json([]) -> [];
tuple_list_to_json([{Key, _} | _] = List) when is_atom(Key) ->
    {struct, [{KeyX, tuple_list_to_json(ValueX)} || {KeyX, ValueX} <- List]};
tuple_list_to_json(X) -> X.
