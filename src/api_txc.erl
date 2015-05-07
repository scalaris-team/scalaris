%% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>,
%% @author Nico Kruber <kruber@zib.de>
%% @doc API for compressed transactional access to replicated DHT items.
%% Same as api_tx but transmits compressed values in both request and result
%% list.
%% @end
%% @version $Id$
-module(api_txc).
-author('schintke@zib.de').
-author('kruber@zib.de').
-vsn('$Id$').

-export_type([request/0, request_on_key/0,
              request_enc/0, request_on_key_enc/0,
              read_request/0, read_random_from_list_request/0,
              read_sublist_request/0,
              write_request/0,
              add_del_on_list_request/0, add_on_nr_request/0,
              test_and_set_request/0]).
-export_type([result/0, read_result/0, read_random_from_list_result/0,
              read_sublist_result/0,
              write_result/0, commit_result/0]).
-export_type([listop_result/0, numberop_result/0, testandset_result/0]).
-export_type([client_key/0]).

-include("api_tx.hrl").

%% @doc Perform several requests inside a transaction and monitors its
%%      execution time.
-spec req_list(tx_tlog:tlog(), [request()]) -> {tx_tlog:tlog(), [result()]}.
req_list(TLog, ReqList) ->
    {TimeInUs, Result} = util:tc(fun rdht_tx:req_list/3, [TLog, ReqList, false]),
    monitor:client_monitor_set_value(api_tx, 'req_list', TimeInUs / 1000),
    Result.
