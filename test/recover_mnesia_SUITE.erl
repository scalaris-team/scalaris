% @copyright 2015 Zuse Institute Berlin

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

%% @author Tanguy Racinet <tanracinet@gmail.com>
%% @doc    Unit tests for the mnesia recovery.
%% @end
%% @version $Id$
-module(recover_mnesia_SUITE).

-include("scalaris.hrl").
-include("unittest.hrl").

-author('tanracinet@gmail.com').
-vsn('$Id$').

-compile(export_all).

-define(CLOSE, close).
-define(EQ, ==).

all() -> [
  tester_ring_8,
  tester_with_data].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
  unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
  unittest_helper:end_per_suite(Config).

-ifdef(PRBR_MNESIA).
init_per_testcase(_TestCase, Config) ->
  % make sure ring is stopped, previous test may have run into timeout
  unittest_helper:stop_ring(),
  application:stop(mnesia),
  file:delete("/home/tanguy/scalaris-dev/data/recover_mnesia_SUITE_ct@127.0.0.1/schema.DAT"),

  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
  unittest_helper:make_ring(8, [{config, [{log_path, PrivDir},
                                {leases, true}]}]),
  unittest_helper:check_ring_size_fully_joined(8),
  Config.
-else.
init_per_testcase(_TestCase, _Config) -> {skip, "db_mnesia not set -> skipping test SUITE"}.
-endif.

end_per_testcase(_TestCase, Config) ->
  Tabs = lists:delete(schema, mnesia:system_info(tables)),
  unittest_helper:stop_ring(),
  application:stop(mnesia),
  [ok = file:delete("/home/tanguy/scalaris-dev/data/recover_mnesia_SUITE_ct@127.0.0.1/"++atom_to_list(X)++".DCD")||X<-Tabs],
  ok = file:delete("/home/tanguy/scalaris-dev/data/recover_mnesia_SUITE_ct@127.0.0.1/schema.DAT"),
  Config.

rw_suite_runs(N) ->
  erlang:min(N, 200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test create_ring_8/1 of mnesia recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec tester_ring_8([db_backend_beh:entry()]) -> true.
tester_ring_8(Config) ->
  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
  unittest_helper:stop_ring(),
  ct:pal("ring stopped -> sleeping. Waiting for leases to expire"),
  timer:sleep(11000),
  unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
                                      {leases, true},
                                      {start_type, recover}]}]),
  util:wait_for(fun admin:check_leases/0, 10000),
  true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test with_data/1 write data to KV DBs and ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tester_with_data(Config) ->
  % writting data to KV
  [kv_on_cseq:write(integer_to_list(X),X)||X<-lists:seq(1,100)],
  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
  unittest_helper:stop_ring(),
  ct:pal("ring stopped -> sleeping. Waiting for leases to expire"),
  timer:sleep(11000),
  unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
    {leases, true},
    {start_type, recover}]}]),
  util:wait_for(fun admin:check_leases/0, 10000),
  % ring restored -> checking KV data integrity
  [{ok, X} = kv_on_cseq:read(integer_to_list(X))||X<-lists:seq(1,100)],
  true.

