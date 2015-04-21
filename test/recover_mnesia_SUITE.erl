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

-define(NUM_EXECUTIONS, 5).
-define(CLOSE, close).

all() -> [
    {group, make_ring_group},
    {group, remove_node_group}
  ].
groups() ->
  [
    {make_ring_group, [sequence], [tester_ring, tester_write, {group, recover_data_group}]},
    {recover_data_group, [sequence, {repeat, ?NUM_EXECUTIONS}], [tester_read]},
    {remove_node_group, [sequence], [tester_write, {group, remove_node}]},
    {remove_node, [sequence, {repeat, ?NUM_EXECUTIONS}], [tester_remove_node]}
  ].

suite() -> [ {timetrap, {seconds, 60}} ].

init_per_suite(Config) ->
  case config:read(db_backend) of
    db_mnesia -> unittest_helper:init_per_suite(Config);
    _ -> {skip, "db_mnesia not set -> skipping test SUITE"}
  end.

end_per_suite(Config) ->
  unittest_helper:end_per_suite(Config).

init_per_group(Group, Config) ->
  case config:read(db_backend) of
    db_mnesia -> 
      case Group of
        recover_data_group ->
          unittest_helper:init_per_group(Group, Config);
        remove_node ->
          unittest_helper:init_per_group(Group, Config);
        _ ->
          %% stop ring and clean repository from previous test case (it may have run into a timeout)
          unittest_helper:stop_ring(),
          application:stop(mnesia),
          %% need config to get db path
          Config2 = unittest_helper:start_minimal_procs(Config, [], false),
          PWD = os:cmd(pwd),
          WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
             "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
          file:delete(WorkingDir ++ "schema.DAT"),
          unittest_helper:stop_minimal_procs(Config2),

          {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
          unittest_helper:make_ring(4, [{config, [{log_path, PrivDir},
            {leases, true}]}]),
          unittest_helper:check_ring_size_fully_joined(4),
          unittest_helper:init_per_group(Group, Config)
      end;
    _ -> {skip, "db_mnesia not set -> skipping test group"}
  end.

end_per_group(Group, Config) ->
  case Group of
    recover_data_group ->
      unittest_helper:end_per_group(Group, Config);
    remove_node ->
      unittest_helper:end_per_group(Group, Config);
    _ ->
      % stop ring, stop mnesia and clean repository
      PWD = os:cmd(pwd),
      WorkingDir = string:sub_string(PWD, 1, string:len(PWD) - 1) ++
          "/" ++ config:read(db_directory) ++ "/" ++ atom_to_list(erlang:node()) ++ "/",
      Tabs = lists:delete(schema, mnesia:system_info(tables)),
      unittest_helper:stop_ring(),
      application:stop(mnesia),
      [ok = file:delete(WorkingDir ++ atom_to_list(X)++".DCD")||X<-Tabs],
      ok = file:delete(WorkingDir ++ "schema.DAT"),
      unittest_helper:end_per_group(Group, Config)
   end.

init_per_testcase(_TestCase, Config) ->
  Config.

end_per_testcase(_TestCase, _Config) ->
  ok.

rw_suite_runs(N) ->
  erlang:min(N, 200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test create_ring/1 of mnesia recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec tester_ring([db_backend_beh:entry()]) -> true.
tester_ring(Config) ->
  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
  unittest_helper:stop_ring(),
  % wait for leases to expire
  timer:sleep(11000),
  unittest_helper:make_ring_recover([{config, [{log_path, PrivDir},
                                     {leases, true},
                                     {start_type, recover}]}]),
  util:wait_for(fun admin:check_leases/0, 10000),
  true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test write/1 write data to KV DBs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tester_write(_Config) ->
  % write data to KV
  [kv_on_cseq:write(integer_to_list(X),X)||X<-lists:seq(1,100)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test read/1 ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tester_read(Config) ->
  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
  unittest_helper:stop_ring(),
  % wait for leases to expire
  timer:sleep(11000),
  unittest_helper:make_ring_recover( [{config, [{log_path, PrivDir},
    {leases, true},
    {start_type, recover}]}]),
  util:wait_for(fun admin:check_leases/0, 10000),
  % ring restored -> checking KV data integrity
  [{ok, X} = kv_on_cseq:read(integer_to_list(X))||X<-lists:seq(1,100)],
  true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% test remove_node/1 remove a node and ensure data integrity after recovery
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tester_remove_node(_Config) ->
  Tabs = lists:delete(schema, mnesia:system_info(tables)),
  % delete random node from ring
  Node_name = string:sub_word(atom_to_list(lists:last(Tabs)), 2, $:),
  Node_tabs = [ Tab || Tab <- Tabs, string:sub_word(atom_to_list(Tab), 2, $:) =:= Node_name ],
  {[Node_name], _Not_found} = admin:del_nodes_by_name([Node_name], false),
  % wait for leases to expire
  timer:sleep(11000),
  [{atomic, ok} = mnesia:delete_table(X)||X<-Node_tabs],
  util:wait_for(fun admin:check_leases/0, 10000),
  % check data integrity
  [{ok, X} = kv_on_cseq:read(integer_to_list(X))||X<-lists:seq(1,100)],
  % add node to reform 4 node ring
  admin:add_nodes(1),
  timer:sleep(3000),
  unittest_helper:check_ring_size_fully_joined(4),
  true.
