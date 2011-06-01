%  @copyright 2010-2011 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    rrepair_SUITE.erl
%%% @author Maik Lange <malange@informatik.hu-berlin.de
%%% @doc    Tests for rep update module.
%%% @end
%%% Created : 2011-05-27 by Maik Lange
%%%-------------------------------------------------------------------
%% @version $Id $

-module(rrepair_SUITE).

-author('malange@informatik.hu-berlin.de').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [get_symmetric_keys_test,
     simpleBloomSync].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

-spec get_symmetric_keys(pos_integer()) -> [pos_integer()].			      
get_symmetric_keys(NodeCount) ->
    B = (16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF div NodeCount) + 1,
    util:for_to_ex(0, NodeCount - 1, fun(I) -> I*B end).

get_symmetric_keys_test(_) ->
    ToTest = get_symmetric_keys(4),
    ToBe = ?RT:get_replica_keys(0),
    ct:pal("GeneratedKeys = ~w~nRT-GetReplicaKeys = ~w", [ToTest, ToBe]),
    Equal = lists:foldl(fun(I, Acc) -> lists:member(I, ToBe) andalso Acc end, true, ToTest),
    ?equals(Equal, true),
    ok.

build_symmetric_ring(NodeCount, Config) ->
    % stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),    
    %Build ring with NodeCount symmetric nodes
    unittest_helper:make_ring_with_ids(
      fun() ->  get_symmetric_keys(NodeCount) end,
      [{config, [{log_path, PrivDir}, {dht_node, mockup_dht_node}]}]),
    % wait for all nodes to finish their join 
    unittest_helper:check_ring_size_fully_joined(NodeCount),
    % wait a bit for the rm-processes to settle
    timer:sleep(500), 
    set_rrepair_config_parameter(),
    ok.

fill_symmetric_ring(DataCount, NodeCount) ->
    NodeIds = lists:sort(get_symmetric_keys(NodeCount)),
    util:for_to(1, 
		NodeCount div 4, 
	        fun(I) ->
			FirstKey = lists:nth(I, NodeIds) + 1,
			%write DataCount-items to nth-Node and its symmetric replicas
			util:for_to(FirstKey, 
				    FirstKey + DataCount, 
				    fun(Key) ->					      
					    RepKeys = ?RT:get_replica_keys(Key),
					    %write replica group
					    lists:foreach(fun(X) -> 
								  DBEntry = db_entry:new(X, "2", 2),
								  %DBEntry = db_entry:new(X),
								  api_dht_raw:unreliable_lookup(X, 
												{set_key_entry, comm:this(), DBEntry}),
								  receive {set_key_entry_reply, _} -> ok end
							  end, 
							  RepKeys),
					    %random replica is outdated
					    OldKey = lists:nth(randoms:rand_uniform(1, length(RepKeys)), RepKeys),
					    api_dht_raw:unreliable_lookup(OldKey, {set_key_entry, comm:this(), db_entry:new(OldKey, "1", 1)}),
					    receive {set_key_entry_reply, _} -> ok end,					      
					    ok
				    end)
		end),
    ct:pal("[~w]-Nodes-Ring filled with [~w] items per node", [NodeCount, DataCount]),
    ok.

set_rrepair_config_parameter() ->
    %stop trigger
    config:write(rep_update_activate, false),
    config:write(rep_update_interval, 100000),
    ok.

end_per_testcase(_TestCase, _Config) ->
    %error_logger:tty(false),
    unittest_helper:stop_ring(),
    ok.

simpleBloomSync(Config) ->
    NodeCount = 4,
    build_symmetric_ring(NodeCount, Config),
    fill_symmetric_ring(10000, NodeCount),
    ok.
