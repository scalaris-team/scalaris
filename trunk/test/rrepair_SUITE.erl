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
    [get_symmetric_keys_test].

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
    set_rrepair_config_parameter(),
    %Build ring with NodeCount symmetric nodes
    unittest_helper:make_ring_with_ids(
      fun() ->  get_symmetric_keys(NodeCount) end,
      [{config, [{log_path, PrivDir}, {dht_node, mockup_dht_node}]}]),
    % wait for all nodes to finish their join 
    unittest_helper:check_ring_size_fully_joined(NodeCount),

    %% write some data (use a function because left-over tx_timeout messages can disturb the tests):
    Pid = erlang:spawn(fun() ->
                               _ = [api_tx:write(erlang:integer_to_list(X), X) || X <- lists:seq(1, 100)]
                       end),
    unittest_helper:wait_for_process_to_die(Pid),
    timer:sleep(500), % wait a bit for the rm-processes to settle
    ok.

fill_symmetric_ring(DataCount, NodeCount) ->
    NodeIds = lists:sort(get_symmetric_keys(NodeCount)),
    utils:for_to(1, 
		 NodeCount div 4, 
		 fun(I) ->
			 FirstKey = lists:nth(I, NodeIds) - (DataCount + 1),
			 %write DataCount-items to nth-Node and its symmetric replicas
			 utils:for_to(FirstKey, 
				      FirstKey + DataCount, 
				      fun(Key) ->
					      _RepKeys = ?RT:get_replica_keys(Key),
					      %TODO WRITE VALUES
					      ok
				      end)
		 end),
    ok.

set_rrepair_config_parameter() ->
    %stop trigger
    config:write(rep_update_interval, 100000000000),
    ok.

end_per_testcase(_TestCase, _Config) ->
    %error_logger:tty(false),
    unittest_helper:stop_ring(),
    ok.

simpleBloomSync(Config) ->
    build_symmetric_ring(4, Config),
    ok.
