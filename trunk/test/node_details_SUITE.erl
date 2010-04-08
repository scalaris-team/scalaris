%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    node_details_SUITE.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    TODO: Add description to node_details_SUITE
%%% @end
%%% Created : 7 Apr 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(node_details_SUITE).

-author('kruber@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("../include/scalaris.hrl").
-include_lib("unittest.hrl").

all() ->
    [tester_new0, tester_new7, tester_set_get].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    error_logger:tty(true),
    Owner = self(),
    Pid = spawn(fun () ->
                        crypto:start(),
                        process_dictionary:start_link(),
                        config:start_link(["scalaris.cfg"]),
                        comm_port:start_link(),
                        timer:sleep(1000),
                        comm_port:set_local_address({127,0,0,1},14195),
                        application:start(log4erl),
                        Owner ! {continue},
                        receive
                            {done} ->
                                ok
                        end
                end),
    receive
        {continue} ->
            ok
    end,
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    gen_component:kill(process_dictionary),
    error_logger:tty(false),
    exit(Pid, kill),
    Config.

-spec node_details_equals(NodeDetails::node_details:node_details(),
                          Pred::node_details:node_type(),
                          PredList::node_details:nodelist(),
                          Node::node_details:node_type(),
                          MyRange::node_details:my_range(),
                          Succ::node_details:node_type(),
                          SuccList::node_details:nodelist(),
                          Load::node_details:load(),
                          Hostname::node_details:hostname(),
                          RTSize::node_details:rt_size(),
                          MsgLog::node_details:message_log(),
                          Memory::node_details:memory()) -> true.
node_details_equals(NodeDetails, Pred, PredList, Node, MyRange, Succ, SuccList, Load, Hostname, RTSize, MsgLog, Memory) ->
    ?equals(node_details:get(NodeDetails, pred), Pred),
    ?equals(node_details:get(NodeDetails, predlist), PredList),
    ?equals(node_details:get(NodeDetails, node), Node),
    ?equals(node_details:get(NodeDetails, my_range), MyRange),
    ?equals(node_details:get(NodeDetails, succ), Succ),
    ?equals(node_details:get(NodeDetails, succlist), SuccList),
    ?equals(node_details:get(NodeDetails, load), Load),
    ?equals(node_details:get(NodeDetails, hostname), Hostname),
    ?equals(node_details:get(NodeDetails, rt_size), RTSize),
    ?equals(node_details:get(NodeDetails, message_log), MsgLog),
    ?equals(node_details:get(NodeDetails, memory), Memory),
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:new/0
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec test_new0() -> true.
test_new0() ->
    NodeDetails = node_details:new(),
    node_details_equals(NodeDetails, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown).

tester_new0(Config) ->
    tester:test(node_details_SUITE, test_new0, 0, 10),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:new/7
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec test_new7(node_details:nodelist(), node_details:node_type(), node_details:nodelist(), node_details:load(), node_details:hostname(), node_details:rt_size(), node_details:memory()) -> true.
test_new7(PredList, Node, SuccList, Load, Hostname, RTSize, Memory) ->
    NodeDetails = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    node_details_equals(NodeDetails, unknown, PredList, Node, unknown, unknown, SuccList, Load, Hostname, RTSize, unknown, Memory).

tester_new7(Config) ->
    tester:test(node_details_SUITE, test_new7, 7, 10),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:set/3 and node_details:get/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_set_get_pred(Pred::node_details:node_type()) -> true.
test_set_get_pred(PredTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, pred, PredTest),
    NodeDetails2_new = node_details:set(NodeDetails2, pred, PredTest),
    node_details_equals(NodeDetails1_new, PredTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, PredTest, PredList, Node, unknown, unknown, SuccList, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_predlist(PredList::node_details:nodelist()) -> true.
test_set_get_predlist(PredListTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, predlist, PredListTest),
    NodeDetails2_new = node_details:set(NodeDetails2, predlist, PredListTest),
    node_details_equals(NodeDetails1_new, unknown, PredListTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredListTest, Node, unknown, unknown, SuccList, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_node(Node::node_details:node_type()) -> true.
test_set_get_node(NodeTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, node, NodeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, node, NodeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, NodeTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, NodeTest, unknown, unknown, SuccList, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_my_range(MyRange::node_details:node_range()) -> true.
test_set_get_my_range(MyRangeTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, my_range, MyRangeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, my_range, MyRangeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, MyRangeTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, MyRangeTest, unknown, SuccList, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_succ(Succ::node_details:node_type()) -> true.
test_set_get_succ(SuccTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, succ, SuccTest),
    NodeDetails2_new = node_details:set(NodeDetails2, succ, SuccTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, SuccTest, unknown, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, SuccTest, SuccList, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_succlist(SuccList::node_details:nodelist()) -> true.
test_set_get_succlist(SuccListTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, succlist, SuccListTest),
    NodeDetails2_new = node_details:set(NodeDetails2, succlist, SuccListTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, SuccListTest, unknown, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccListTest, Load, Hostname, RTSize, unknown, Memory).

-spec test_set_get_load(Load::node_details:load()) -> true.
test_set_get_load(LoadTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, load, LoadTest),
    NodeDetails2_new = node_details:set(NodeDetails2, load, LoadTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, LoadTest, unknown, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccList, LoadTest, Hostname, RTSize, unknown, Memory).
                          
-spec test_set_get_hostname(Hostname::node_details:hostname()) -> true.
test_set_get_hostname(HostnameTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, hostname, HostnameTest),
    NodeDetails2_new = node_details:set(NodeDetails2, hostname, HostnameTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, HostnameTest, unknown, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccList, Load, HostnameTest, RTSize, unknown, Memory).

-spec test_set_get_rt_size(RTSize::node_details:rt_size()) -> true.
test_set_get_rt_size(RTSizeTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, rt_size, RTSizeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, rt_size, RTSizeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, RTSizeTest, unknown, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccList, Load, Hostname, RTSizeTest, unknown, Memory).

-spec test_set_get_message_log(MsgLog::node_details:message_log()) -> true.
test_set_get_message_log(MsgLogTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, message_log, MsgLogTest),
    NodeDetails2_new = node_details:set(NodeDetails2, message_log, MsgLogTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, MsgLogTest, unknown) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccList, Load, Hostname, RTSize, MsgLogTest, Memory).

-spec test_set_get_memory(Memory::node_details:memory()) -> true.
test_set_get_memory(MemoryTest) ->
    NodeDetails1 = node_details:new(),
    PredList = [], Node = node:new(cs_send:this(), 0), SuccList = [],
    Load = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, memory, MemoryTest),
    NodeDetails2_new = node_details:set(NodeDetails2, memory, MemoryTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, MemoryTest) andalso
    node_details_equals(NodeDetails2_new, unknown, PredList, Node, unknown, unknown, SuccList, Load, Hostname, RTSize, unknown, MemoryTest).

tester_set_get(Config) ->
    tester:test(node_details_SUITE, test_set_get_pred, 1, 10),
    tester:test(node_details_SUITE, test_set_get_predlist, 1, 10),
    tester:test(node_details_SUITE, test_set_get_node, 1, 10),
    tester:test(node_details_SUITE, test_set_get_my_range, 1, 10),
    tester:test(node_details_SUITE, test_set_get_succ, 1, 10),
    tester:test(node_details_SUITE, test_set_get_succlist, 1, 10),
    tester:test(node_details_SUITE, test_set_get_load, 1, 10),
    tester:test(node_details_SUITE, test_set_get_hostname, 1, 10),
    tester:test(node_details_SUITE, test_set_get_rt_size, 1, 10),
    tester:test(node_details_SUITE, test_set_get_message_log, 1, 10),
    tester:test(node_details_SUITE, test_set_get_memory, 1, 10),
    Config.
