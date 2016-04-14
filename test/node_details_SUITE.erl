%  @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Test suites for the node_details module.
%% @end
%% @version $Id$
-module(node_details_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

groups() ->
    [{tester_tests, [sequence],
      [tester_new0, tester_new9,
       tester_set_get_pred,
       tester_set_get_predlist,
       tester_set_get_node,
       tester_set_get_my_range,
       tester_set_get_succ,
       tester_set_get_succlist,
       tester_set_get_load,
       tester_set_get_hostname,
       tester_set_get_rt_size,
       tester_set_get_message_log,
       tester_set_get_memory]}
    ].

all() ->
    [
     {group, tester_tests}
    ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

-spec safe_compare(NodeDetails::node_details:node_details(),
                   Tag::node_details:node_details_name(),
                   ExpValue::node:node_type() |
                             nodelist:non_empty_snodelist() |
                             intervals:interval() |
                             node_details:load() |
                             node_details:hostname() |
                             node_details:rt_size() |
                             node_details:message_log() |
                             node_details:memory() |
                             unknown,
                   Unknown::[node_details:node_details_name()],
                   Known::[node_details:node_details_name()]) -> true.
safe_compare(NodeDetails, Tag, ExpValue, Unknown, Known) ->
    ValIsUnknown = (ExpValue =:= unknown) andalso
                       ((Known =:= [] andalso lists:member(Tag, Unknown)) orelse
                        (Unknown =:= [] andalso not lists:member(Tag, Known))),
    case ValIsUnknown of
        true -> ?equals_w_note(node_details:contains(NodeDetails, Tag), false, atom_to_list(Tag));
        _    -> ?equals_w_note(node_details:get(NodeDetails, Tag), ExpValue, atom_to_list(Tag))
    end.

%% @doc Compares NodeDetails with the given values. Either Unknown or Known
%%      must be non-empty. A value is unknown (and thus not part of the
%%      NodeDetails object) if its tag is either in Unknown and Known is emty
%%      or if its tag is not in Known and Unknown is empty.
-spec node_details_equals(NodeDetails::node_details:node_details(),
                          Pred::node:node_type() | unknown,
                          PredList::nodelist:non_empty_snodelist() | unknown,
                          Node::node:node_type() | unknown,
                          MyRange::intervals:interval() | unknown,
                          Succ::node:node_type() | unknown,
                          SuccList::nodelist:non_empty_snodelist() | unknown,
                          Load::node_details:load() | unknown,
                          Load2::node_details:load() | unknown,
                          Load3::node_details:load() | unknown,
                          Hostname::node_details:hostname() | unknown,
                          RTSize::node_details:rt_size() | unknown,
                          MsgLog::node_details:message_log() | unknown,
                          Memory::node_details:memory() | unknown,
                          Unknown::[node_details:node_details_name()],
                          Known::[node_details:node_details_name()]) -> true.
node_details_equals(NodeDetails, Pred, PredList, Node, MyRange, Succ, SuccList, Load, Load2, Load3, Hostname, RTSize, MsgLog, Memory, Unknown, Known) ->
    safe_compare(NodeDetails, pred, Pred, Unknown, Known),
    safe_compare(NodeDetails, predlist, PredList, Unknown, Known),
    safe_compare(NodeDetails, node, Node, Unknown, Known),
    safe_compare(NodeDetails, my_range, MyRange, Unknown, Known),
    safe_compare(NodeDetails, succ, Succ, Unknown, Known),
    safe_compare(NodeDetails, succlist, SuccList, Unknown, Known),
    safe_compare(NodeDetails, load, Load, Unknown, Known),
    safe_compare(NodeDetails, load2, Load2, Unknown, Known),
    safe_compare(NodeDetails, load3, Load3, Unknown, Known),
    safe_compare(NodeDetails, hostname, Hostname, Unknown, Known),
    safe_compare(NodeDetails, rt_size, RTSize, Unknown, Known),
    safe_compare(NodeDetails, message_log, MsgLog, Unknown, Known),
    safe_compare(NodeDetails, memory, Memory, Unknown, Known).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:new/0
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec test_new0() -> true.
test_new0() ->
    NodeDetails = node_details:new(),
    node_details_equals(NodeDetails, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], []).

tester_new0(Config) ->
    tester:test(node_details_SUITE, test_new0, 0, 10, [{threads, 2}]),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:new/7
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec test_new9(nodelist:non_empty_snodelist(), node:node_type(), nodelist:non_empty_snodelist(), node_details:load(), node_details:load2(), node_details:load3(), node_details:hostname(), node_details:rt_size(), node_details:memory()) -> true.
test_new9(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory) ->
    NodeDetails = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    node_details_equals(NodeDetails, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

tester_new9(Config) ->
    tester:test(node_details_SUITE, test_new9, 9, 10, [{threads, 2}]),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node_details:set/3 and node_details:get/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_set_get_pred(Pred::node:node_type()) -> true.
test_set_get_pred(PredTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, pred, PredTest),
    NodeDetails2_new = node_details:set(NodeDetails2, pred, PredTest),
    node_details_equals(NodeDetails1_new, PredTest, [PredTest], unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [pred, predlist]),
    node_details_equals(NodeDetails2_new, PredTest, [PredTest], Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_predlist(PredList::nodelist:non_empty_snodelist()) -> true.
test_set_get_predlist(PredListTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, predlist, PredListTest),
    NodeDetails2_new = node_details:set(NodeDetails2, predlist, PredListTest),
    node_details_equals(NodeDetails1_new, hd(PredListTest), PredListTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [pred, predlist]),
    node_details_equals(NodeDetails2_new, hd(PredListTest), PredListTest, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_node(Node::node:node_type()) -> true.
test_set_get_node(NodeTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, node, NodeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, node, NodeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, NodeTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [node]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, NodeTest, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_my_range(MyRange::intervals:interval()) -> true.
test_set_get_my_range(MyRangeTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, my_range, MyRangeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, my_range, MyRangeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, MyRangeTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [my_range]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, MyRangeTest, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [message_log], []).

-spec test_set_get_succ(Succ::node:node_type()) -> true.
test_set_get_succ(SuccTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, succ, SuccTest),
    NodeDetails2_new = node_details:set(NodeDetails2, succ, SuccTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, SuccTest, [SuccTest], unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [succ, succlist]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, SuccTest, [SuccTest], Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_succlist(SuccList::nodelist:non_empty_snodelist()) -> true.
test_set_get_succlist(SuccListTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, succlist, SuccListTest),
    NodeDetails2_new = node_details:set(NodeDetails2, succlist, SuccListTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, hd(SuccListTest), SuccListTest, unknown, unknown, unknown, unknown, unknown, unknown, unknown, [], [succ, succlist]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccListTest), SuccListTest, Load, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_load(Load::node_details:load()) -> true.
test_set_get_load(LoadTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, load, LoadTest),
    NodeDetails2_new = node_details:set(NodeDetails2, load, LoadTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, LoadTest, unknown, unknown, unknown, unknown, unknown, unknown, [], [load]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, LoadTest, Load2, Load3, Hostname, RTSize, unknown, Memory, [my_range, message_log], []).
                          
-spec test_set_get_hostname(Hostname::node_details:hostname()) -> true.
test_set_get_hostname(HostnameTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, hostname, HostnameTest),
    NodeDetails2_new = node_details:set(NodeDetails2, hostname, HostnameTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, HostnameTest, unknown, unknown, unknown, [], [hostname]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, HostnameTest, RTSize, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_rt_size(RTSize::node_details:rt_size()) -> true.
test_set_get_rt_size(RTSizeTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, rt_size, RTSizeTest),
    NodeDetails2_new = node_details:set(NodeDetails2, rt_size, RTSizeTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, RTSizeTest, unknown, unknown, [], [rt_size]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSizeTest, unknown, Memory, [my_range, message_log], []).

-spec test_set_get_message_log(MsgLog::node_details:message_log()) -> true.
test_set_get_message_log(MsgLogTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, message_log, MsgLogTest),
    NodeDetails2_new = node_details:set(NodeDetails2, message_log, MsgLogTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, MsgLogTest, unknown, [], [message_log]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, MsgLogTest, Memory, [my_range], []).

-spec test_set_get_memory(Memory::node_details:memory()) -> true.
test_set_get_memory(MemoryTest) ->
    NodeDetails1 = node_details:new(),
    Node = node:new(comm:this(), 0, 0), PredList = [Node], SuccList = [Node],
    Load = 0, Load2 = 0, Load3 = 0, Hostname = "localhost", RTSize = 0, Memory = 0,
    NodeDetails2 = node_details:new(PredList, Node, SuccList, Load, Load2, Load3, Hostname, RTSize, Memory),
    NodeDetails1_new = node_details:set(NodeDetails1, memory, MemoryTest),
    NodeDetails2_new = node_details:set(NodeDetails2, memory, MemoryTest),
    node_details_equals(NodeDetails1_new, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, unknown, MemoryTest, [], [memory]),
    node_details_equals(NodeDetails2_new, hd(PredList), PredList, Node, unknown, hd(SuccList), SuccList, Load, Load2, Load3, Hostname, RTSize, unknown, MemoryTest, [my_range, message_log], []).

tester_set_get_pred(Config) ->
    tester:test(node_details_SUITE, test_set_get_pred, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_predlist(Config) ->
    tester:test(node_details_SUITE, test_set_get_predlist, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_node(Config) ->
    tester:test(node_details_SUITE, test_set_get_node, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_my_range(Config) ->
    tester:test(node_details_SUITE, test_set_get_my_range, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_succ(Config) ->
    tester:test(node_details_SUITE, test_set_get_succ, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_succlist(Config) ->
    tester:test(node_details_SUITE, test_set_get_succlist, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_load(Config) ->
    tester:test(node_details_SUITE, test_set_get_load, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_hostname(Config) ->
    tester:test(node_details_SUITE, test_set_get_hostname, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_rt_size(Config) ->
    tester:test(node_details_SUITE, test_set_get_rt_size, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_message_log(Config) ->
    tester:test(node_details_SUITE, test_set_get_message_log, 1, 1000, [{threads, 2}]),
    Config.

tester_set_get_memory(Config) ->
    tester:test(node_details_SUITE, test_set_get_memory, 1, 1000, [{threads, 2}]),
    Config.
