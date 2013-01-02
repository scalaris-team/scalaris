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

%% @author Thorsten Schuett <schuett@zib.de>
%% @version $Id$
-module(l_on_cseq_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all()   -> [
            test_renew_with_concurrent_renew
            %test_renew_with_concurrent_owner_change
           ].
suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

test_renew_with_concurrent_renew(_Config) ->
    %ct:pal("starting test_renew~n", []),
    DHTNode = pid_groups:find_a(dht_node),
    pid_groups:join(pid_groups:group_with(dht_node)),

    % intercept lease renew
    M = {l_on_cseq, renew, Old} = intercept_lease_renew(),
    % now we update the lease
    OldVersion = l_on_cseq:get_version(Old),
    OldEpoch   = l_on_cseq:get_epoch(Old),
    Id         = l_on_cseq:get_id(Old),
    New = l_on_cseq:set_timeout(
            l_on_cseq:set_version(Old, l_on_cseq:get_version(Old)+1)),
    %ct:pal("write new lease~n", []),
    l_on_cseq:lease_update(Old, New),
    %ct:pal("wait for change~n", []),
    wait_for_lease(New),
    % now the error handling of lease_renew is going to be tested
    %ct:pal("sending message ~p~n", [M]),
    comm:send_local(DHTNode, M),
    wait_for_lease_version(Id, OldEpoch, OldVersion+2),
    %timer:sleep(1000),
    true.

wait_for_lease(Lease) ->
    %DHTNode = pid_groups:find_a(dht_node),
    Id = l_on_cseq:get_id(Lease),
    wait_for_lease_helper(Id, fun (L) -> L == Lease end).

wait_for_lease_version(Id, Epoch, Version) ->
    wait_for_lease_helper(Id,
                          fun (Lease) ->
                                  Epoch   == l_on_cseq:get_epoch(Lease)
                          andalso Version == l_on_cseq:get_version(Lease)
                          end).

wait_for_lease_helper(Id, F) ->
    case l_on_cseq:read(Id) of
        {ok, Lease} ->
            case F(Lease) of
                true ->
                    ok;
                false ->
                    wait_for_lease_helper(Id, F)
            end;
        _ ->
            wait_for_lease_helper(Id, F)
    end.

intercept_lease_renew() ->
    DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_renew(self()), block_renew),
    Msg = receive
              M = {l_on_cseq, renew, _Lease} ->
                  M
          end,
    ct:pal("intercepted renew request ~p~n", [Msg]),
    gen_component:bp_set_cond(DHTNode, block_trigger(self()), block_trigger),
    gen_component:bp_del(DHTNode, block_renew),
    Msg.


block_renew(Pid) ->
    fun (Message, _State) ->
            %ct:pal("called block_renew~n", []),
            case Message of
                {l_on_cseq, renew, _Lease} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_trigger(Pid) ->
    fun (Message, _State) ->
            %ct:pal("called block_renew~n", []),
            case Message of
                {l_on_cseq, renew_leases} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.
