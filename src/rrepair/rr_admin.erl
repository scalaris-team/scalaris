% @copyright 2012 Zuse Institute Berlin

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

%% @author Maik Lange <lakedaimon300@googlemail.com>
%% @doc Administrative helper functions for replica repair evaluation
%% @version $Id:  $
-module(rr_admin).

-export([make_ring/2, 
         fill_ring/3,
         start_sync/0,
         reset/0,
         db_stats/0,
         set_recon_method/1]).

-export([test_bloom/1,
         test_regen/0]). %EVAL SETS

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type ring_type()    :: symmetric | random.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DBSizeKey, rr_admin_dbsize).    %Process Dictionary Key for generated db size
-define(REP_FACTOR, 4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Eval Quick Start
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_bloom(float()) -> ok.
test_bloom(Fpr) ->
    test_bloom(Fpr, update, 10, 10000).

-spec test_regen() -> ok.
test_regen() ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_bloom(Fpr, FType, Nodes, DBCount) ->
    Method = bloom,
    make_ring(symmetric, Nodes),
    InitStats = fill_ring(random, DBCount, [{ftype, FType}]),
    print_db_status(InitStats),
    IterCount = 5,
    _ = set_recon_method(Method),
    config:write(rr_bloom_fpr, Fpr),
    Result = util:for_to_ex(1, IterCount, 
                            fun(I) ->
                                    Stats = start_sync(),
                                    io:format("<<<<ROUND ~p/~p>>>>>>", [I, IterCount]),
                                    print_db_status(Stats),
                                    {I, Stats}
                            end,
                            [{0, InitStats}]),
    R = lists:map(fun({I, {_, _, _, O}}) -> [I, (?REP_FACTOR * DBCount) - O] end, 
                  lists:reverse(Result)),
    {{YY, MM, DD}, {Hour, Min, Sec}} = erlang:localtime(),
    eval_export:write_to_file(gnuplot, R, 
                              [{filename, io_lib:format("~p-~p-~p_~p-~p-~p_test_upd-~p", 
                                                        [YY, MM, DD, Hour, Min, Sec, Method])},
                               {caption, io_lib:format("~p rep ~p - fpr=~p", [Method, FType, Fpr])},
                               {comment, io_lib:format("Parameter: ftype=~p ; Nodes=~p ; DBSize=~p ; Fpr=~p", [FType, Nodes, DBCount, Fpr])},
                               {gnuplot_args, io_lib:format("u 1:2 t \"~p\" w lp, ~p t \"max\"", [Method, ?REP_FACTOR * DBCount])},
                               {set_yrange, io_lib:format("0:~p", [(?REP_FACTOR * DBCount) + 2000])},
                               {gp_xlabel, "round"},
                               {gp_ylabel, "up-to-date copies"}]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec make_ring(ring_type(), pos_integer()) -> ok.
make_ring(Type, Size) ->
    %if ring exists kill all   
    _ = set_recon_method(bloom),
    _ = case Type of
            random -> 
                _ = admin:add_node([{first}]),
                admin:add_nodes(Size -1);
            symmetric ->
                Ids = get_symmetric_ids(Size),
                _ = admin:add_node([{first}, {{dht_node, id}, hd(Ids)}]),
                [admin:add_node_at_id(Id) || Id <- tl(Ids)]
        end,    
    wait_for_stable_ring(),
    io:format("RING READY: ~p - ~p/~p Nodes~n", [Type, get_ring_size(), Size]),
    ok.

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_generator:db_type(), pos_integer(), [db_generator:db_parameter()]) -> db_generator:db_status().
fill_ring(Type, DBSize, Params) ->
    erlang:put(?DBSizeKey, ?REP_FACTOR * DBSize),
    db_generator:fill_ring(Type, DBSize, Params).

-spec reset() -> ok | failed.
reset() ->
    {_, NotFound} = api_vm:kill_nodes_by_name(api_vm:get_nodes()),
    erlang:length(NotFound) > 0 andalso
        erlang:error(some_nodes_not_found),
    util:wait_for(fun() -> api_vm:number_of_nodes() =:= 0 end),
    case api_vm:number_of_nodes() of
        0 -> ok;
        _ -> failed
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_sync() -> db_generator:db_status().
start_sync() ->
    Nodes = get_node_list(),
    io:format("--> START SYNC - ~p~n", [config:read(rr_recon_method)]),
    %start
    lists:foreach(fun(Node) ->
                          comm:send(Node, {send_to_group_member, rrepair, {rr_trigger}})
                  end, 
                  Nodes),
    %wait for end
    lists:foreach(
      fun(Node) -> 
              util:wait_for(
                fun() -> 
                        comm:send(Node, {send_to_group_member, rrepair, {get_state, comm:this(), open_sync}}),
                        receive
                            {get_state_response, Val} -> Val =:= 0
                        end
                end)
      end, 
      Nodes),
    get_db_status().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec db_stats() -> ok.
db_stats() ->
    S = get_db_status(),
    print_db_status(S),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec set_recon_method(rr_recon:method()) -> ok | {error, term()}.
set_recon_method(Method) ->
    config:write(rrepair_enabled, true),
    config:write(rr_trigger_interval, 100000000),
    config:write(rr_trigger, trigger_periodic),
    config:write(rr_recon_method, Method),
    config:write(rr_resolve_method, simple),
    config:write(rr_bloom_fpr, 0.01),
    config:write(rr_max_items, case Method of
                                   bloom -> 10000;
                                   _ -> 100000
                               end),
    config:write(rr_negotiate_sync_interval, case Method of
                                                         bloom -> false;
                                                         _ -> true
                                                     end),    
    
    RM = config:read(rr_recon_method),
    case RM =:= Method of
        true -> ok;
        _ -> {error, set_failed}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Analysis Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec print_db_status(db_generator:db_status()) -> ok.
print_db_status({Count, Inserted, Missing, Outdated}) ->
    io:format("Items=~p~nExisting=~p~nMissing=~p~nOutdated=~p~n",
              [Count, Inserted, Missing, Outdated]),
    ok.

-spec get_ring_size() -> non_neg_integer().
get_ring_size() ->
    api_vm:number_of_nodes().

-spec get_db_status() -> db_generator:db_status().
get_db_status() ->
    DBSize = erlang:get(?DBSizeKey),
    Ring = statistics:get_ring_details(),
    Stored = statistics:get_total_load(Ring),
    {DBSize, Stored, DBSize - Stored, count_outdated()}.

-spec count_outdated() -> non_neg_integer().
count_outdated() ->
    Req = {rr_stats, {count_old_replicas, comm:this(), intervals:all()}},
    lists:foldl(
      fun(Node, Acc) -> 
              comm:send(Node, {send_to_group_member, rrepair, Req}),
              receive
                  {count_old_replicas_reply, Old} -> Acc + Old
              end
      end, 
      0, get_node_list()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, N} -> N
        after 2000 ->
            log:log(error,"[ ST ] Timeout getting node list from mgmt server"),
            throw('mgmt_server_timeout')
    end.

-spec get_symmetric_ids(NodeCount::pos_integer()) -> [?RT:key(),...].
get_symmetric_ids(NodeCount) ->
    [element(2, intervals:get_bounds(I)) || I <- intervals:split(intervals:all(), NodeCount)].

-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    util:wait_for(fun() ->
                          R = admin:check_ring(),
                          R =:= ok
                  end, 500).
