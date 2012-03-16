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
         db_stats/0,
         set_recon_method/1]).

-export([test_upd/0,
         test_regen/0,
         test_man/0]). %TEST SETS

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type ring_type()    :: symmetric | random.
-type failure_type() :: update | regen | mixed.
-type db_type()      :: wiki | random.
-type db_parameter() :: {ftype, failure_type()} |
                        {fprob, 0..100} |            %failure probability
                        {distribution, db_generator:distribution()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DBSizeKey, rr_admin_dbsize).    %Process Dictionary Key for generated db size
-define(ReplicationFactor, 4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Eval Quick Start
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec test_upd() -> ok.
test_upd() ->
    make_ring(symmetric, 10),
    fill_ring(random, 10000, []),
    ok.

-spec test_regen() -> ok.
test_regen() ->
    make_ring(symmetric, 10),
    fill_ring(random, 10000, [{ftype, regen}]),
    start_sync(),
    ok.

-spec test_man() -> ok.
test_man() ->
    make_ring(symmetric, 10),
    fill_ring(random, 10000, [{ftype, regen}]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec make_ring(ring_type(), pos_integer()) -> ok.
make_ring(Type, Size) ->
    %if ring exists kill all   
    set_recon_method(bloom), 
    case Type of
        random -> 
            admin:add_node([{first}]),
            admin:add_nodes(Size -1);
        symmetric ->
            Ids = get_symmetric_ids(Size),
            admin:add_node([{first}, {{dht_node, id}, hd(Ids)}]),
            [admin:add_node_at_id(Id) || Id <- tl(Ids)]
    end,
    wait_for_stable_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_type(), pos_integer(), [db_parameter()]) -> ok.
fill_ring(Type, DBSize, Params) ->
    erlang:put(?DBSizeKey, ?ReplicationFactor * DBSize),
    case Type of
        random -> fill_random(DBSize, Params);
        wiki -> fill_wiki(DBSize, Params)
    end.

fill_random(DBSize, Params) ->    
    Distr = proplists:get_value(distribution, Params, uniform),            
    I = hd(intervals:split(intervals:all(), ?ReplicationFactor)),
    Keys = db_generator:get_db(I, DBSize, Distr),    
    insert_random_db(Keys, Params),
    ok.
fill_wiki(DBSize, Params) ->
    %TODO
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_sync() -> ok.
start_sync() ->
    Nodes = get_node_list(),
    io:format("NodeS=~p", [Nodes]),
    %start
    lists:foreach(fun(Node) ->
                          comm:send(Node, {send_to_group_member, rep_upd, {rep_update_trigger}})
                  end, 
                  Nodes),
    %wait for end
    lists:foreach(
      fun(Node) -> 
              util:wait_for(
                fun() -> 
                        comm:send(Node, {send_to_group_member, rep_upd, {get_state, comm:this(), open_sync}}),
                        receive
                            {get_state_response, Val} -> Val =:= 0
                        end
                end)
      end, 
      Nodes),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec db_stats() -> ok.
db_stats() ->
    DBSize = erlang:get(?DBSizeKey),
    Ring = statistics:get_ring_details(),
    Stored = statistics:get_total_load(Ring),
    io:format("Replica Status~nStored/Dest/Missing~n~p/~p/~p~n", 
              [Stored, DBSize, DBSize - Stored]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec set_recon_method(rep_upd_recon:method()) -> ok | {error, term()}.
set_recon_method(Method) ->
    config:write(rep_update_activate, true),
    config:write(rep_update_interval, 100000000),
    config:write(rep_update_trigger, trigger_periodic),
    config:write(rep_update_recon_method, Method),
    config:write(rep_update_resolve_method, simple),
    config:write(rep_update_recon_fpr, 0.01),
    config:write(rep_update_max_items, case Method of
                                           bloom -> 10000;
                                           _ -> 100000
                                       end),
    config:write(rep_update_negotiate_sync_interval, case Method of
                                                         bloom -> false;
                                                         _ -> true
                                                     end),    
    
    RM = config:read(rep_update_recon_method),
    case RM =:= Method of
        true -> ok;
        _ -> {error, set_failed}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DB Generation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Inserts a list of keys replicated into the ring
-spec insert_random_db([?RT:key()], [db_parameter()]) -> ok.
insert_random_db(Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),    
    lists:foreach(
      fun(Key) ->
              RepKeys = ?RT:get_replica_keys(Key),
              %insert error?
              EKey = case FProb >= randoms:rand_uniform(1, 100) of
                         true -> util:randomelem(RepKeys);
                         _ -> null
                     end,
              %insert regen error
              {EType, WKeys} = case EKey =/= null of
                                   true ->
                                       EEType = case FType of 
                                                    mixed -> 
                                                        case randoms:rand_uniform(1, 2) of
                                                            1 -> update;
                                                            _ -> regen
                                                        end;
                                                    _ -> FType
                                                end,
                                       {EEType, 
                                        case EEType of
                                            regen -> [X || X <- RepKeys, X =/= EKey];
                                            _ -> RepKeys
                                        end};
                                   _ -> {FType, RepKeys}
                               end,
              %write replica group
              lists:foreach(
                fun(RKey) ->
                        DBEntry = db_entry:new(RKey, "2", 2),
                        api_dht_raw:unreliable_lookup(RKey, 
                                                      {set_key_entry, comm:this(), DBEntry}),
                        receive {set_key_entry_reply, _} -> ok end
                end,
                WKeys),
              %insert update error
              if EType =:= update andalso EKey =/= null ->
                     Msg = {set_key_entry, comm:this(), db_entry:new(EKey, "old", 1)},
                     api_dht_raw:unreliable_lookup(EKey, Msg),
                     receive {set_key_entry_reply, _} -> ok end;
                 true -> ok
              end     
      end, 
      Keys),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Analysis Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



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

get_symmetric_ids(NodeCount) ->
    [element(2, intervals:get_bounds(I)) || I <- intervals:split(intervals:all(), NodeCount)].

-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    util:wait_for(fun() ->
                          R = admin:check_ring(),
                          R =:= ok
                  end, 500).
