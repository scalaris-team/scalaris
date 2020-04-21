% @copyright 2012-2018 Zuse Institute Berlin,

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Wrapper of Zheng et al. wrapper implementaiton
%% @end
-module(zheng_proposer).
-author('skrzypczak.de').

-define(PDB, pdb).
%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).

-define(CACHED_ROUTING, (config:read(cache_dht_nodes))).

-include("scalaris.hrl").

-behaviour(gen_component).

-export_type([state/0]).

-export([write/5]).
-export([read/5]).

-export([check_config/0]).
-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer() %% reqid
                 }.

-include("gen_component.hrl").


-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, DBSelector,
                             [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    {?PDB:new(?MODULE, [set]), DBSelector, 0, false, false}.


%%%% API

-spec read(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(),
    [crdt:query_fun()] | crdt:query_fun()) -> ok.
read(CSeqPidName, Client, Key, DataType, QueryFuns) ->
    start_request(CSeqPidName, {req_start, {read, Client, Key, DataType, QueryFuns}}).

-spec write(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(),
    [crdt:update_fun()] | crdt:update_fun()) -> ok.
write(CSeqPidName, Client, Key, DataType, UpdateFun) when is_function(UpdateFun) ->
    write(CSeqPidName, Client, Key, DataType, [UpdateFun]);

write(CSeqPidName, Client, Key, DataType, UpdateFuns) when is_list(UpdateFuns) ->
    start_request(CSeqPidName, {req_start, {write, Client, Key, DataType, UpdateFuns}}).

-spec start_request(pid_groups:pidname(), comm:message()) -> ok.
start_request(CSeqPidName, Msg) ->
    Pid = pid_groups:find_a(CSeqPidName),
    trace_mpath:log_info(self(), {start_request, request, Msg}),
    comm:send_local(Pid, Msg).


-spec on(comm:message(), state()) -> state().

%%%%%%%% Query protocol (as query is a keyword, use read/write terminology)
on({req_start, {read, Client, Key, DataType, QueryFuns}}, State) ->
    {ReqId, NewState} = get_next_reqid(State),
    create_open_req_entry(ReqId, Client, tablename(State)),

    This = comm:this(),
    Msg = {zheng_acceptor, new_read, '_', This, key, ReqId, DataType, QueryFuns},
    send_to_local_replica(Key, Msg),
    NewState;

on({read_result, ReqId, Result}, State) ->
    case get_open_req_entry(ReqId, tablename(State)) of
        undefined -> ok; % already answered
        {ReqId, Client} ->
            delete_open_req_entry(ReqId, tablename(State)),
            inform_client(read_done, Client, Result)
    end,
    State;
   

%%%%% UPDATE protocol (as query is a keyword, use read/write terminology)

on({req_start, {write, Client, Key, DataType, UpdateFuns}}, State) ->
    {ReqId, NewState} = get_next_reqid(State),
    create_open_req_entry(ReqId, Client, tablename(State)),

    This = comm:this(),
    Msg = {zheng_acceptor, new_write, '_', This, key, ReqId, DataType, UpdateFuns},
    send_to_local_replica(Key, Msg),
    NewState;

on({write_result, ReqId, ok}, State) ->
    case get_open_req_entry(ReqId, tablename(State)) of
        undefined -> ok; % already answered
        {ReqId, Client} ->
            delete_open_req_entry(ReqId, tablename(State)),
            inform_client(write_done, Client)
    end,
    State;

on({local_range_req, Key, Message, {get_state_response, LocalRange}}, State) ->
    Keys = replication:get_keys(Key),

    LocalKeys = lists:filter(fun(K) -> intervals:in(K, LocalRange) end, Keys),

    K = case LocalKeys of
        [] ->
            ?TRACE("cannot send locally ~p ~p ", [Keys, LocalRange]),
            %% the local dht node is not responsible for any replca... route to
            %% random replica
            Idx = randoms:rand_uniform(1, length(Keys)+1),
            lists:nth(Idx, Keys);
        [H|_] ->
            %% use replica managed by local dht node
            H
        end,

    Dest = pid_groups:find_a(routing_table),
    LookupEnvelope = dht_node_lookup:envelope(3, setelement(5, Message, K)),
    comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope}),
    State.

%%%%%% state management
tablename(State) -> element(1, State).

get_next_reqid(State) ->
    Next = element(3, State) + 1,
    {Next, setelement(3, State, Next)}.

create_open_req_entry(ReqId, Client, TableName) ->
   ?PDB:set({ReqId, Client}, TableName).

get_open_req_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

delete_open_req_entry(ReqId, TableName) ->
    ?PDB:delete(ReqId, TableName).


%%%%%% internal helper


-spec send_to_local_replica(?RT:key(), tuple()) -> ok.
send_to_local_replica(Key, Message) ->
    %% assert element(3, message) =:= '_'
    %% assert element(5, message) =:= key
    send_to_local_replica(Key, Message, ?CACHED_ROUTING).

-spec send_to_local_replica(?RT:key(), tuple(), boolean()) -> ok.
send_to_local_replica(Key, Message, _CachedRouting=true) ->
    dht_node_cache:cached_send_to_local_replica(Key, _KeyPos=5,
                                                Message, _LookupEnvPos=3);
send_to_local_replica(Key, Message, _CachedRouting=false) ->
    LocalDhtNode = pid_groups:get_my(dht_node),
    This = comm:reply_as(comm:this(), 4, {local_range_req, Key, Message, '_'}),
    comm:send_local(LocalDhtNode, {get_state, This, my_range}).


-spec inform_client(write_done, any()) -> ok.
inform_client(write_done, Client) ->
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(Client, {write_done});
        false ->
            comm:send_local(Client, {write_done})
    end.

-spec inform_client(read_done, any(), [any()]) -> ok.
inform_client(read_done, Client, QueryResults) ->
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(Client, {read_done, QueryResults});
        false ->
            comm:send_local(Client, {read_done, QueryResults})
    end.

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_bool(cache_dht_nodes).

