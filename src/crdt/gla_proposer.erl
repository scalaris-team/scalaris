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
%% @doc    Proposer + Learner implementation from "Generalized Lattice Agreement"
%% TODO: instead of sending write done messages, let them poll? only local repla sends msg
%% @end
-module(gla_proposer).
-author('skrzypczak.de').

-define(PDB, pdb).
%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).

-define(ROUTING_DISABLED, false).

-include("scalaris.hrl").

-behaviour(gen_component).

-export([write/5]).
-export([write_eventual/5]).
-export([read/5]).
-export([read_eventual/5]).

-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer(), %% period this process is in
                   [any()], %% list of known proposers
                   [any()]  %% list of open requests
                 }.

-type entry() :: {?RT:key(), %% key
                  crdt:crdt_module(), %% data type
                  [any()], %% clients waiting for reply TODO use this or keep close to paper
                  active | passive,
                  non_neg_integer(), %% ack count
                  non_neg_integer(), %% nack count
                  non_neg_integer(), %% active proposal number
                  crdt:crdt(), %% proposed value
                  crdt:crdt(), %% buffered value
                  crdt:crdt() %% output value
                 }.

-type learner_entry() :: {{learner, ?RT:key()},
                           crdt:crdt_module(),
                           crdt:crdt(),
                           [{any(), non_neg_integer()}]}.

-include("gen_component.hrl").


-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, DBSelector,
                             [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    {?PDB:new(?MODULE, [set]), DBSelector, 0, [], []}.


%%%% API

-spec write(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    start_request(CSeqPidName, {req_start, {write, strong, Client, Key, DataType, UpdateFun}}).

-spec read(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:query_fun()) -> ok.
read(CSeqPidName, Client, Key, DataType, QueryFun) ->
    start_request(CSeqPidName, {req_start, {read, strong, Client, Key, DataType, QueryFun}}).

%% eventual operations are not really supported -> use stronger ops
-spec write_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write_eventual(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    write(CSeqPidName, Client, Key, DataType, UpdateFun).

-spec read_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:query_fun()) -> ok.
read_eventual(CSeqPidName, Client, Key, DataType, QueryFun) ->
    read(CSeqPidName, Client, Key, DataType, QueryFun).


-spec start_request(pid_groups:pidname(), comm:message()) -> ok.
start_request(CSeqPidName, Msg) ->
    Pid = pid_groups:find_a(CSeqPidName),
    trace_mpath:log_info(self(), {start_request, request, Msg}),
    comm:send_local(Pid, Msg).


-spec on(comm:message(), state()) -> state().

%%%% strong consistent read
on({req_start, {read, strong, Client, Key, DataType, QueryFun}}, State) ->
    This = comm:reply_as(comm:this(), 6, {read_write_done, Client, Key, DataType, QueryFun, '_'}),
    gen_component:post_op({req_start, {write, strong, This, Key, DataType, fun crdt:update_noop/2}}, State);

on({read_write_done, Client, Key, DataType, QueryFun, _Msg}, State) ->
    This = comm:reply_as(comm:this(), 5, {apply_query, Client, DataType, QueryFun, '_'}),
    gen_component:post_op({get_learnt_value, Key, This}, State);

on({apply_query, Client, DataType, QueryFun, {ok, LearntValue}}, State) ->
    inform_client(read_done, Client, DataType:apply_query(QueryFun, LearntValue)),
    State;

%%%%% strong consistent write

%% Start request... first retrieve value to propose as a crdt only supplies an update fun
%% If we implemented state machine replication as described in paper (i.e. set-inclusion
%% of proposed update commands), the size of the proposed lattice value will grow
%% linear with the number of commands. As it is not trivial how to prune this set,
%% this is not really a practical solution... Instead we apply the update
%% command first on a replica and use the result as value to propose. However, this
%% makes it harder to known when a particular update command was learned.
on({req_start, {write, strong, Client, Key, DataType, UpdateFun}}, State) ->
    NewState =
        case get_entry(Key, tablename(State)) of
            undefined ->
                NewEntry = new_entry(Key, DataType),
                save_entry(NewEntry, tablename(State)),
                add_proposer(State, comm:this());
            _ -> State
        end,

    ReqId = uid:get_pids_uid(), %% unique identifier for this request
    Request = {ReqId, Client},
    NewState2 = add_open_request(NewState, Request),
    %% Wrapper called after write completes to ensure that each client gets only
    %% a single write done message
    WriteCompleteClient = comm:reply_as(comm:this(), 3, {notify_client, Request, '_'}),
    %% Wrapper to start the write after the lattice value to be proposed was retrieved
    WriteBeginClient = comm:reply_as(comm:this(), 3, {write, WriteCompleteClient, '_'}),

    Msg = {gla_acceptor, get_proposal_value, '_', WriteBeginClient, key, DataType, UpdateFun},
    send_to_local_replica(Key, Msg),
    NewState2;

%% write is complete
on({notify_client, Request={_ReqId, Client}, Msg}, State) ->
   case lists:member(Request, open_requests(State)) of
        false ->
            %% request was already answered -> do nothing
            State;
        true ->
            case is_tuple(Client) of
                true -> comm:send(Client, Msg); %% clients is also enveloped
                false -> comm:send_local(Client, Msg)
            end,
            remove_open_request(State, Request)
    end;

%% ReceiveValue
on({write, Client, {proposal_val_reply, Key, ProposalValue}}, State) ->
    ProposerList = proposers(State),
    [comm:send(Proposer, {internal_receive, Key, Client, ProposalValue})
     || Proposer <- ProposerList],

    State;

%% Internal Receive
on({internal_receive, Key, Client, PropVal}, State) ->
    Entry = get_entry(Key, tablename(State)),
    DataType = entry_datatype(Entry),
    NewBufVal = DataType:merge(entry_bufval(Entry), PropVal),
    NewEntry = entry_set_bufval(Entry, NewBufVal),
    NewEntry2 = entry_add_client(NewEntry, Client),
    _ = save_entry(NewEntry2, tablename(State)),

    gen_component:post_op({propose, Key, _ForeceProposal=true}, State);

%% Propose
on({propose, Key, ForceProposal}, State) ->
    Entry = get_entry(Key, tablename(State)),
    DataType = entry_datatype(Entry),
    PropVal = entry_propval(Entry),
    BufVal = entry_bufval(Entry),
    NewPropVal = DataType:merge(PropVal, BufVal),
    case entry_status(Entry) =:= passive
        andalso (ForceProposal orelse DataType:lt(PropVal, NewPropVal)) of
        true ->
            NewPropNum = entry_propnum(Entry) + 1,
            NewEntry1 = entry_set_ackcount(Entry, 0),
            NewEntry2 = entry_set_nackcount(NewEntry1, 0),
            NewEntry3 = entry_set_propnum(NewEntry2, NewPropNum),
            NewEntry4 = entry_set_propval(NewEntry3, NewPropVal),
            NewEntry5 = entry_set_bufval(NewEntry4, DataType:new()),
            NewEntry6 = entry_set_status(NewEntry5, active),
            _ = save_entry(NewEntry6, tablename(State)),
            Clients = entry_clients(Entry),
            Msg =  {gla_acceptor, propose, '_', comm:this(), key, Clients, NewPropNum, NewPropVal, DataType},
            send_to_all_replicas(Key, Msg),

            State;
        false ->
            State
    end;

%% Proposer Ack
on({ack_reply, Key, ProposalNumber, _ProposalValue}, State) ->
    Entry = get_entry(Key, tablename(State)),
    case entry_propnum(Entry) =:= ProposalNumber of
        true ->
            NewAckCount = entry_ackcount(Entry) + 1,
            NewEntry = entry_set_ackcount(Entry, NewAckCount),
            _ = save_entry(NewEntry, tablename(State)),
            gen_component:post_op({refine, Key}, State);
        false -> State
    end;

%% Proposer Nack
on({nack_reply, Key, ProposalNumber, ProposalValue}, State) ->
    Entry = get_entry(Key, tablename(State)),
    DataType = entry_datatype(Entry),
    case entry_propnum(Entry) =:= ProposalNumber of
        true ->
            NewAckCount = entry_nackcount(Entry) + 1,
            NewProposalValue = DataType:merge(ProposalValue, entry_propval(Entry)),
            NewEntry = entry_set_nackcount(Entry, NewAckCount),
            NewEntry2 = entry_set_propval(NewEntry, NewProposalValue),
            _ = save_entry(NewEntry2, tablename(State)),
            gen_component:post_op({refine, Key}, State);
        false -> State
    end;

%% Proposer Refine
on({refine, Key}, State) ->
    Entry = get_entry(Key, tablename(State)),
    case entry_status(Entry) =:= active andalso
         entry_nackcount(Entry) > 0 andalso
         replication:quorum_accepted(entry_nackcount(Entry) + entry_ackcount(Entry)) of
        true ->
            NewPropNum = entry_propnum(Entry) + 1,
            DataType = entry_datatype(Entry),
            PropVal = entry_propval(Entry),
            NewEntry1 = entry_set_ackcount(Entry, 0),
            NewEntry2 = entry_set_nackcount(NewEntry1, 0),
            NewEntry3 = entry_set_propnum(NewEntry2, NewPropNum),
            _ = save_entry(NewEntry3, tablename(State)),
            Clients = entry_clients(Entry),
            Msg =  {gla_acceptor, propose, '_', comm:this(), key, Clients, NewPropNum, PropVal, DataType},
            send_to_all_replicas(Key, Msg),
            State;
        false ->
            gen_component:post_op({decide, Key}, State)
    end;

%% Proposer Decide
on({decide, Key}, State) ->
    Entry = get_entry(Key, tablename(State)),
    case entry_status(Entry) =:= active andalso
         replication:quorum_accepted(entry_ackcount(Entry)) of
        true ->
            PropVal = entry_propval(Entry),
            NewEntry = entry_set_outval(Entry, PropVal),
            NewEntry2 = entry_set_status(NewEntry, passive),
            _ = save_entry(NewEntry2, tablename(State)),
            gen_component:post_op({propose, Key, _ForceProposal=false}, State);
        false ->
            State
    end;

%% Learner ack
on({learner_ack_reply, Key, Clients, DataType, ProposalNumber, ProposalValue, ProposerId}, State) ->
    Entry = case get_entry({learner, Key}, tablename(State)) of
                undefined -> new_learner_entry(Key, DataType);
                E -> E
            end,

    NewEntry = learner_add_vote(Entry, ProposerId, ProposalNumber),
    VoteCount = learner_votes(NewEntry, ProposerId, ProposalNumber),
    Learnt = learner_learnt(NewEntry),
    NewEntry2 =
        case replication:quorum_accepted(VoteCount) andalso
            DataType:lteq(Learnt, ProposalValue) of
            true ->
                ProposerEntry = get_entry(Key, tablename(State)),
                NewProposerEntry = entry_remove_clients(ProposerEntry, Clients),
                _ = save_entry(NewProposerEntry, tablename(State)),
                [inform_client(write_done, Client) || Client <- Clients],

                learner_set_learnt(NewEntry, ProposalValue);
            false ->
                NewEntry
        end,

    _ = save_entry(NewEntry2, tablename(State)),

    %% lazily complete lists of active proposers
    add_proposer(State, ProposerId);

on({get_learnt_value, Key, Client}, State) ->
    Ret = case get_entry({learner, Key}, tablename(State)) of
            undefined -> {fail, not_found};
            Entry -> {ok, learner_learnt(Entry)}
        end,

    comm:send(Client, Ret),
    State;

%% Internal to get local acceptor process
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

%%%%%%%%%%%%%%%% message sending helpers
-spec send_to_local_replica(?RT:key(), tuple()) -> ok.
send_to_local_replica(Key, Message) ->
    %% assert element(3, message) =:= '_'
    %% assert element(5, message) =:= key
    send_to_local_replica(Key, Message, not ?ROUTING_DISABLED).

-spec send_to_local_replica(?RT:key(), tuple(), boolean()) -> ok.
send_to_local_replica(Key, Message, _Routing=true) ->
    LocalDhtNode = pid_groups:get_my(dht_node),
    This = comm:reply_as(comm:this(), 4, {local_range_req, Key, Message, '_'}),
    comm:send_local(LocalDhtNode, {get_state, This, my_range}).

-spec send_to_all_replicas(?RT:key(), tuple()) -> ok.
send_to_all_replicas(Key, Message) ->
    %% assert element(3, message) =:= '_'
    %% assert element(6, message) =:= key
    send_to_all_replicas(Key, Message, not ?ROUTING_DISABLED).

-spec send_to_all_replicas(?RT:key(), tuple(), boolean()) -> ok.
send_to_all_replicas(Key, Message, _Routing=true) ->
    Dest = pid_groups:find_a(routing_table),

    _ = [begin
            LookupEnvelope = dht_node_lookup:envelope(3, setelement(5, Message, K)),
            comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope})
         end
        || K <- replication:get_keys(Key)],
    ok.

%%%%%%%%%%%%%%%%%% access of proposer entry
-spec entry_datatype(entry())       -> crdt:crdt_module().
entry_datatype(Entry)               -> element(2, Entry).
-spec entry_clients(entry())       -> [any()].
entry_clients(Entry)               -> element(3, Entry).

-spec entry_status(entry())         -> passive | active.
entry_status(Entry)                 -> element(4, Entry).
-spec entry_ackcount(entry())       -> non_neg_integer().
entry_ackcount(Entry)               -> element(5, Entry).
-spec entry_nackcount(entry())      -> non_neg_integer().
entry_nackcount(Entry)              -> element(6, Entry).
-spec entry_propnum(entry())        -> non_neg_integer().
entry_propnum(Entry)                -> element(7, Entry).
-spec entry_propval(entry())        -> crdt:crdt().
entry_propval(Entry)                -> element(8, Entry).
-spec entry_bufval(entry())        -> crdt:crdt().
entry_bufval(Entry)                -> element(9, Entry).

-spec entry_add_client(entry(), comm:mypid()) -> entry().
entry_add_client(Entry, Client) ->
    Clients = element(3, Entry),
    setelement(3, Entry, [Client | Clients]).
-spec entry_remove_clients(entry(), [comm:mypid()]) -> entry().
entry_remove_clients(Entry, ClientsToRemve) ->
    Clients = element(3, Entry),
    NewClients = lists:filter(fun(C) -> not lists:member(C, ClientsToRemve) end, Clients),
    setelement(3, Entry, NewClients).

-spec entry_set_status(entry(), passive | active) -> entry().
entry_set_status(Entry, Status) -> setelement(4, Entry, Status).
-spec entry_set_ackcount(entry(), non_neg_integer()) -> entry().
entry_set_ackcount(Entry, X)    -> setelement(5, Entry, X).
-spec entry_set_nackcount(entry(), non_neg_integer()) -> entry().
entry_set_nackcount(Entry, X)   -> setelement(6, Entry, X).
-spec entry_set_propnum(entry(), non_neg_integer()) -> entry().
entry_set_propnum(Entry, X)     -> setelement(7, Entry, X).
-spec entry_set_propval(entry(), crdt:crdt()) -> entry().
entry_set_propval(Entry, X)     -> setelement(8, Entry, X).
-spec entry_set_bufval(entry(), crdt:crdt()) -> entry().
entry_set_bufval(Entry, X)      -> setelement(9, Entry, X).
-spec entry_set_outval(entry(), crdt:crdt()) -> entry().
entry_set_outval(Entry, X)      -> setelement(10, Entry, X).


%%%%%%%%%%%%%%%% access of learner entry
-spec learner_learnt(entry())   -> crdt:crdt().
learner_learnt(Entry)           -> element(3, Entry).

-spec learner_set_learnt(learner_entry(), crdt:crdt()) -> learner_entry().
learner_set_learnt(Entry, Value) -> setelement(3, Entry, Value).

-spec learner_votes(learner_entry(), any(), non_neg_integer()) -> non_neg_integer().
learner_votes(Entry, Proposer, ProposalNumber) ->
    Votes = element(4, Entry),
    Key = {Proposer, ProposalNumber},
    maps:get(Key, Votes).

-spec learner_add_vote(learner_entry(), any(), non_neg_integer()) -> learner_entry().
learner_add_vote(Entry, Proposer, ProposalNumber) ->
    Votes = element(4, Entry),
    Key = {Proposer, ProposalNumber},
    NewVotes = maps:update_with(Key, fun(E) -> E + 1 end, 1, Votes),
    setelement(4, Entry, NewVotes).


%%%%%%%%%%%%%%% creation/retrieval/save of entries
-spec new_entry(?RT:key(), crdt:crdt_module()) -> entry().
new_entry(Key, DataType) ->
    {lowest_key(Key), DataType, [], passive, 0, 0, 0, DataType:new(), DataType:new(), DataType:new()}.

-spec new_learner_entry(?RT:key(), crdt:crdt_module()) -> learner_entry().
new_learner_entry(Key, DataType) ->
    {{learner, lowest_key(Key)}, DataType, DataType:new(), #{}}.


-spec get_entry(?RT:key() | {learner, ?RT:key()}, ?PDB:tableid()) -> entry() | undefined.
get_entry({learner, Key}, TableName) ->
    ?PDB:get({learner, lowest_key(Key)}, TableName);
get_entry(Key, TableName) ->
    ?PDB:get(lowest_key(Key), TableName).

-spec save_entry(entry(), ?PDB:tableid()) -> ok.
save_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).


%%%%%%%%%%%%%% functions to retrieve/modify state
-spec tablename(state()) -> ?PDB:tableid().
tablename(State) -> element(1, State).

-spec proposers(state()) -> [any()].
proposers(State) -> element(4, State).
-spec add_proposer(state(), any()) -> state().
add_proposer(State, Proposer) ->
    Known =  element(4, State),
    case lists:member(Proposer, Known) of
        true -> State;
        false -> setelement(4, State, [Proposer | Known])
    end.

-spec open_requests(state()) -> [{any(), comm:mypid()}].
open_requests(State) -> element(5, State).
-spec add_open_request(state(), {any(), comm:mypid()}) -> state().
add_open_request(State, Req) ->
    Reqs = element(5, State),
    setelement(5, State, [Req | Reqs]).
-spec remove_open_request(state(), {any(), comm:mypid()}) -> state().
remove_open_request(State, Req) ->
    Reqs = element(5, State),
    setelement(5, State, lists:delete(Req, Reqs)).

%%%%%%%%%%%%%% misc
inform_client(write_done, Client) ->
    case is_tuple(Client) of
        true ->
            comm:send(Client, {write_done});
        false ->
            comm:send_local(Client, {write_done})
    end.

inform_client(read_done, Client, Value) ->
    comm:send_local(Client, {read_done, Value}).

-spec lowest_key(?RT:key()) -> ?RT:key().
lowest_key(Key) ->
    hd(lists:sort(replication:get_keys(Key))).
