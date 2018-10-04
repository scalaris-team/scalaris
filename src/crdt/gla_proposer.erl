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
%% TODO: instead of sending write done messages, let them poll? only local repla sends msg %% @end
-module(gla_proposer).
-author('skrzypczak.de').

-define(PDB, pdb).
%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).

-define(ROUTING_DISABLED, false).
-define(WRITE_DONE_POLLING_PERIOD, 10). %% time in ms how often to check if writes completed

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
                   [{any(), ?RT:key(), any()}] %% list of writer clients waiting for reply
                 }.

-type entry() :: {?RT:key(), %% key
                  crdt:crdt_module(), %% data type
                  [any()], %% clients waiting for reply TODO use this or keep close to paper
                  active | passive,
                  non_neg_integer(), %% ack count
                  non_neg_integer(), %% nack count
                  non_neg_integer(), %% active proposal number
                  gset:crdt(), %% proposed value
                  gset:crdt(), %% buffered value
                  gset:crdt(), %% output value
                  gset:crdt() %% refine value
                 }.

-type learner_entry() :: {{learner, ?RT:key()},
                           crdt:crdt_module(),
                           gset:crdt(),
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
    gen_component:post_op({learnt_value, Key, This}, State);

on({apply_query, Client, DataType, QueryFun, {LearntValue}}, State) ->
    CrdtVal = gset:fold(fun({_CmdId, UpdateCmd}, Acc) ->
                                %% replica ID does not matter (i.e. when using gcounter)
                                %% as the CRDT value is not distributed, only its update commands.
                                DataType:apply_update(UpdateCmd, 1, Acc)
                        end,
                        DataType:new(), LearntValue),
    inform_client(read_done, Client, DataType:apply_query(QueryFun, CrdtVal)),
    State;

%%%%% strong consistent write

%% procedure ExecuteUpdate
on({req_start, {write, strong, Client, Key, DataType, UpdateFun}}, State) ->
    NewState =
        case get_entry(Key, tablename(State)) of
            undefined ->
                NewEntry = new_entry(Key, DataType),
                save_entry(NewEntry, tablename(State)),
                add_proposer(State, comm:this());
            _ -> State
        end,

    CmdId = {comm:this(), uid:get_pids_uid()}, %% unique identifier of this command
    PropVal = gset:update_add({CmdId, UpdateFun}, gset:new()),

    case waiting_clients(NewState) of
        [] ->
            %% we do not poll currently as the wait set was empty -> start polling
            comm:send_local_after(?WRITE_DONE_POLLING_PERIOD, self(), {learnt_cmd_poller}),
            ok;
        _ ->
            %% we are already polling
            ok
    end,
    NewState2 = add_waiting_client(NewState, {CmdId, Key, Client}),
    gen_component:post_op({receive_value, Key, DataType, PropVal},  NewState2);

%% check which commands are learnt to notify the client
on({learnt_cmd_poller}, State) ->
    %% notfiy all clients whose write was completed and remove the respective requests
    %% from the wait set
    NewList =
       lists:filter(fun({CmdId, Key, Client}) ->
                        case get_entry({learner, Key}, tablename(State)) of
                            undefined -> true;
                            Entry ->
                                CmdSet = learner_learnt(Entry),
                                case gset:exists(fun(E) -> element(1, E) =:= CmdId end, CmdSet) of
                                    true ->
                                        inform_client(write_done, Client),
                                        false;
                                    false ->
                                        true
                                end
                        end
                    end, waiting_clients(State)),

    case NewList of
        [] ->
            %% no waiting clients... we can stop polling for now
            ok;
        _ ->
            %% there are still waiting clients... continue to poll
            comm:send_local_after(?WRITE_DONE_POLLING_PERIOD, self(), {learnt_cmd_poller}),
            ok
    end,

    NewState = set_waiting_clients(State, NewList),
    NewState;


%% action ReceiveValue
on({receive_value, Key, DataType, Command}, State) ->
    ProposerList = proposers(State),
    [comm:send(Proposer, {internal_receive, Key, DataType, Command})
     || Proposer <- ProposerList],

    State;

%% Internal Receive
on({internal_receive, Key, DataType, PropVal}, State) ->
    Entry = get_entry(Key, DataType, tablename(State)),
    NewBufVal = gset:merge(entry_bufval(Entry), PropVal),
    NewEntry = entry_set_bufval(Entry, NewBufVal),
    _ = save_entry(NewEntry, tablename(State)),

    gen_component:post_op({propose, Key}, State);

%% Propose
on({propose, Key}, State) ->
    Entry = get_entry(Key, tablename(State)),
    PropVal = entry_propval(Entry),
    BufVal = entry_bufval(Entry),
    NewPropVal = gset:merge(PropVal, BufVal),
    case entry_status(Entry) =:= passive
        andalso gset:lt(PropVal, NewPropVal) of
        true ->
            NewPropNum = entry_propnum(Entry) + 1,
            NewEntry1 = entry_set_propval(Entry, NewPropVal),
            NewEntry2 = entry_set_status(NewEntry1, active),
            NewEntry3 = entry_set_ackcount(NewEntry2, 0),
            NewEntry4 = entry_set_nackcount(NewEntry3, 0),
            NewEntry5 = entry_set_propnum(NewEntry4, NewPropNum),
            _ = save_entry(NewEntry5, tablename(State)),

            DataType = entry_datatype(NewEntry5),
            Msg =  {gla_acceptor, propose, '_', comm:this(), key, DataType, NewPropNum, BufVal},
            send_to_all_replicas(Key, Msg),

            NewEntry6 = entry_set_bufval(NewEntry5, gset:new()),
            _ = save_entry(NewEntry6, tablename(State)),

            State;
        false ->
            State
    end;

%% Proposer Ack
on({ack_reply, Key, ProposalNumber}, State) ->
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
on({nack_reply, Key, ProposalNumber, MissingValues}, State) ->
    Entry = get_entry(Key, tablename(State)),
    case entry_propnum(Entry) =:= ProposalNumber of
        true ->
            NewAckCount = entry_nackcount(Entry) + 1,
            RefVal = gset:merge(MissingValues, entry_refval(Entry)),
            NewEntry = entry_set_nackcount(Entry, NewAckCount),
            NewEntry2 = entry_set_refval(NewEntry, RefVal),
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
            RefVal = entry_refval(Entry),
            PropVal = gset:merge(entry_propval(Entry), RefVal),
            NewEntry1 = entry_set_ackcount(Entry, 0),
            NewEntry2 = entry_set_nackcount(NewEntry1, 0),
            NewEntry3 = entry_set_propnum(NewEntry2, NewPropNum),
            NewEntry4 = entry_set_propval(NewEntry3, PropVal),
            NewEntry5 = entry_set_refval(NewEntry4, gset:new()),
            _ = save_entry(NewEntry5, tablename(State)),

            DataType = entry_datatype(Entry),
            Msg =  {gla_acceptor, propose, '_', comm:this(), key, DataType, NewPropNum, RefVal},
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
            gen_component:post_op({propose, Key}, State);
        false ->
            State
    end;

%% Learner ack
on({learner_ack_reply, Key, DataType, ProposalNumber, Value, ProposerId}, State) ->
    Entry = case get_entry({learner, Key}, tablename(State)) of
                undefined -> new_learner_entry(Key, DataType);
                E -> E
            end,

    NewEntry = learner_add_vote(Entry, ProposerId, ProposalNumber),
    VoteCount = learner_votes(NewEntry, ProposerId, ProposalNumber),
    Learnt = learner_learnt(NewEntry),
    NewEntry2 =
        case replication:quorum_accepted(VoteCount) andalso
            gset:lt(Learnt, Value) of
            true ->
                learner_set_learnt(NewEntry, Value);
            false ->
                NewEntry
        end,
    _ = save_entry(NewEntry2, tablename(State)),

    %% lazily complete lists of active proposers
    add_proposer(State, ProposerId);

%% procedure LearntValue
on({learnt_value, Key, Client}, State) ->
    Ret =
        case get_entry({learner, Key}, tablename(State)) of
            undefined -> gset:new();
            Entry -> learner_learnt(Entry)
        end,
    comm:send(Client, {Ret}),
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

-spec entry_status(entry())         -> passive | active.
entry_status(Entry)                 -> element(4, Entry).
-spec entry_ackcount(entry())       -> non_neg_integer().
entry_ackcount(Entry)               -> element(5, Entry).
-spec entry_nackcount(entry())      -> non_neg_integer().
entry_nackcount(Entry)              -> element(6, Entry).
-spec entry_propnum(entry())        -> non_neg_integer().
entry_propnum(Entry)                -> element(7, Entry).
-spec entry_propval(entry())        -> gset:crdt().
entry_propval(Entry)                -> element(8, Entry).
-spec entry_bufval(entry())        -> gset:crdt().
entry_bufval(Entry)                -> element(9, Entry).
-spec entry_refval(entry())        -> gset:crdt().
entry_refval(Entry)                -> element(11, Entry).

-spec entry_set_status(entry(), passive | active) -> entry().
entry_set_status(Entry, Status) -> setelement(4, Entry, Status).
-spec entry_set_ackcount(entry(), non_neg_integer()) -> entry().
entry_set_ackcount(Entry, X)    -> setelement(5, Entry, X).
-spec entry_set_nackcount(entry(), non_neg_integer()) -> entry().
entry_set_nackcount(Entry, X)   -> setelement(6, Entry, X).
-spec entry_set_propnum(entry(), non_neg_integer()) -> entry().
entry_set_propnum(Entry, X)     -> setelement(7, Entry, X).
-spec entry_set_propval(entry(), gset:crdt()) -> entry().
entry_set_propval(Entry, X)     -> setelement(8, Entry, X).
-spec entry_set_bufval(entry(), gset:crdt()) -> entry().
entry_set_bufval(Entry, X)      -> setelement(9, Entry, X).
-spec entry_set_outval(entry(), gset:crdt()) -> entry().
entry_set_outval(Entry, X)      -> setelement(10, Entry, X).
-spec entry_set_refval(entry(), gset:crdt()) -> entry().
entry_set_refval(Entry, X)      -> setelement(11, Entry, X).

%%%%%%%%%%%%%%%% access of learner entry
-spec learner_learnt(learner_entry()) -> gset:crdt().
learner_learnt(Entry) -> element(3, Entry).

-spec learner_set_learnt(learner_entry(), gset:crdt()) -> learner_entry().
learner_set_learnt(Entry, Value) -> setelement(3, Entry, Value).

-spec learner_votes(learner_entry(), any(), non_neg_integer()) -> non_neg_integer().
learner_votes(Entry, Proposer, ProposalNumber) ->
    Votes = element(4, Entry),
    Key = {Proposer, ProposalNumber},
    case lists:keyfind(Key, 1, Votes) of
        false -> 0;
        E -> element(2, E)
    end.

-spec learner_add_vote(learner_entry(), any(), non_neg_integer()) -> learner_entry().
learner_add_vote(Entry, Proposer, ProposalNumber) ->
    Votes = element(4, Entry),
    Dummy = {{Proposer, -1}, 0},

    {HighestPropNum, OldCount} = lists:max([{PropNum, OldCount} ||
                                            {{P, PropNum}, OldCount} <- [Dummy | Votes], P =:= Proposer]),
    case HighestPropNum > ProposalNumber of
        true -> Entry;
        false ->
            NewVoteCount = case ProposalNumber =:= HighestPropNum of
                               true -> OldCount + 1;
                               false -> 1
                           end,
            NewList = [E || E={{P,_},_} <- Votes, P =/= Proposer],
            NewVotes = [{{Proposer, ProposalNumber}, NewVoteCount}| NewList],
            setelement(4, Entry, NewVotes)
    end.

%%%%%%%%%%%%%%% creation/retrieval/save of entries
-spec new_entry(?RT:key(), crdt:crdt_module()) -> entry().
new_entry(Key, DataType) ->
    {lowest_key(Key), DataType, [], passive, 0, 0, 0, gset:new(), gset:new(), gset:new(), gset:new()}.

-spec new_learner_entry(?RT:key(), crdt:crdt_module()) -> learner_entry().
new_learner_entry(Key, DataType) ->
    {{learner, lowest_key(Key)}, DataType, gset:new(), []}.


-spec get_entry(?RT:key() | {learner, ?RT:key()}, ?PDB:tableid()) -> entry() | learner_entry() | undefined.
get_entry({learner, Key}, TableName) ->
    ?PDB:get({learner, lowest_key(Key)}, TableName);
get_entry(Key, TableName) ->
    ?PDB:get(lowest_key(Key), TableName).

-spec get_entry(?RT:key(), crdt:crdt_module(), ?PDB:tableid()) -> entry().
get_entry(Key, DataType, TableName) ->
    case ?PDB:get(lowest_key(Key), TableName) of
        undefined -> new_entry(Key, DataType);
        Entry -> Entry
    end.

-spec save_entry(entry() | learner_entry(), ?PDB:tableid()) -> ok.
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

-spec waiting_clients(state()) -> [{any(), ?RT:key(), any()}].
waiting_clients(State) -> element(5, State).
-spec set_waiting_clients(state(), [{any(), ?RT:key(), any()}]) -> state().
set_waiting_clients(State, Clients) -> setelement(5, State, Clients).
-spec add_waiting_client(state(), {any(), ?RT:key(), any()}) -> state().
add_waiting_client(State, Client) ->
    Known =  element(5, State),
    case lists:member(Client, Known) of
        true -> State;
        false -> setelement(5, State, [Client | Known])
    end.

%%%%%%%%%%%%%% misc
-spec inform_client(write_done, comm:mypid()) -> ok.
inform_client(write_done, Client) ->
    case is_tuple(Client) of
        false -> comm:send_local(Client, {write_done});
        true -> comm:send(Client, {write_done})
    end.

-spec inform_client(read_done, comm:mypid(), any()) -> ok.
inform_client(read_done, Client, Value) ->
    comm:send_local(Client, {read_done, Value}).

-spec lowest_key(?RT:key()) -> ?RT:key().
lowest_key(Key) ->
    hd(lists:sort(replication:get_keys(Key))).
