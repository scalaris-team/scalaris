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

%% @author Jan Skrzypczak
%% @doc U/Q Replicated State Machine implementation of Zheng et al.
%%      For every key, a separate instance of the protocol is managed.
%% @end
-module(zheng_acceptor).
-author('skrzypczak@zib.de').

-define(TRACE(X,Y), ct:pal(X,Y)).
%-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-define(PDB, db_prbr).

%%% functions for module where embedded into
-export([on/2, init/1, close/1, close_and_delete/1]).
-export([check_config/0]).

%% let fetch the number of DB entries
-export([get_load/1]).

%% only for unittests
-export([tab2list_raw_unittest/1]).

%% only during recover
-export([tab2list/1]).

-export_type([state/0]).

-type state() :: {?PDB:db(), [comm:mypid_plain()]}.

-define(n, config:read(replication_factor)).
-define(f, ((?n - 1) div 2)).


-define(get(X, State), get_field(?i(X), State)).
-define(set(X, NewV, State), set_field(?i(X), State, NewV)).
-define(union(X, ListOfElements, State), union_buffer(?i(X), ListOfElements, State)).
-define(add(X, Client, Fun, State), add_to_buffer(?i(X), Client, Fun, State)).

-define(rinc(X, State), inc_field(?r(X), State)).
-define(rget(X, State), get_field(?r(X), State)).
-define(rset(X, NewV, State), set_field(?r(X), State, NewV)).
-define(radd(X, Buffer, State), add_to_list(?r(X), Buffer, State)).

-define(i(X), #pstate.X).
-define(r(X), #replies.X).

-record(replies,
  {
    count = 0 :: non_neg_integer(), 
    accept_count = 0 :: non_neg_integer(),
    decide_val = [] :: [buffer()],
    reject_val = [] :: [buffer()]
  }).

-record(pstate,
  {
    seq = 0 :: non_neg_integer(),
    max_seq = -1 :: integer(),

    buff_val = new_buffer() :: buffer(),
    learned_val = dict:new() :: dict:dict(non_neg_integer(), buffer()),

    accept_val = new_buffer() :: buffer(),
    active = false :: boolean(),

    crdt_type = null :: null | crdt:crdt_module(),
    cmd_set = new_buffer() :: buffer(),
    crdt = null :: null | crdt:crdt(),
    replies = #replies{} :: #replies{} | done
  }).

%% More sensible management of query commands... these do not have to be sent
%% to remotely as they are noops for updates?

%% Replace reply_as from proposer .. no evelope saves space and we have to
%% detect when we get multiple replies from the same request

-type buffer() :: set:sets(buffer_element()).
-type buffer_element() ::
  {comm:mypid(),
    {qfun, crdt:query_fun() | [crdt:query_fun()]}
    | [crdt:update_fun()]}.

%% initialize: return initial state.
-spec init(atom() | tuple()) -> state().
init(DBName) -> {?PDB:new(DBName), []}.

%% @doc Closes the given DB (it may be recoverable using open/1 depending on
%%      the DB back-end).
-spec close(state()) -> true.
close(State) -> ?PDB:close(State).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_and_delete(state()) -> true.
close_and_delete(State) -> ?PDB:close_and_delete(State).

-spec on(tuple(), state()) -> state().
on({zheng_acceptor, new_read, _Cons, Proposer, Key, ReqId, DataType, QueryFuns}, State) ->
    %?TRACE("zheng_acceptor: new query at key: ~p~n~p ", [Key, Proposer]),
    comm:send_local(self(),
      {zheng_acceptor, new_write, _Cons, Proposer, Key, ReqId, DataType, {qfun, QueryFuns}}),
    State;


%% Procedure ReceiveValue
on({zheng_acceptor, new_write, _Cons, Proposer, Key, ReqId, DataType, UpdateFuns}, State) ->
    PState = get_pstate(Key, State),
    NP1 =
      case ?get(crdt, PState) =:= null of
        true ->
          T = ?set(crdt, DataType:new(), PState),
          ?set(crdt_type, DataType, T);
        false ->
          PState
      end,
    NewPState = ?add(buff_val, {ReqId, Proposer}, UpdateFuns, NP1),
    save_pstate(Key, NewPState, State),

    %?TRACE("zheng_acceptor: new update at key ~p~n Buffered Values: ~p", [{ReqId, Key}, ?get(buff_val, NewPState)]),

    %comm:send(Proposer, {result, ok}),
    comm:send_local(self(), {zheng_acceptor, agree, Key}),
    State;


%% The following on-handlers implement the Agree Procedure in the paper
on({zheng_acceptor, agree, Key}, State) ->
  PState = get_pstate(Key, State),
  Buffered = ?get(buff_val, PState),
  case ?get(active, PState) =:= false andalso
    (not sets:is_empty(Buffered)  orelse ?get(max_seq, PState) >= ?get(seq, PState)) of
    true ->
      NP1 = ?set(active, true, PState),
      NP2 = ?union(accept_val, Buffered, NP1),
      NP3 = ?set(buff_val, new_buffer(), NP2),

      NewReplies = new_replies(),
      NP4 = ?set(replies, NewReplies, NP3),
      save_pstate(Key, NP4, State),
      comm:send_local(self(), {zheng_acceptor, agree_loop, Key, _Iteration=1}),
      State;
    false ->
      %% GUARD is false -> nothing to do
      State
  end;


on({zheng_acceptor, agree_loop, Key, Iteration}, State) ->
  PState = get_pstate(Key, State),
  Val = ?get(accept_val, PState),
  SeqNum = ?get(seq, PState),

  This = comm:reply_as(comm:this(), 5, {zheng_acceptor, agree_loop_collect, Key, Iteration, '_'}),
  Message = {zheng_acceptor, prop, Key, This, Val, Iteration, SeqNum, '_'},
  send_to_all_replicas(Key, Message, _ConsLookup=8, _KeyReplacement=3),

  State;

on({zheng_acceptor, agree_loop_collect, Key, Iteration,
    {ReplyType, Val, Iter, SeqNum}}, State) ->
  PState = get_pstate(Key, State),
  Replies = ?get(replies, PState),
  case ?get(seq, PState) =:= SeqNum andalso Iteration =:= Iter andalso Replies =/= done of
    false ->
      %% is outdated reply from a previous iteration / seqnumber --> ignore msg
      State;
    true ->
      Replies = ?get(replies, PState),
      NR1 = ?rinc(count, Replies),
      NR2 = 
        case ReplyType of
          accept ->
            ?rinc(accept_count, NR1);
          reject ->
            ?radd(reject_val, Val, NR1);
          decide ->
            ?radd(decide_val, Val, NR1)
        end,
      NP1 = ?set(replies, NR2, PState),

      DecidedVals = ?rget(decide_val, NR2),
      Accepted = ?rget(accept_count, NR2),
      RejectVals = ?rget(reject_val, NR2),
      NP2 =
        case {DecidedVals =/= [], Accepted > (?n div 2)} of
          {true, _} ->
            LearnedVal = union(DecidedVals),
            accept_loop_end(Key, LearnedVal, NP1);
          {false, true} ->
            LearnedVal = ?get(accept_val, NP1),
            accept_loop_end(Key, LearnedVal, NP1);
          {false, false} ->
            AcceptVal = ?get(accept_val, NP1),
            TP = ?set(accept_val, union([AcceptVal | RejectVals]), NP1),
            %% check if we are doing another loop iteration
            case Iteration =< ?f + 1 of
              true ->
                comm:send_local(self(), {zheng_acceptor, agree_loop, Key, Iteration+1}),
                TP;
              false ->
                accept_loop_end(Key, AcceptVal, TP)
            end
        end,

      save_pstate(Key, NP2, State),
      State
  end;


on({zheng_acceptor, prop, Key, Client, Val, Iteration, SeqNum, _Cons}, State) ->
  PState = get_pstate(Key, State),
  Seq = ?get(seq, PState),

  case SeqNum < Seq of
    true ->
      NP1 = ?union(buff_val, Val, PState),
      Lv = ?get(learned_val, PState),
      comm:send(Client, {decide, dict:fetch(SeqNum, Lv), Iteration, SeqNum}),
      save_pstate(Key, NP1, State),
      comm:send_local(self(), {zheng_acceptor, agree, Key});
    false ->
      NP1 = ?set(max_seq, max(SeqNum, Seq), PState),
      save_pstate(Key, NP1, State),
      comm:send_local(self(), {zheng_acceptor, prop_wait, Key, Client, Val, Iteration, SeqNum})
  end,
  State;

on({zheng_acceptor, prop_wait, Key, Client, Val, Iteration, SeqNum}, State) ->
  PState = get_pstate(Key, State),

  case ?get(seq, PState) < SeqNum of
    true ->
      %% wait more
      comm:send_local(self(), {zheng_acceptor, prop_wait, Key, Client, Val, Iteration, SeqNum});
    false ->
      AcceptVal = ?get(accept_val, PState),
      case is_subset(AcceptVal, Val) andalso ?get(seq, PState) =:= SeqNum of
        %% second part of condition is safeguard as it might be that local sequence number is already higher
        %% and I do not know for certain if returning with accept is still correct...
        %% but this should be pretty rare (impossible if messages are processed in order?)
        %% and thus not affect performance noticable.
        true ->
          comm:send(Client, {accept, new_buffer(), Iteration, SeqNum}),
          NP1 = ?set(accept_val, Val, PState),
          save_pstate(Key, NP1, State);
        false ->
          comm:send(Client, {reject, AcceptVal, Iteration, SeqNum})
      end
  end,
  State.


accept_loop_end(Key, LearnedVal, PState) -> %% TODO: notify clients?
  SeqNum = ?get(seq, PState),
  LearnedDict = ?get(learned_val, PState),
  AcceptedVal = ?get(accept_val, PState),

  NewLD = dict:store(SeqNum, LearnedVal, LearnedDict),

  NewAV =
    case dict:find(SeqNum - 1, NewLD) of
      {ok, Val} -> subtract(AcceptedVal, Val);
      error -> AcceptedVal
    end,

  NP1 = ?set(learned_val, NewLD, PState),
  NP2 = ?set(accept_val, NewAV, NP1),
  NP3 = ?set(seq, SeqNum + 1, NP2),
  NP4 = ?set(active, false, NP3),


  %% apply all learned updates commands to the CRDT
  CrdtAppliedSet = ?get(cmd_set, NP4),
  DataType = ?get(crdt_type, NP4),
  Crdt = ?get(crdt, NP4),
  ReplicaId = get_replica_id(Key),

  NewLearned = subtract(LearnedVal, CrdtAppliedSet),
  LearnedAsList = sets:to_list(NewLearned),
  NCrdt = lists:foldl(
                fun({_Client, Fun}, Acc) ->
                  case Fun of
                    {qfun, _} -> Acc;
                    Fun -> apply_updates(DataType, Fun, ReplicaId, Acc)
                  end
                end, Crdt, LearnedAsList),
  NP5 = ?set(crdt, NCrdt, NP4),
  NP6 = ?set(cmd_set, union(NewLearned, CrdtAppliedSet), NP5),

  %% notify all clients of learned commands and apply queries
  [
    begin
      case Fun of
        {qfun, F} ->
          Result = apply_queries(DataType, F, NCrdt),
          comm:send(Client, {read_result, ReqId, Result});
        _ ->
          comm:send(Client, {write_result, ReqId, ok})
      end
    end
  || {{ReqId, Client}, Fun} <- LearnedAsList],

  NP7 = ?set(replies, done, NP6),

  comm:send_local(self(), {zheng_acceptor, agree, Key}),
  %print_pstate("After learned", Key, NP7),
  NP7.

apply_updates(DataType, Funs, ReplicaId, Crdt) when is_list(Funs) ->
  lists:foldl(
    fun(UpdateFun, Acc) ->
      DataType:apply_update(UpdateFun, ReplicaId, Acc)
    end, Crdt, lists:flatten(Funs));
apply_updates(DataType, Fun, ReplicaId, Crdt) ->
  DataType:apply_update(Fun, ReplicaId, Crdt).

-spec apply_queries(crdt:crdt_module(), [crdt:query_fun()] | crdt:query_fun(), crdt:crdt()) -> any() | [any()].
apply_queries(DataType, QueryFun, Crdt) when is_function(QueryFun) ->
    DataType:apply_query(QueryFun, Crdt);
apply_queries(DataType, QueryFuns, Crdt) ->
   [apply_queries(DataType, Fun, Crdt) || Fun <- QueryFuns].

send_to_all_replicas(Key, Message, ConsLookupField, KeyFieldReplace) ->
  Dest = pid_groups:find_a(routing_table),
  [
    begin
      %% let fill in whether lookup was consistent
      Msg = setelement(KeyFieldReplace, Message, K),
      LookupEnvelope = dht_node_lookup:envelope(ConsLookupField, Msg),
      comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope})
    end
  || K <- replication:get_keys(Key) ].

get_replica_id(_Key) ->
  % DOES NOT MATTER AS CRDT VALUES ARE NEVER MERGED
  1.
  %Keys = lists:sort(replication:get_keys(Key)),
  %Tmp = lists:dropwhile(fun(E) -> E =/= Key end, Keys),
  %length(Keys) - length(Tmp) + 1.

%%%%%%%%%%% State management 
-spec get_pstate(any(), state()) -> #pstate{}.
get_pstate(Key, State) ->
  case ?PDB:get(tablename(State), Key) of
    {} ->
      #pstate{};
    {Key, PState} ->
      PState
  end.

-spec save_pstate(any(), #pstate{}, state()) -> ok.
save_pstate(Key, PState, State) ->
  ?PDB:set(tablename(State), {Key, PState}).

-spec union_buffer(non_neg_integer(), buffer(), tuple()) -> tuple().
union_buffer(BufIdx, BufferToMerge, PState) ->
  NewBuf = union(BufferToMerge, element(BufIdx, PState)),
  setelement(BufIdx, PState, NewBuf).

-spec add_to_list(non_neg_integer(), buffer(), tuple()) -> tuple().
add_to_list(BufIdx, NewBuffer, PState) ->
  NewBuf = [NewBuffer | element(BufIdx, PState)],
  setelement(BufIdx, PState, NewBuf).

-spec add_to_buffer(non_neg_integer(), {any(), comm:mypid()}, crdt:update_fun(), tuple()) -> tuple().
add_to_buffer(BufIdx, Client, Fun, PState) ->
  BufEle = {Client, Fun},
  NewBuf = add(BufEle, element(BufIdx, PState)),
  setelement(BufIdx, PState, NewBuf).

-spec get_field(non_neg_integer(), tuple()) -> any().
get_field(FieldIdx, PState) ->
  element(FieldIdx, PState).

-spec inc_field(non_neg_integer(), tuple()) -> tuple().
inc_field(FieldIdx, PState) ->
  NewVal = element(FieldIdx, PState) + 1,
  setelement(FieldIdx, PState, NewVal).

-spec set_field(non_neg_integer(), tuple(), any()) -> tuple().
set_field(FieldIdx, PState, NewValue) ->
  setelement(FieldIdx, PState, NewValue).

-spec new_replies() -> #replies{}.
new_replies() -> #replies{}.

%% Buffer management
-spec add(buffer_element(), buffer()) -> buffer().
add(Element, Buff) -> sets:add_element(Element, Buff).

-spec subtract(buffer(), buffer()) -> buffer().
subtract(A, B) -> sets:subtract(A, B).

-spec is_subset(buffer(), buffer()) -> boolean().
is_subset(A, B) -> sets:is_subset(A, B).

-spec union([buffer()]) -> buffer().
union(ListOfBuffers) -> sets:union(ListOfBuffers).

-spec union(buffer(), buffer()) -> buffer().
union(A, B) -> sets:union(A, B).

-spec new_buffer() -> buffer().
new_buffer() -> sets:new().

%%%%%%%%%

-spec tablename(state()) -> ?PDB:db().
tablename(State) -> element(1, State).


-spec get_load(state()) -> non_neg_integer().
get_load(State) -> ?PDB:get_load(State).

-spec tab2list(state()) -> [{any(), #pstate{}}].
tab2list(State) ->
    %% without prbr own data
    tab2list_raw(State).

-spec tab2list_raw_unittest(state()) -> [{any(), #pstate{}}].
tab2list_raw_unittest(State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    tab2list_raw(State).

-spec tab2list_raw(state()) -> [{any(), #pstate{}}].
tab2list_raw(State) ->
    %% with prbr own data
    ?PDB:tab2list(State).


%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() -> true.


%% DEBUGGING
print_pstate(Key, PState) ->
  print_pstate("", Key, PState).
print_pstate(Msg, Key, PState) ->
  ?TRACE(
    "Msg: ~p~n"
    "Key:~p~n~n" ++
    "seq:~p~n" ++
    "max_seq:~p~n" ++
    "buff_val:~p~n" ++
    "learned_val:~p~n" ++
    "accept_val:~p~n" ++
    "active:~p~n" ++
    "cmd_set:~p~n" ++
    "crdt_type:~p~n" ++
    "crdt:~p~n" ++
    "replies:~p~n"
    ,
    [Msg, Key,
    ?get(seq, PState),
    ?get(max_seq, PState),
    ?get(buff_val, PState),
    ?get(learned_val, PState),
    ?get(accept_val, PState),
    ?get(active, PState),

    ?get(cmd_set, PState),
    ?get(crdt_type, PState),
    ?get(crdt, PState),
    ?get(replies, PState)
    ]).

