% @copyright 2012-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Transaction validation based on a sequence of Paxos
%%      round based registers (kv_on_cseq).
%% @end
%% @version $Id:$
-module(tx_tm).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).
-include("scalaris.hrl").
-include("client_types.hrl").

%% public interface for transaction validation.
-export([commit/4]).           %% commit a transaction log
-export([msg_commit_reply/3]). %% expect this message as reply to a commit
-export([redecide/1]).         %% take over an open tx based on a txid
-export([redecide_on_key/1]).  %% take over an open tx based on an involved key

%% detect whether txnew is enabled or not
-export([is_txnew_enabled/0]).

%% TODO: commit should return the txid (maybe on the layer above) and
%% include that in its replies, so duplicate replies can be detected
%% and eliminated / ignored by the client or a proxy process. A client
%% could retrigger the tx if it takes to long. But, if the inform
%% client message is lost, the system would not be able to reanswer
%% the request, but would reply with {fail, tx_not_open}.

%% functions for dht embedding
%%-export([get_my/2]).
-export([rm_send_update/5]). %% subscribe to rm changes for local tx_ids

%% functions for gen_component module, supervisor callbacks and config
-export([start_link/2]).
-export([on/2, init/1]).
-export([on_init/2]).
-export([check_config/0]).

-export_type([tx_id/0]).

-include("gen_component.hrl").


%% TM on kv_on_cseq

%% commit
%% 1. assign txid
%% 2. get locks *and* persist txid
%%    store TLog (for big values in a tlog use two steps here
%%      to avoid value transfer via lookups: first lookup the
%%      responsible node, then send value directly)
%%    a) write txid (content check ensures, it does not exist already)
%%    b) read lock: write txid and client into readlock,
%%                  content check ensures no write lock is set
%%                  and item is untouched (has same version as in TLog).
%%       write lock: write txid, planned value, and client into write lock
%%                  content check ensures no write lock is set,
%%                  and no read lock is set,
%%                  and item is untouched (has same version as in TLog).
%% 3. wait for txid persisted and all keys replied
%% 4. if we got all locks, decide on commit else abort.
%%    write decision to txid record in the dht to be able to decide later
%%    in the same way. (otherwise arguments below would become true [*])
%%    wait for that to finish.
%% 5. release all gotten locks and actually perform the planned value
%%    change when decided commit or rollback in case of abort.
%%    read lock: simply remove the readlock
%%    write lock: write the value and increase the version number
%%                in the database field by one.
%% 6. if all items released the locks (majority of replicas) and txid is written
%%    a) inform client
%%    b) remove txid
%% Done

%% it could happen that a client is informed twice on the outcome of a
%% tx?! Example: read-read-commit: tm performs steps 1-5 and informs
%% in step 6 the client and then crashes. Another tm takes over and
%% cannot decide whether the client was informed or not.

%% This scenario may also happen in the old transaction protocol?!


%% What happens if a single txid entry survives?
%% 1. the tx start is gone wrong
%% 2. everything was done and the delete failed.
%% We can distiguish both cases on the content of the entry.
%% 1. the txid status is open
%% 2. the txid statis is commit/abort
%% We do not know whether the client was informed or not, so we should
%% inform it once more?!.




%% [*]

%% 1. Inconsistent decisions when not persisting decision to txid entry:

%% Example: read-read-commit: tm performs steps 1-5 and informs in
%% step 6 the client and then crashes. Another tm takes over and
%% cannot detect whether 2-5 were done or not (to decide in the same
%% way) (he cannot decide whether the tx was actually commited or
%% aborted (maybe we now would have to abort, but the first tm was
%% able to commit - the system state changed in the meantime))?! There
%% remained no system state change to detect that.

%% This scenario may also happen in the old transaction protocol?!


%% 2. Inconsistent decision when writing decision and freeing locks is
%%    done in a single step concurrently:
%%
%% Scenario 1. may happen when all locks are removed and the txid
%% remains without the decision persisted. (Why can that happen: Too
%% many messages lost...? / The TM crashed in that moment).

%% ** Why store the complete TLog in the txid field?

%% We could distribute it to the respective items and store the
%% necessary information there in the read and write locks.


%% recover:
%% a lock is found + no txid
%% -> try to write txid with abort and release lock when successful

%% a lock is found + a txid is found with status open (lock acquisition phase)
%% -> contact other items and propose abort as we do not know the content
%%    if all are prepared - decide commit (they all were already locked)
%%    else decide abort

%% a lock is found + a txid is found with status commit/abort
%% -> quorum free the locks + inform client + delete txid

%% a txid is found in state open
%% -> contact tx participants and try to decide abort

%% a txid is found in state decided/abort
%% -> look items up and check whether the locks are gone (resend
%%    decision with txid, content check will do the rest)

%% getting rid of a single remaining txid:
%%   if it is decided and the other replicas are empty, delete it.
%%   if it is open and the other replicas are empty


%% concurrency control
%% recover a crash that happened during the locking phase

%% recover a crash that happened during decision phase

%% recover a crash that happened during actual commit phase





-type tx_id() :: ?RT:key(). %% was {?tx_id, uid:global_uid()}.

-type state() ::
    {TableName      :: pdb:tableid(),
     Id             :: ?RT:key(),
     OpenTxNum      :: non_neg_integer()}.

%% getter and setters for tx_state implemented at the end of this file.
-define(enum_id,              1).
-define(enum_marker,          2).
-define(enum_client,          3).
-define(enum_clientid,        4).
-define(enum_status,          5).
-define(enum_tlog,            6).
-define(enum_txid_round,      7).
-define(enum_txid_writtenval, 8).
-define(enum_open_locks,      9).
-define(enum_open_commits,   10).
-define(enum_failed_locks,   11).

-type tx_state() ::
        {tx_id(),                  %% Tid
         ?tx_state,                %% type-marker (unnecessary in principle)
         comm:mypid(),             %% Client
         any(),                    %% ClientsId,
         open | ?commit | ?abort,  %% status
         tx_tlog:tlog(),           %% tlog to be committed
         pr:pr() | '_',   %% next txid write token for fast write
         any(),                    %% last written txid value for fast write
         %% keys of locks to be confirmed.
         [?RT:key() | client_key() | binary()],
         %% keys, rounds, and oldvalues of commits to be confirmed,
         [{?RT:key() | client_key() | binary(),
           pr:pr() | '_', %% for fast write, set on lock reply
           any() %% old value for fast write
          }],
         %% keys that failed to be locked,
         [?RT:key() | client_key() | binary()]
        }.


%% non_neg_integer(),        %% NumIds,
         %% non_neg_integer(),        %% NumPrepared,
         %% non_neg_integer(),        %% NumAbort,
         %% non_neg_integer(),        %% NumTPsInformed
         %% non_neg_integer(),        %% Number of items committed
         %% non_neg_integer(),        %% NumTpsRegistered
         %% new | uninitialized | ok  %% status: new / uninitialized / ok
         %% }.

%% public interface for transaction validation.
%% ClientsID may be nil, its not used by tx_tm. It will be repeated in
%% replies to allow to map replies to the right requests in the
%% client.
%% TODO: we could eliminate ClientsID from this implementation, as we
%%       have comm:reply_as - maybe that would be a bit slower.
-spec commit(comm:erl_local_pid(), comm:mypid(), any(), tx_tlog:tlog()) -> ok.
commit(TM, Client, ClientsID, TLog) ->
    Msg = {tx_tm_commit, Client, ClientsID, TLog},
    comm:send_local(TM, Msg).

%% messages a client has to expect when using this module
-spec msg_commit_reply(comm:mypid(), any(),
                       commit | {abort, [client_key() | binary]}) -> ok.
msg_commit_reply(Client, ClientsID, Result) ->
    comm:send(Client, {tx_tm_commit_reply, ClientsID, Result}).

%% @doc takes care of a transaction with the given transaction id.
%% TODO: to be implemented
-spec redecide(tx_id()) -> ok.
redecide(_TxId) ->
    %% quorum read txid from dht to learn about involved keys.
    %% if not existing: nothing is known -> {fail, tx_not_open}.
    %% else
    %% quorum read all involved keys.

    %% If all items locked with tx_id make progress: decide to prepared,
    %% release the locks, inform client and release txid entry in dht.

    %% If only some items are locked, the original tm may still try to
    %% lock the others. If we decide to abort, we have to ensure that
    %% concurrent tms for the same tx will also decide to abort. This
    %% can be done by increasing the version number
    ok.

-spec redecide_on_key(?RT:key()) -> ok.
redecide_on_key(_Key) -> ok.

-spec is_txnew_enabled() -> boolean().
-ifdef(TXNEW).
is_txnew_enabled() -> true.
-else.
is_txnew_enabled() -> false.
-endif.

%% @doc Notifies the tx_tm of a changed node ID.
-spec rm_send_update(Subscriber::pid(), Tag::?MODULE,
                     OldNeighbors::nodelist:neighborhood(),
                     NewNeighbors::nodelist:neighborhood(),
                     Reason::rm_loop:reason()) -> ok.
rm_send_update(Pid, ?MODULE, OldNeighbors, NewNeighbors, _Reason) ->
    OldId = node:id(nodelist:node(OldNeighbors)),
    NewId = node:id(nodelist:node(NewNeighbors)),
    if OldId =/= NewId -> comm:send_local(Pid, {new_node_id, NewId});
       true            -> ok
    end.

%% be startable via supervisor, use gen_component
%% TODO: for now: be compatible with old tx startup,
%%       later we can eliminate the role (the atom tx_tm_new)?
-spec start_link(pid_groups:groupname(), any()) -> {ok, pid()}.
start_link(DHTNodeGroup, tx_tm_new) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [], %% empty list as parameter to init
                             [{pid_groups_join_as, DHTNodeGroup, tx_tm_new},
                              {spawn_opts, [{fullsweep_after, 0},
                                            {min_heap_size, 16383}]}]).

%% initialize: return initial state.
-spec init([]) -> state() | {'$gen_component', [{on_handler, gen_component:handler()},...],
                             state()}.
init([]) ->
    _Role = pid_groups:my_pidname(),
    ?TRACE("tx_tm:init for instance: ~p ~p~n",
           [pid_groups:my_groupname(), _Role]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_tx_tm_" ++ atom_to_list(_Role)),
    %%Table = pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    Table = pdb:new(?MODULE, [set]),

    comm:send_local(self(), {get_node_details}),
    %% subscribe to id changes
    rm_loop:subscribe(self(), ?MODULE,
                      fun rm_loop:subscribe_dneighbor_change_filter/3,
                      fun ?MODULE:rm_send_update/5, inf),
    State = {Table, _ID = unknown, _OpenTxNum = 0},
    gen_component:change_handler(State, fun ?MODULE:on_init/2).

-spec on(comm:message(), state()) -> state().

%% step 1: Write the tx to the txid store
on({tx_tm_commit, Client, ClientsID, TLog}, State) ->
    ?TRACE("tx_tm:on({commit, ...}) for TLog ~p as ~p~n",
           [TLog, state_get_role(State)]),

    TLogUid = uid:get_global_uid(),
    NewTid = {?tx_id, TLogUid},

    NewTidKey = ?RT:hash_key(term_to_binary(NewTid)),
    TxState = tx_state_new(NewTidKey, Client, ClientsID, TLog),
    _ = set_entry(TxState, State),

    ReplyAsTxIdStored = comm:reply_as(self(), 3,
                                      {tx_tm_txid_stored, NewTidKey, '_'}),

    %% initiate storage of txid
    Keys = [ tx_tlog:get_entry_key(X) || X <- TLog ],
    txid_on_cseq:new(NewTidKey, Keys, ReplyAsTxIdStored),

    state_inc_opentxnum(State);

%% tx is stored in txid store
on({tx_tm_txid_stored, TxId,
    {qwrite_done, _ReqId, NextWriteRound, WrittenVal, _WriteRet}}, State) ->
    %% store NextWriteRound and WrittenValue for fast write of decision
    TxState = get_entry(TxId, State),
    T1TxState = tx_state_set_txid_next_write_token(TxState, NextWriteRound),
    NewTxState = tx_state_set_txid_written_value(T1TxState, WrittenVal),
    set_entry(NewTxState, State),
    %% progress to the next step in the protocol
    gen_component:post_op({tx_tm_acquire_locks, TxId}, State);

%% step 2: acquire locks for tlog entries (this cannot be done
%% overlapping with step 1 because when locks were acquired without
%% the tx_id stored properly, we cannot safely cleanup remaining
%% locks, as we do not know whether the TM is just slow (and will
%% later write the tx_id and decide the tx and relies on the set
%% locks) or crashed and will never write and decide the tx_id).
on({tx_tm_acquire_locks, TxId}, State) ->
    %% initiate lock operations on all keys
    TxState = get_entry(TxId, State),
    TLog = tx_state_tlog(TxState),
    _ = [
     begin
         ReplyAs =
             comm:reply_as(self(), 4,
                           {tx_tm_lock_get_done, TxId,
                            tx_tlog:get_entry_key(X), '_'}),
         kv_on_cseq:set_lock(X, TxId, ReplyAs)
     end || X <- TLog ],
    State;

%% lock for a tlog entry was acquired (or not)
on({tx_tm_lock_get_done, TxId, Key,
    {qwrite_done, _ReqId, NextRound, WrittenVal, _WriteRet}}, State) ->
    case get_entry(TxId, State) of
        undefined ->
            State;
        TxState ->
            NewOpenLocks = lists:delete(Key, tx_state_open_locks(TxState)),
            T1TxState = tx_state_set_open_locks(TxState, NewOpenLocks),
            %% remember nextround and writtenval for tx execution
            NewTxState = tx_state_add_nextround_writtenval_for_commit(
                           T1TxState, Key, NextRound, WrittenVal),
            set_entry(NewTxState, State),
            case NewOpenLocks of
                [] ->
                    %% all locks acquired? -> proceed to next step
                    Round = tx_state_txid_next_write_token(NewTxState),
                    OldVal = tx_state_txid_written_value(NewTxState),
                    case tx_state_failed_locks(NewTxState) of
                        [] ->
                            %% io:format("decide commit~n"),
                            gen_component:post_op(
                              {tx_tm_write_decision, TxId, commit, Round, OldVal}, State);
                        _ ->
                            gen_component:post_op(
                              {tx_tm_write_decision, TxId, abort, Round, OldVal}, State)
                    end;
                _ -> State
            end
    end;

on({tx_tm_lock_get_done, TxId, Key,
    {qwrite_deny, _ReqId, NextRound, WrittenVal, _Reason}}, State) ->
    %% a lock was not acquirable -> abort the tx
    case get_entry(TxId, State) of
        undefined ->
            State;
        TxState ->
            %% for performance reasons we only record the first key
            %% that made the tx aborting
            NewOpenLocks = lists:delete(Key, tx_state_open_locks(TxState)),
            NewFailedLocks = [Key | tx_state_failed_locks(TxState)],
            T1TxState = tx_state_set_open_locks(TxState, NewOpenLocks),
            T2TxState = tx_state_set_failed_locks(T1TxState, NewFailedLocks),
            NewTxState = tx_state_add_nextround_writtenval_for_commit(
                           T2TxState, Key, NextRound, WrittenVal),
            set_entry(NewTxState, State),
            case NewOpenLocks of
                [] ->
                    %% all locks acquired? -> proceed to next step
                    Round = tx_state_txid_next_write_token(NewTxState),
                    OldVal = tx_state_txid_written_value(NewTxState),
                    %%log:log("decide abort~n"),
                    gen_component:post_op(
                      {tx_tm_write_decision, TxId, abort, Round, OldVal}, State);
                _ -> State
            end
    end;


%% step 2: Write the decision to the txid store
on({tx_tm_write_decision, TxId, Decision, WriteRound, OldVal}, State) ->
    ReplyAs = comm:reply_as(self(), 3,
                            {tx_tm_decision_stored, TxId, '_'}),

%%    log:log("Decision ~p~n", [Decision]),
    txid_on_cseq:decide(TxId, Decision, ReplyAs, WriteRound, OldVal),
    State;

on({tx_tm_decision_stored, TxId,
    {qwrite_done, _ReqId, _Round, Decision, _WriteRet}}, State) ->
    gen_component:post_op({tx_tm_execute_decision, TxId, Decision}, State);

on({tx_tm_execute_decision, TxId, Decision}, State) ->
    TxState = get_entry(TxId, State),
    TLog = tx_state_tlog(TxState),
    _ = [ begin
              ReplyAs = comm:reply_as(self(), 5,
                                      {tx_tm_executed_decision, TxId,
                                       Key, Decision, '_'}),
              %% retrieve corresponding TLog Entry
              TLogEntry = tx_tlog:find_entry_by_key(TLog, Key),
              Op = tx_tlog:get_entry_operation(TLogEntry),
              case {Op, Decision} of
                  {?read, commit} ->
                      kv_on_cseq:commit_read(
                        TLogEntry, TxId, ReplyAs, NextRound, OldVal);
                  {?write, commit} ->
                      kv_on_cseq:commit_write(
                        TLogEntry, TxId, ReplyAs, NextRound, OldVal);
                  {?read, abort} ->
                      kv_on_cseq:abort_read(
                        TLogEntry, TxId, ReplyAs, NextRound, OldVal);
                  {?write, abort} ->
                      kv_on_cseq:abort_write(
                        TLogEntry, TxId, ReplyAs, NextRound, OldVal)
              end
          end
          || {Key, NextRound, OldVal} <- tx_state_open_commits(TxState) ],
    State;

on({tx_tm_executed_decision, TxId, Key, Decision,
    _QwriteAnswer},
    %% either: {qwrite_done, _ReqId, _Round, _Val, _WriteRet}}
    %% or:     {qwrite_deny, _ReqId, _Round, _Val, Reason}}
    %% deny is also ok, probably the decision was propagated otherwise
    %% (write_through during concurrency, so we accept a write deny).
    %% We had the lock, and only we can release it.
    State) ->
    case get_entry(TxId, State) of
        %% this should not happen!
        %% undefined ->
        %%     State;
        TxState ->
            NewOpenCommits = lists:keydelete(Key, 1, tx_state_open_commits(TxState)),
            case NewOpenCommits of
                [] ->
                    %% all keys committed -> inform client
                    Client = tx_state_client(TxState),
                    ClientsId = tx_state_clients_id(TxState),
                    case Decision of
                        commit ->
                            msg_commit_reply(Client, ClientsId, commit);
                        abort ->
                            msg_commit_reply(Client, ClientsId, {abort, tx_state_failed_locks(TxState)})
                    end,
                    %% proceed to next step
                    gen_component:post_op({tx_tm_delete_txid, TxId}, State);
                _ ->
                    NewTxState = tx_state_set_open_commits(TxState, NewOpenCommits),
                    set_entry(NewTxState, State),
                    State
            end
    end;

on({tx_tm_delete_txid, TxId}, State) ->
    %% TODO: delete in dht
    %% delete locally:
    pdb:delete(TxId, state_get_tablename(State)),
    State;

on({new_node_id, Id}, State) ->
    %% change txid range to remain local with the txids
    state_set_id(State, Id).

-spec on_init(comm:message(), state())
    -> state() |
       {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::state()}.
on_init({get_node_details}, State) ->
    util:wait_for(fun() -> comm:is_valid(comm:this()) end),
    comm:send_local(pid_groups:get_my(dht_node),
                    {get_node_details, comm:this(), [node]}),
    State;

%% While initializing
on_init({get_node_details_response, NodeDetails}, State) ->
    ?TRACE("tx_tm:on_init:get_node_details_response State; ~p~n", [_State]),
    IdSelf = node:id(node_details:get(NodeDetails, node)),
    NewState = state_set_id(State, IdSelf),
    gen_component:change_handler(NewState, fun ?MODULE:on/2);

on_init({new_node_id, Id}, State) ->
    state_set_id(State, Id);

%% do not accept new commit requests when not enough rtms are valid
on_init({tx_tm_commit, _Client, _ClientsID, _TransLog} = Msg, State) ->
    %% forward request to a node which is ready to serve requests.
    %% There, redirect message to tx_tm_new
    RedirectMsg = {?send_to_group_member, tx_tm_new, Msg},
    api_dht_raw:unreliable_lookup(?RT:get_random_node_id(), RedirectMsg),
    State.



-spec get_entry(any(), state()) -> tx_state() | undefined.
get_entry(ReqId, State) ->
    pdb:get(ReqId, state_get_tablename(State)).

-spec set_entry(tx_state(), state()) -> ok.
set_entry(NewEntry, State) ->
    pdb:set(NewEntry, state_get_tablename(State)).

%% -spec inform_client(tx_state(), state(), ?commit | ?abort) -> ok.
%% inform_client(TxState, State, Result) ->
%%     ?TRACE("tx_tm:inform client~n", []),
%%     Client = tx_state_get_client(TxState),
%%     ClientsId = tx_state_get_clientsid(TxState),
%%     ClientResult = case Result of
%%                        ?commit -> commit;
%%                        ?abort -> {abort, get_failed_keys(TxState, State)}
%%                    end,
%%     case Client of
%%         unknown -> ok;
%%         _       -> msg_commit_reply(Client, ClientsId, ClientResult)
%%     end,
%%    ok.

%% -spec get_my(, %% pid_groups:pidname(),
%%              atom()) -> pid() | failed.
%% get_my(Role, PaxosRole) ->
%%     PidName = list_to_existing_atom(
%%                 atom_to_list(Role) ++ "_" ++ atom_to_list(PaxosRole)),
%%     pid_groups:get_my(PidName).
%%
-spec state_get_tablename(state()) -> pdb:tableid().
state_get_tablename(State)     -> element(1, State).

-spec state_set_id(state(), ?RT:key()) -> state().
state_set_id(State, Id) -> setelement(2, State, Id).

%%-spec state_get_opentxnum(state()) -> non_neg_integer().
%% state_get_opentxnum(State) -> element(3, State).
-spec state_inc_opentxnum(state()) -> state().
state_inc_opentxnum(State) -> setelement(3, State, element(3, State) + 1).
%%-spec state_dec_opentxnum(state()) -> state().
%% state_dec_opentxnum(State) ->
%%     setelement(3, State, erlang:max(0, element(3, State) - 1)).


%% @doc Checks whether config parameters for tx_tm exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(replication_factor) and
    config:cfg_is_greater_than(replication_factor, 0)
    .

-spec tx_state_new(tx_id(), comm:mypid(), any(), tx_tlog:tlog()) -> tx_state().
tx_state_new(TxId, Client, ClientId, TLog) ->
    InvolvedKeys = [ tx_tlog:get_entry_key(X) || X <- TLog ],
    {TxId, ?tx_state, Client, ClientId, open, TLog,
     _NextWriteRound= '_', %% assigned by reply to txid_on_cseq:new
     prbr_bottom, %% last written val for txid
     _ToBeLockedKeys = InvolvedKeys,
     _ToBeCommittedKeys =
         [ {X, _NextRoundPlaceholder = '_', no_value_yet} %% written_value_placeholder}
           || X <- InvolvedKeys],
     _FailedLocks = []}.

%% ?enum_marker
%% ?enum_id
%% ?enum_client
%% ?enum_clientid
%% ?enum_status
%% ?enum_tlog
%% ?enum_txid_round
%% ?enum_txid_writtenval
%% ?enum_open_locks
%% ?enum_open_commits
%% ?enum_failed_locks

-spec tx_state_client(tx_state()) -> comm:mypid().
tx_state_client(TxState) -> element(?enum_client, TxState).

-spec tx_state_clients_id(tx_state()) -> any().
tx_state_clients_id(TxState) -> element(?enum_clientid, TxState).

-spec tx_state_tlog(tx_state()) -> tx_tlog:tlog().
tx_state_tlog(TxState) -> element(?enum_tlog, TxState).

-spec tx_state_txid_next_write_token(tx_state()) -> pr:pr() | '_'.
tx_state_txid_next_write_token(TxState) ->
    element(?enum_txid_round, TxState).

-spec tx_state_set_txid_next_write_token(tx_state(), pr:pr())
                             -> tx_state().
tx_state_set_txid_next_write_token(TxState, NextRound) ->
    setelement(?enum_txid_round, TxState, NextRound).

-spec tx_state_txid_written_value(tx_state()) -> any().
tx_state_txid_written_value(TxState) ->
    element(?enum_txid_writtenval, TxState).

-spec tx_state_set_txid_written_value(tx_state(), any()) -> tx_state().
tx_state_set_txid_written_value(TxState, WrittenVal) ->
    setelement(?enum_txid_writtenval, TxState, WrittenVal).

-spec tx_state_open_locks(tx_state()) -> [client_key() | binary() | ?RT:key()].
tx_state_open_locks(TxState) -> element(?enum_open_locks, TxState).

-spec tx_state_set_open_locks(tx_state(), [client_key() | binary() | ?RT:key()])
                             -> tx_state().
tx_state_set_open_locks(TxState, OpenLocks) ->
    setelement(?enum_open_locks, TxState, OpenLocks).

-spec tx_state_open_commits(tx_state()) ->
                                   [{client_key() | binary() | ?RT:key(),
                                     pr:pr() | '_',
                                     any()}].
tx_state_open_commits(TxState)    -> element(?enum_open_commits, TxState).

-spec tx_state_set_open_commits(tx_state(),
                                [{client_key() | binary() | ?RT:key(),
                                  pr:pr(),
                                  any()}])
                             -> tx_state().
tx_state_set_open_commits(TxState, FailedLocks) ->
    setelement(?enum_open_commits, TxState, FailedLocks).

-spec tx_state_add_nextround_writtenval_for_commit(
        tx_state(), client_key() | binary() | ?RT:key(),
        pr:pr(), any()) -> tx_state().
tx_state_add_nextround_writtenval_for_commit(TxState, Key, NextRound, WrittenVal) ->
    OpenCommits = tx_state_open_commits(TxState),
    NewOpenCommits = lists:keyreplace(Key, 1, OpenCommits,
                                      {Key, NextRound, WrittenVal}),
    tx_state_set_open_commits(TxState, NewOpenCommits).

-spec tx_state_failed_locks(tx_state())
                           -> [client_key() | binary() | ?RT:key()].
tx_state_failed_locks(TxState)    -> element(?enum_failed_locks, TxState).

-spec tx_state_set_failed_locks(tx_state(),
                                [client_key() | binary() | ?RT:key()])
                               -> tx_state().
tx_state_set_failed_locks(TxState, FailedLocks) ->
    setelement(?enum_failed_locks, TxState, FailedLocks).
