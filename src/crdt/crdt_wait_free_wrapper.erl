% @copyright 2012-2020 Zuse Institute Berlin,

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
%% @doc    An wrapper around CRDTPaxos to make it wait-free
%% @end

-module(crdt_wait_free_wrapper).
-author('skrzypczak@zib.de').

-define(PDB, pdb).
-define(TRACE(X), ?TRACE(X, [])).
-define(TRACE(X,Y),
	case is_list(Y) of
		true ->
			ct:pal(X ++ "~n",Y);
		false ->
			ct:pal(X ++ "~n", [Y])
	end).

-include("scalaris.hrl").
-include("client_types.hrl").
-behaviour(gen_component).

%% protocol API
-export([write/5, read/5]).

%% gen_componenet message handling
-export([start_link/3]).
-export([init/1, on/2]).


%% protocol state
-define(get(X, Y), get_field(?i(X), Y)).
-define(set(X, Y, V), set_field(?i(X), Y, V)).
-define(add(X, Client, DataType, Fun, Y), add_to_buffer(?i(X), Client, DataType, Fun, Y)).

-define(i(X), #pstate.X).

-record(pstate,
	{
		%% state needed to execute updates
		update_in_progress = false :: boolean(),
		buffer_id = 0 :: non_neg_integer(),
		update_buffer = new_buffer() :: buffer(),
		curr_update_buffer = new_buffer() :: buffer(),

		%% state needed to execute queries
		query_id = 0 :: non_neg_integer(), %% needed to prevent sending multiple replies to client
		query_in_progress = false :: boolean(),
		query_buffer = new_buffer() :: buffer(),
		curr_query_buffer = new_buffer() :: buffer(),

		pending_queries = [] :: waitlist(),
		updates_waiting = [] :: waitlist()
	}).

-type buffer() :: [buffer_element()].
-type buffer_element() :: {{comm:mypid(), non_neg_integer()} | comm:erl_local_pid(),
	crdt:crdt_module(), crdt:query_fun() | crdt:update_fun()}.

-type waitlist() :: [{comm:mypid_plain(), non_neg_integer()}].

%$ scalaris state
-type state() ::
	{
		?PDB:tableid(),
		dht_node_state:db_selector(),
		[comm:mypid_plain()],
		crdt_proposer:state()	%% it this wrapper is running, crdt_proposer gen_component isn't!
	}.


-include("gen_component.hrl").

-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, DBSelector,
                             [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
	crdt_proposer:send_to_all_replicas(?RT:hash_key("1"),
		{crdt_acceptor, register_proposer, '_', comm:this(), 0, key}),
    {?PDB:new(?MODULE, [set]), DBSelector, [comm:this()], crdt_proposer:init(DBSelector)}.


%%%%%%%%% API
%% Do not use query/update as query is a reserved keyword...?
-spec read(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(),
	crdt:crdt_module(), crdt:query_fun()) -> ok.
read(CSeqPidName, Client, Key, DataType, QueryFun) ->
	Pid = pid_groups:find_a(CSeqPidName),
	comm:send_local(Pid, {new_request, {read, Client, Key, DataType, QueryFun}}).

-spec write(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(),
	crdt:crdt_module(), crdt:update_fun()) -> ok.
write(CSeqPidName, Client, Key, DataType, UpdateFun) ->
	Pid = pid_groups:find_a(CSeqPidName),
	comm:send_local(Pid, {new_request, {write, Client, Key, DataType, UpdateFun}}).


%%%%%%%%%% PROTOCOL IMPLEMENTATION
-spec on(comm:message(), state()) -> state().

on({new_request, {read, Client, Key, DataType, QueryFun}}, State) ->
	PState = get_pstate(Key, State),
	QueryId = ?get(query_id, PState) + 1,
	NP1 = ?set(query_id, PState, QueryId),
	NP2 = add_to_pending(Client, QueryId, NP1),
	save_pstate(Key, NP2, State),
	%% send to every proposer!
	%% This is needed to guarantee wait freedom, as only progress of a
	%% single proposer can be ensured.
	send_to_all_proposers({query_internal, {comm:this(), QueryId}, Key, DataType, QueryFun}, State),
	State;

on({query_internal, FromProposer, Key, DataType, QueryFun}, State) ->
	PState = get_pstate(Key, State),
	NewPState = ?add(query_buffer, FromProposer, DataType, QueryFun, PState),
	save_pstate(Key, NewPState, State),
	gen_component:post_op({process_query, Key}, State);

on({process_query, Key}, State) ->
	PState = get_pstate(Key, State),
	QueryBuf = ?get(query_buffer, PState),
	InProgress = ?get(query_in_progress, PState),
	case InProgress orelse QueryBuf == [] of
		true ->
			%% do not process current buffer
			State;
		false ->
			NP1 = ?set(query_in_progress, PState, true),

			%% base protocol query execution...
			DataType = element(2, hd(QueryBuf)), %% assumes every op access same datatype!
			QueryFuns = [element(3, E) || E <- QueryBuf],
			
			NP2 = ?set(query_buffer, NP1, new_buffer()),
			NP3 = ?set(curr_query_buffer, NP2, QueryBuf),
			save_pstate(Key, NP3, State),

			This = comm:reply_as(comm:this(), 3, {query_buffer_done, Key, '_'}),
			crdt_proposer:read(crdt_db, This, Key, DataType, QueryFuns),
			State
	end;

on({query_buffer_done, Key, _Result={read_done, Results}}, State) ->
	PState = get_pstate(Key, State),
	QueryBuf = ?get(curr_query_buffer, PState),
	notify_waiting_progress(Key, PState),
	NP1 = clear_waitlist(PState),
	NP2 = ?set(query_in_progress, NP1, false),
	save_pstate(Key, NP2, State),

	[
		comm:send(Proposer, {query_done, Key, QueryId, Result}) ||
		{{{Proposer, QueryId}, _, _}, Result} <- lists:zip(QueryBuf, Results)
	],
	gen_component:post_op({process_query, Key}, State);

on({query_done, Key, QueryId, Result}, State) ->
	PState = get_pstate(Key, State),
	case remove_pending(QueryId, PState) of
		{{Client, QueryId}, NewPState} ->
			inform_client(read_done, Client, Result),
			save_pstate(Key, NewPState, State);
		{none, _PState} ->
			%% client was already notified
			ok
	end,
	State;

%%%% update protocol
on({new_request, {write, Client, Key, DataType, UpdateFun}}, State) ->
	PState = get_pstate(Key, State),
	NewPState = ?add(update_buffer, Client, DataType, UpdateFun, PState),
	save_pstate(Key, NewPState, State),
	gen_component:post_op({process_update, Key}, State);	

on({process_update, Key}, State) ->
	PState = get_pstate(Key, State),
	UpdateBuf = ?get(update_buffer, PState),
	InProgress = ?get(update_in_progress, PState),
	case InProgress orelse UpdateBuf == [] of
		true ->
			%% do not process current buffer
			State;
		false -> 
			NP1 = ?set(update_in_progress, PState, true),
			BuffId = ?get(buffer_id, NP1),
			save_pstate(Key, NP1, State),
			case ?get(query_in_progress, PState) of
				true ->
					send_to_all_proposers({wait_for_progess, comm:this(), Key, BuffId}, State),
					State;
				false ->
					gen_component:post_op({progress_made, Key, BuffId}, State)
			end
	end;

on({progress_made, Key, ThisBuffId}, State) ->
	PState = get_pstate(Key, State),
	BuffId = ?get(buffer_id, PState),
	case ?get(buffer_id, PState) =/= ThisBuffId of
		true ->
			%% this is an outdated progress message which we can ignore
			State;
		false ->
			UpdateBuf = ?get(update_buffer, PState),
			NP1 = ?set(curr_update_buffer, PState, UpdateBuf),
			NP2 = ?set(update_buffer, NP1, new_buffer()),
			NP3 = ?set(buffer_id, NP2,BuffId + 1),
			
			%% base protocol query execution...
			DataType = element(2, hd(UpdateBuf)), %% assumes every op access same datatype!
			UpdateFuns = [element(3, E) || E <- UpdateBuf],
			save_pstate(Key, NP3, State),

			This = comm:reply_as(comm:this(), 3, {update_buffer_done, Key, '_'}),
			crdt_proposer:write(crdt_db, This, Key, DataType, UpdateFuns),
			State
	end;

on({update_buffer_done, Key, {write_done}}, State) ->
	PState = get_pstate(Key, State),
	CurrBuff = ?get(curr_update_buffer, PState),
	NP1 = ?set(update_in_progress, PState, false),
	inform_all_clients(write_done, CurrBuff),
	save_pstate(Key, NP1, State),
	gen_component:post_op({process_update, Key}, State);

on({wait_for_progess, Proposer, Key, BuffId}, State) ->
	PState = get_pstate(Key, State),
	case ?get(query_in_progress, PState) of
		true ->
			NP1 = add_to_waitlist(Proposer, BuffId, PState),
			save_pstate(Key, NP1, State);
		false ->
			%% no query in progress -> we can immediately reply
			comm:send(Proposer, {progress_made, Key, BuffId})
	end,
	State;

on({registered_proposers, ProposerList}, State) ->
	OldList = proposerlist(State),
	case length(OldList) > length(ProposerList) of
		true ->
			State;
		false ->
			set_proposerlist(State, ProposerList)
	end;

%% forward unknown messages to crdt_proposer!
on(UnknownMessage, State) ->
	%?TRACE("crdt_proposer msg ~n~p", UnknownMessage),
	ProposerState = proposerstate(State),
	NewProposerState = crdt_proposer:on(UnknownMessage, ProposerState),
	set_proposerstate(State, NewProposerState).

%%%%%%% STATE MANAGEMENT
-spec get_pstate(client_key(), state()) -> #pstate{}.
get_pstate(Key, State) ->
    case ?PDB:get(Key, tablename(State)) of
    	undefined ->
    		#pstate{};
    	{Key, PState} ->
    		PState
    end.

-spec save_pstate(client_key(), #pstate{}, state()) -> ok.
save_pstate(Key, PState, State) ->
    ?PDB:set({Key, PState}, tablename(State)).

-spec get_field(non_neg_integer(), #pstate{}) -> any().
get_field(FieldIdx, PState) ->
	element(FieldIdx, PState).

-spec set_field(non_neg_integer(), #pstate{}, any()) -> #pstate{}.
set_field(FieldIdx, PState, NewValue) ->
	setelement(FieldIdx, PState, NewValue).

-spec new_buffer() -> buffer().
new_buffer() -> [].

-spec add_to_buffer(non_neg_integer(), {comm:mypid(), non_neg_integer()} | comm:erl_local_pid(),
	crdt:crdt_module(), crdt:query_fun() | crdt:update_fun(), #pstate{}) -> #pstate{}.
add_to_buffer(BufIdx, Client, DataType, Fun, PState) ->
	BufEle = {Client, DataType, Fun},
	NewBuf = [BufEle | element(BufIdx, PState)],
	setelement(BufIdx, PState, NewBuf).

-spec add_to_pending(comm:mypid_plain(), non_neg_integer(), #pstate{}) -> #pstate{}.
add_to_pending(Client, Id, PState) ->
	Pending = ?get(pending_queries, PState),
	NewPending = [{Client, Id} | Pending],
	?set(pending_queries, PState, NewPending).

-spec remove_pending(non_neg_integer(), #pstate{}) ->
	{none | {comm:mypid_plain(), non_neg_integer()}, #pstate{}}.
remove_pending(Id, PState) -> 
	Pending = ?get(pending_queries, PState),
	{Removed, NewPending} = pd_helper(Id, Pending, []),
	{Removed, ?set(pending_queries, PState, NewPending)}.

-spec pd_helper(non_neg_integer(), waitlist(), waitlist()) -> 
	{none | {comm:mypid_plain(), non_neg_integer()}, waitlist()}.
pd_helper(_Id, [], Pending) -> {none, Pending};
pd_helper(Id, [E={_, Id} | T], Pending) -> {E, T ++ Pending};
pd_helper(Id, [H | T], Pending) -> pd_helper(Id, T, [H | Pending]).

-spec add_to_waitlist(comm:mypid_plain(), non_neg_integer(), #pstate{}) -> #pstate{}.
add_to_waitlist(Client, Id, PState) ->
	Waitlist = ?get(updates_waiting, PState),
	NewWaitList = wl_helper(Client, Id, Waitlist),
	?set(updates_waiting, PState, NewWaitList).

% only keep the maximal bufferID for each proposer
-spec wl_helper(comm:mypid_plain(), non_neg_integer(), waitlist()) -> waitlist().
wl_helper(P, B, [])	-> [{P, B}];
wl_helper(P, B, [{P, OB} | T]) -> [{P, max(B, OB)} | T];
wl_helper(P, B, [H | T]) -> [H | wl_helper(P, B, T)].

-spec clear_waitlist(#pstate{}) -> #pstate{}.
clear_waitlist(PState) ->
	?set(updates_waiting, PState, []).

-spec tablename(state()) -> ?PDB:tableid().
tablename(State) -> element(1, State).

-spec proposerlist(state()) -> [comm:mypid_plain()].
proposerlist(State) -> element(3, State).
-spec set_proposerlist(state(), [comm:mypid_plain()]) -> state().
set_proposerlist(State, ProposerList) -> setelement(3, State, ProposerList).

-spec proposerstate(state()) -> crdt_proposer:state().
proposerstate(State) -> element(4, State).
-spec set_proposerstate(state(), crdt_proposer:state()) -> state().
set_proposerstate(State, ProposerState) -> setelement(4, State, ProposerState).

%%%%% Communication helper
-spec send_to_all_proposers(any(), state()) -> ok.
send_to_all_proposers(Msg, State) ->
	ProposerList = proposerlist(State),
	[comm:send(Proposer, Msg) || Proposer <- ProposerList],
	ok.

-spec notify_waiting_progress(client_key(), #pstate{}) -> ok.
notify_waiting_progress(Key, PState) ->
	WaitList = ?get(updates_waiting, PState),
	[comm:send(Proposer, {progress_made, Key, BuffId}) ||
		{Proposer, BuffId} <- WaitList],
	ok.


-spec inform_all_clients(atom(), buffer()) -> ok.
inform_all_clients(write_done, Buffer) ->
	[inform_client(write_done, E) || E <- Buffer],
	ok.

-spec inform_client(write_done, buffer_element()) -> ok.
inform_client(write_done, BufEle) ->
	Client = element(1, BufEle),
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(Client, {write_done});
        false ->
            comm:send_local(Client, {write_done})
    end.

-spec inform_client(read_done, comm:erl_local_pid(), any()) -> ok.
inform_client(read_done, Client, QueryResult) ->
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(Client, {read_done, QueryResult});
        false ->
            comm:send_local(Client, {read_done, QueryResult})
    end.
