% @copyright 2008-2014 Zuse Institute Berlin

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
%% @doc Service for loggins messages.
%% @version $Id$
-module(comm_logger).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_server).

-include("scalaris.hrl").

%% API
-export([start_link/0]).

-export([log/3, dump/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([stat_tree/0]).

-type stat_tree() :: gb_trees:tree(Tag::atom(), {Size::non_neg_integer(), Count::pos_integer()}).
-record(state, {start    :: erlang_timestamp(),
                received :: stat_tree(),
                sent     :: stat_tree()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
-spec start_link() -> {ok,pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: log(Tag, Size) -> ok
%% Description: logs a message type with its size
%%--------------------------------------------------------------------
-spec log('send' | 'rcv', term(), non_neg_integer()) -> ok.
log(SendRcv, Tag, Size) ->
    gen_server:cast(?MODULE, {log, SendRcv, Tag, Size}).

%%--------------------------------------------------------------------
%% Function: dump() -> {gb_tree:gb_trees(), {Date, Time}}
%% Description: gets the logging state
%%--------------------------------------------------------------------
-spec dump() -> {Received::stat_tree(), Sent::stat_tree(), erlang_timestamp()}.
dump() ->
    gen_server:call(?MODULE, {dump}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
-spec init(list()) -> {ok, #state{}}.
init([]) ->
    {ok, #state{start=os:timestamp(), received=gb_trees:empty(), sent=gb_trees:empty()}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
-spec handle_call({dump}, any(), #state{})
        -> {reply, {Received::stat_tree(), Sent::stat_tree(), erlang_timestamp()}, #state{}}.
handle_call({dump}, _From, State) ->
    Reply = {State#state.received, State#state.sent, State#state.start},
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
-spec handle_cast({log, 'send' | 'rcv', atom(), non_neg_integer()}, #state{})
        -> {noreply, #state{}}.
handle_cast({log, 'rcv', Tag, Size}, State) ->
    case gb_trees:lookup(Tag, State#state.received) of
        none ->
            {noreply, State#state{received=gb_trees:insert(Tag, {Size, 1}, State#state.received)}};
        {value, {OldSize, OldCount}} ->
            {noreply, State#state{received=gb_trees:update(Tag, {Size + OldSize, OldCount + 1}, State#state.received)}}
    end;
handle_cast({log, 'send', Tag, Size}, State) ->
    case gb_trees:lookup(Tag, State#state.sent) of
        none ->
            {noreply, State#state{sent=gb_trees:insert(Tag, {Size, 1}, State#state.sent)}};
        {value, {OldSize, OldCount}} ->
            {noreply, State#state{sent=gb_trees:update(Tag, {Size + OldSize, OldCount + 1}, State#state.sent)}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
-spec handle_info(any(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
-spec terminate(any(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
