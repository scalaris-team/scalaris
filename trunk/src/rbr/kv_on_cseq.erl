% @copyright 2012 Zuse Institute Berlin,

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
%% @doc key value store based on rbrcseq.
%% @end
%% @version $Id:$
-module(kv_on_cseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([write/2]).

%% filters and checks for rbr_cseq operations
%% consistency
-export([is_valid_next_req/2]).
%% read filters
-export([rf_val/1]).
-export([rf_rl_wl_vers/1]).
%% write filters
-export([wf_set_vers_val/3]).


-type readlock()  :: non_neg_integer(). %% later: [tx_id_keys].
-type writelock() :: boolean(). %% later: tx_id_key | write_lock_is_free.
-type version()   :: non_neg_integer().
-type value()     :: any().

-type db_entry()  :: { %% plainkey(),
                readlock(),
                writelock(),
                version(),
                value()
               }.

-spec read(client_key()) -> api_tx:read_result().
read(Key) ->
    rbrcseq:qread(self(), Key, fun kv_on_cseq:rf_val/1),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _Round, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                           end
                      )
    end.

-spec write(client_key(), client_value()) -> api_tx:write_result().
write(Key, Value) ->
    rbrcseq:qwrite(self(), Key, fun kv_on_cseq:rf_rl_wl_vers/1,
                   fun kv_on_cseq:is_valid_next_req/2,
                   fun kv_on_cseq:wf_set_vers_val/3, Value),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _Round, _Value}, {ok} ) %%;
%%        ?SCALARIS_RECV({qwrite_deny, _ReqId, _Round, _Value}, {fail, timeout} )
    end.

%% inc(...)

%% append(...)

%% content checks
-spec is_valid_next_req(any(), {prbr:write_filter(), any()}) ->
                               {boolean(), any()}.
is_valid_next_req({RL, WL, Vers}, {_WriteFilter, _Val}) ->
    {RL =:= 0 andalso
     WL =:= false %% version check??, for single write inc is done implicitly
     , Vers}.

%% read filters
-spec rf_val(db_entry()) -> client_value().
rf_val(prbr_bottom) -> no_value_yet;
rf_val(X)          -> val(X).

-spec rf_rl_wl_vers(db_entry()) ->
        {readlock(), writelock(), non_neg_integer()}. %% drop -1 in the future?
rf_rl_wl_vers(prbr_bottom) -> {0, false, 0};
rf_rl_wl_vers(X)          -> {readlock(X),
                              writelock(X),
                              vers(X)}.

%% write filters

-spec wf_set_vers_val(db_entry() | prbr_bottom, non_neg_integer(),
                      client_value()) -> db_entry().
wf_set_vers_val(prbr_bottom, Version, WriteValue) ->
    {0, false, Version, WriteValue};
wf_set_vers_val(Entry, Version, WriteValue) ->
    T = set_vers(Entry, Version + 1),
    set_val(T, WriteValue).








%% abstract data type db_entry()

-spec readlock(db_entry()) -> readlock().
readlock(Entry) -> element(1, Entry).
-spec writelock(db_entry()) -> writelock().
writelock(Entry) -> element(2, Entry).
-spec vers(db_entry()) -> version().
vers(Entry) -> element(3, Entry).
-spec set_vers(db_entry(), version()) -> db_entry().
set_vers(Entry, Vers) -> setelement(3, Entry, Vers).
-spec val(db_entry()) -> value().
val(Entry) -> element(4, Entry).
-spec set_val(db_entry(), value()) -> db_entry().
set_val(Entry, Val) -> setelement(4, Entry, Val).


%% -spec val_vers(db_entry()) -> {value(), version()}.
%% val_vers(Entry) ->
%% {val(Entry), vers(Entry)}.

%% -spec set_val_vers(db_entry(), {value(), version()}) -> db_entry().
%% set_val_vers(Entry, {Val, Vers}) ->
%%     set_val(set_vers(Entry, Vers), Val).
%%
%% -spec rl_wl_vers(db_entry()) -> {readlock(), writelock(), version()}.
%% rl_wl_vers(Entry) ->
%%     {readlock(Entry), writelock(Entry), vers(Entry)}.
