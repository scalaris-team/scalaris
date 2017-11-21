% @copyright 2014-2017 Zuse Institute Berlin,

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
%% @doc    Provides a minimalistic interface to read or increment an integer
%%         value based on rbrcseq.
%% @end
%% @version $Id$
-module(inc_on_cseq).
-author('skrzypczak@zib.de').
-vsn('$Id:$ ').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([inc/1]).

-export([rf_val/1]).
-export([rf_none/1]).
-export([cc_noop/3]).
-export([wf_inc/3]).

-spec read(client_key()) -> {ok, client_value()} | {fail, not_found}.
read(Key) ->
    rbrcseq:qread(kv_db, self(), ?RT:hash_key(Key), ?MODULE, fun ?MODULE:rf_val/1),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                           end
                      )
    after 1000 ->
        log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                            case Value of
                                no_value_yet -> {fail, not_found};
                                _ -> {ok, Value}
                            end
                          )
       end
 end.

%% increments value stored at Key. For simplicity, does not fail if current
%% value is not an integer. Instead, the value will be replaced by 1.
-spec inc(client_key()) -> {ok}.
inc(Key) ->
    rbrcseq:qwrite(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                   fun ?MODULE:rf_none/1, fun ?MODULE:cc_noop/3, fun ?MODULE:wf_inc/3, 1),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet}, {ok});
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                       begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                       {ok} end)
    after 1000 ->
            log:log("~p write hangs at key ~p, ~p~n",
                    [self(), Key, erlang:process_info(self(), messages)]),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet},
                               begin
                                   log:log("~p write was only slow at key ~p~n",
                                           [self(), Key]),
                                   {ok}
                               end); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                               begin log:log("~p Write failed: ~p~n",
                                             [self(), Reason]),
                                     {ok} end)
                end
    end.

-spec rf_val(prbr_bottom | non_neg_integer()) -> non_neg_integer().
rf_val(prbr_bottom) -> 0;
rf_val(X) -> X.

-spec rf_none(any()) -> none.
rf_none(_) -> none.

-spec cc_noop(any(), any(), any()) -> {true, none}.
cc_noop(_,_,_) -> {true, none}.

-spec wf_inc(prbr_bottom | non_neg_integer(), none, non_neg_integer()) -> {non_neg_integer(), none}.
wf_inc(prbr_bottom, none, ToAdd) -> {ToAdd, none};
wf_inc(Val, none, ToAdd) when not is_integer(Val) -> {ToAdd, none};
wf_inc(Val, none, ToAdd) ->  {Val + ToAdd, none}.
