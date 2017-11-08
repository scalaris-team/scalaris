% @copyright 2012-2017 Zuse Institute Berlin,

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

%% @author Jan Skrzypczak  <skrzypczak@zib.de>
%% @doc    Replicated storage of JSON objects based on rbrcseq. Implements
%%         a superset of RFC 6902 (JSON Patch) which allows  partial updates on
%%         the stored objects. This is based on dotto. See scalaris/contrib or:
%%              https://github.com/rumoe/dotto
%%
%%         In its current state, erlang dicts are used to represent JSON objects.
%% @end
%% @version $Id$
-module(json_on_cseq).
-author('skrzypczak@zib.de').
-vsn('$Id:$ ').

-define(NON_EXIST_VAL, no_value_yet).

-include("scalaris.hrl").
-include("client_types.hrl").


%% PUBLIC API
-export([read/1]).
-export([write/2]).
-export([patch/2]).

-export([fetch/2]).

%% READ FILTER
-export([rf_empty/1]).
-export([rf_val/1]).

%% CONTENT CHECK
-export([cc_noop/3]).

%% WRITE FILTER
-export([wf_val/3]).
-export([wf_patch/3]).

-ifdef(namespaced_dict).
-type json() :: dict:dict().
-else.
-type json() :: dict().
-endif.

%% A JSON patch is list of sequentially executed patch commands.
-type patch() :: [patch_cmd()].

%% Valid patch commands as per RFC 6902.
%% Consult https://tools.ietf.org/html/rfc6902 for the effects of the respective
%% commands on a given JSON object.
-type patch_cmd() :: {Op :: remove, Path :: path()} |
                     {Op :: add | replace | test, Path :: path(), Value :: any()} |
                     {Op :: move | copy, From :: path(), Path :: path()}.

%% JSON Path similar to RFC 6901 (https://tools.ietf.org/html/rfc6901).
%% Howerver, instead of strings ("/this/is/a/path"), lists are used
%% ([this, is, a, path]).
-type path() :: [any()].

%% @doc Read full JSON object stored at given key.
%% Returns
%%      {ok, JsonObject}    - JSON Object stored at given key.
%%      {fail, not_fount}   - No value at this key exists.
-spec read(client_key()) -> {ok, client_value()} | {fail, not_found}.
read(Key) ->
    read_helper(Key, fun ?MODULE:rf_val/1).

%% @doc Partially read a JSON object stored at a given key.
-spec fetch(client_key(), path()) ->
    {ok, client_value()} | {fail, not_fount} | {error, any()}.
fetch(Key, Path) ->
    read_helper(Key, get_rf_fetch_fun(Path)).

%% @doc Write full JSON object to a given key.
%% Returns:
%%      ok                  - JSON Object was successfully written
%%      {fail, Reason}      - The write was denied by scalaris.
-spec write(client_key(), json()) -> ok | {fail, any()}.
write(Key, Value) ->
    write_helper(Key, Value, fun ?MODULE:wf_val/3).

%% %%%%%%%%%%%%%%%%
%% JSON PATCH SPECIFIC API
%% %%%%%%%%%%%%%%%%

%% @doc Apply a JSON patch to the JSON object at a given key. A patch consists
%% of a list of patch commands, which are applied sequentially to the JSON
%% object. If all operations are performed successfully, then the resulting
%% JSON object replaces the currently stored value. If *any* command fails,
%% the stored object will not be modified.
%% Returns:
%%      ok                  - The patch was successfully applied
%%      {error, ErrorList}  - At least one patch command failed.
%%      {fail, Reason}      - The write was denied by scalaris.
-spec patch(client_key(), patch_cmd() | patch()) ->
    ok | {fail | error,any()}.
patch(Key, PatchCommand) when not is_list(PatchCommand) ->
    patch(Key, [PatchCommand]);
patch(Key, Patch) ->
    write_helper(Key, Patch, fun ?MODULE:wf_patch/3).


%% %%%%%%%%%%%%%%%%
%% READ FILTER
%% %%%%%%%%%%%%%%%%
%% @doc RedFilter that always returns an empty value
-spec rf_empty(any()) -> none.
rf_empty(_Any) -> none.

%% @doc ReadFilter returning the full JSON object
-spec rf_val(json() | prbr_bottom) -> client_value() | ?NON_EXIST_VAL.
rf_val(prbr_bottom) -> ?NON_EXIST_VAL;
rf_val(X)           -> X.

%% @doc Returns a ReadFilter to partially read a JSON object.
-spec get_rf_fetch_fun(path()) -> prbr:read_filter().
get_rf_fetch_fun(Path) ->
    fun
        (prbr_bottom) -> ?NON_EXIST_VAL;
        (Obj)         ->
            case dotto:fetch(Obj, Path) of
                {ok, Result} -> Result;
                Any -> Any
            end
    end.


%% %%%%%%%%%%%%%%%%
%% CONTENT CHECK
%% %%%%%%%%%%%%%%%%
%% @doc Empty ContentCheck that always passes. And gives no update information
%% (none) to the WriteFilter
-spec cc_noop(any(), prbr:write_filter(), any()) -> {true, none}.
cc_noop(_ReadVal, _WriteFilter, _ValToWrite) -> {true, none}.

%% %%%%%%%%%%%%%%%%
%% WRITE FILTER
%% %%%%%%%%%%%%%%%%
%% @doc WriteFilter that replaces the current value with the client provided one.
-spec wf_val(json() | prbr_bottom, any(), json()) -> {json(), ok}.
wf_val(_OldVal, _UpdateInfo, ValToWrite) -> {ValToWrite, ok}.

%% @doc WriteFilter that applies a list of patch commands to a JSON object.
%% A non-existing JSON object is treated as an empty object (dict:new()).
-spec wf_patch(json() | prbr_bottom, any(), patch()) ->  {json(), ok | {error, [any()]}}.
wf_patch(prbr_bottom, UI, Patch) -> wf_patch(dict:new(), UI, Patch);
wf_patch(Json, _UI, Patch) ->
    case dotto:apply(Patch, Json) of
        {ok, ResultJson} -> {ResultJson, ok};
        {error, _, Errors} -> {Json, {error, Errors}}
    end.


%% %%%%%%%%%%%%%%%%
%% INTERNAL HELPER
%% %%%%%%%%%%%%%%%%
-spec read_helper(client_key(), prbr:read_filter()) ->
    {ok, client_value()} | {fail, not_found} | {error, any()}.
read_helper(Key, ReadFilter) ->
    rbrcseq:qread(kv_db, self(), ?RT:hash_key(Key), ?MODULE, ReadFilter),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                       case Value of
                           ?NON_EXIST_VAL -> {fail, not_found};
                           {error, Reason} -> {error, Reason};
                           _ -> {ok, Value}
                           end
                      )
    after 1000 ->
        log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                            case Value of
                                ?NON_EXIST_VAL -> {fail, not_found};
                                {error, Reason} -> {error, Reason};
                                _ -> {ok, Value}
                            end
                          )
        end
    end.

-spec write_helper(client_key(), client_value(), prbr:write_filter()) ->
    ok | {fail | error, any()}.
write_helper(Key, Value, WriteFilter) ->
    write_helper(Key, Value, fun ?MODULE:rf_empty/1, fun ?MODULE:cc_noop/3,
                  WriteFilter).

-spec write_helper(client_key(), client_value(), prbr:read_filter(),
                   fun((any(), any(), any()) -> any()), prbr:write_filter()) ->
    ok | {fail | error, any()}.
write_helper(Key, Value, ReadFilter, ContentCheck, WriteFilter) ->
    rbrcseq:qwrite(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                   ReadFilter, ContentCheck, WriteFilter, Value),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, WriteRet}, WriteRet);
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                        begin
                            log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                            {fail, Reason}
                        end)
    after 1000 ->
        log:log("~p write hangs at key ~p, ~p~n",
                [self(), Key, erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, Value, WriteRet},
                            begin
                                log:log("~p write was only slow at key ~p~n", [self(), Key]),
                                WriteRet
                            end);
            ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                            begin
                                log:log("~p Write failed: ~p~n", [self(), Reason]),
                                {fail, Reason}
                            end)
            end
    end.
