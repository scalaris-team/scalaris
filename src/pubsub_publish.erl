%  @copyright 2008-2011 Zuse Institute Berlin

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
%% @doc Publish function
%% @end
%% @version $Id$
-module(pubsub_publish).
-author('schuett@zib.de').
-vsn('$Id$').

-export([publish/3, publish_internal/3]).

%%====================================================================
%% public functions
%%====================================================================

%% @doc publishs an event to a given url.
%% @todo use pool:pspawn
-spec publish(URL::string(), Topic::string(), Content::string()) -> ok.
publish(URL, Topic, Content) ->
    spawn(pubsub_publish, publish_internal, [URL, Topic, Content]),
    ok.

-spec publish_internal(URL::string(), Topic::string(), Content::string()) -> {ok, {response, Result::[term()]}} | {error, Reason::term()}.
publish_internal(URL, Topic, Content) ->
    jsonrpc_call(URL, [], {call, notify, [Topic, Content]}).

% these two methods originated in the jsonrpc.erl file from yaws and have been
% removed in version 1.90 - we do need them however
% -> these are copied from version 1.89 and have been adapted to 1.90

jsonrpc_call(URL, Options, Payload) -> 
    try
        {ok, CallPayloadDeep} = jsonrpc_encode_call_payload(Payload),
        CallPayload = lists:flatten(CallPayloadDeep),
        {ok, Response} = httpc:request(post, 
            {URL,[{"Content-length",length(CallPayload)}],
             "application/x-www-form-urlencoded",CallPayload}, 
                                      Options, []),

        RespBody= if (size(Response) == 2) or (size(Response) == 3) -> 
                          element(size(Response), Response) 
                  end,
        jsonrpc_decode_call_payload(RespBody)
    catch
        error:Err-> 
            error_logger:error_report([{'json_rpc:call', error}, 
                                       {error, Err}, 
                                       {stack, erlang:get_stacktrace()}]),
            {error,Err}
    end. 

%%%
%%% json-rpc.org defines such structure for making call
%%% 
%%% {"method":"methodname", "params": object, "id": integer}
jsonrpc_encode_call_payload({call, Method, Args})
  when is_atom(Method) and is_list(Args) ->
    Struct =  json2:encode({struct, [{method, atom_to_list(Method)}, 
                                    {params, {array, Args}}, 
                                    {id, 0}]}),
    {ok, Struct}.

%%%
%%% decode response structure
%%% 
%%% {"id":requestID,"result":object,"error":error_description}
jsonrpc_decode_call_payload(JSonStr) -> 
    {ok, JSON} = json2:decode_string(JSonStr),
    Result = jsonrpc:s(JSON, result),
    Error = jsonrpc:s(JSON, error),
%    ID = jsonrpc:s(JSON, id),    % ignored for now
    if 
        (Error =/= undefined) -> 
            {error, Error};
        true -> 
            {ok,{response,[Result]}} % make it compliant with xmlrpc response
    end. 
