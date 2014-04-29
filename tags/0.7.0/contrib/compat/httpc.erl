%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2002-2010. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
%%

-module(httpc).

%% Deprecated
-export([
     request/1, request/2, request/4, request/5,
     cancel_request/1, cancel_request/2,
     set_option/2, set_option/3,
     set_options/1, set_options/2,
     verify_cookies/2, verify_cookies/3, 
     cookie_header/1, cookie_header/2, 
     stream_next/1,
     default_profile/0
    ]).


%%%=========================================================================
%%%  API
%%%=========================================================================

%%--------------------------------------------------------------------------
%% request(Url [, Profile]) ->
%% request(Method, Request, HTTPOptions, Options [, Profile])
%%--------------------------------------------------------------------------

request(Url)          -> http:request(Url).
request(Url, Profile) -> http:request(Url, Profile).

request(Method, Request, HttpOptions, Options) ->
    http:request(Method, Request, HttpOptions, Options). 
request(Method, Request, HttpOptions, Options, Profile) ->
    http:request(Method, Request, HttpOptions, Options, Profile). 


%%--------------------------------------------------------------------------
%% cancel_request(RequestId [, Profile])
%%-------------------------------------------------------------------------

cancel_request(RequestId) ->
    http:cancel_request(RequestId).
cancel_request(RequestId, Profile) ->
    http:cancel_request(RequestId, Profile).


%%--------------------------------------------------------------------------
%% set_options(Options [, Profile])
%% set_option(Key, Value [, Profile])
%%-------------------------------------------------------------------------

set_options(Options) ->
    http:set_options(Options).
set_options(Options, Profile) ->
    http:set_options(Options, Profile).

set_option(Key, Value) ->
    http:set_option(Key, Value).
set_option(Key, Value, Profile) ->
    http:set_option(Key, Value, Profile).


%%--------------------------------------------------------------------------
%% verify_cookies(SetCookieHeaders, Url [, Profile])
%%-------------------------------------------------------------------------

verify_cookies(SetCookieHeaders, Url) ->
    http:store_cookies(SetCookieHeaders, Url).
verify_cookies(SetCookieHeaders, Url, Profile) ->
    http:store_cookies(SetCookieHeaders, Url, Profile).


%%--------------------------------------------------------------------------
%% cookie_header(Url [, Profile])
%%-------------------------------------------------------------------------

cookie_header(Url) ->
    http:cookie_header(Url).
cookie_header(Url, Profile) ->
    http:cookie_header(Url, Profile).


%%--------------------------------------------------------------------------
%% stream_next(Pid)
%%-------------------------------------------------------------------------

stream_next(Pid) ->
    http:stream_next(Pid).


%%--------------------------------------------------------------------------
%% default_profile()
%%-------------------------------------------------------------------------

default_profile() ->
    http:default_profile().
