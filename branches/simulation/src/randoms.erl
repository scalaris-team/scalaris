%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%----------------------------------------------------------------------
%%% File    : randoms.erl
%%% Author  : Thorsten Schuett
%%% Purpose :
%%% Created : 13 Dec 2002 by Alexey Shchepin <alexey@sevcom.net>
%%% Id      : $Id$
%%%----------------------------------------------------------------------

%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2002-2007 Alexey Shchepin
%% @version $Id$
-module(randoms).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("chordsharp.hrl").

-export([getRandomId/0, start/0, rand_uniform/2, init/2, start_seed/1]).

%% @doc generates a random id
%% @spec getRandomId() -> list()



-ifdef(SIMULATION).

start() -> 
    crypto:start().

start_seed(Seed) ->
    spawn_link(?MODULE,init,[Seed,self()]),
    receive 
        {ok} ->
                ok
    end.

getRandomId() ->
    random ! {getRandomId,self()},
    receive
         {getRandomId_response,T} ->
               T
    end.
    
rand_uniform(L,H) ->
  
    random ! {rand_uniform,L,H,self()},
    receive
         {rand_uniform_response,T} ->
               
               T
    end.
    

    

-else.


start() -> crypto:start().
getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).
rand_uniform(L,H) ->
    crypto:rand_uniform(L, H).


-endif.

init({A,B,C},Sup) ->
    random:seed(A,B,C),
    register(random,self()),
    Sup ! {ok},
    loop().

loop() ->
    
    receive
        {rand_uniform,L,H,S} ->
            %S ! crypto:rand_uniform(L, H),
            S !  {rand_uniform_response,random:uniform(H-L)+L-1},
            loop();
        {getRandomId,S} ->
            S ! {getRandomId_response,integer_to_list(random:uniform(65536 * 65536))},
            loop();
        _ -> 
            io:format("Randoms unhadle message"),
             halt(-1)
     end.

