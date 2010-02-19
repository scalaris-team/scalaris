%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%%-------------------------------------------------------------------
%%% File    : ioutils.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : IO Helpers
%%%
%%% Created : 26 Mar 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------

-module(ioutils).

-author('schuett@zib.de').

-export([for_each_line_in_file/4]).

for_each_line_in_file(Name, Proc, Mode, Accum0) ->
    {ok, Device} = file:open(Name, Mode),
    for_each_line(Device, Proc, Accum0).

for_each_line(Device, Proc, Accum) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device), Accum;
        Line -> 
	    CleanLine = string:substr(Line, 1, string:len(Line) - 1),
	    NewAccum = Proc(CleanLine, Accum),
	    for_each_line(Device, Proc, NewAccum)
    end.
