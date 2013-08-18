%  @copyright 2010-2013 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Type definitions for the client, e.g. api_*.
%% @end
%% @version $Id$

%% client_key() is actually of type unicode:chardata().
%% But we cannot generate that type in our tester as it contains
%% maybe_improper_list() without further specifying its element type.
%% See ?RT:hash_key/1
%% -type client_key() :: unicode:chardata().
-type client_key() :: string().
-type client_value() :: any().
-type client_version() :: non_neg_integer(). %%?DB:version().
