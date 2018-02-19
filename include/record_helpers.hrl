%  @copyright 2010-2018 Zuse Institute Berlin

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
%% @doc    Helper macros for record definitions
%% @end
%% @version $Id$

% thanks to Vincent de Phily for this macro to specify required fields in
% record where no hard-coded value makes sense at the time of the type
% definition:
-define(required(Record, Field),
        throw({record_field_required, Record, Field, ?MODULE})).
