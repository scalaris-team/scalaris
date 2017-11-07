dotto
-----

patcho dotto...

a data driven data structure manipulation library for erlang, a superset of
`json patch <http://tools.ietf.org/html/rfc6902>`_

The primary change of this fork is the replacement of maps with
dicts to support older erlang versions.

tests
-----

::

    make tests

this are just basic, hardcoded tests

usage
-----

TODO: Due to compatibility with older erlang versions, maps where replaced
with dicts.

::

    Data = #{name => "bob", age => 29, friends => ["sandy", "patrick"], data => #{numbers => [10,11,12]}}.

    dotto:remove(Data, [friends, 1]).

    % {ok,#{age => 29, data => #{numbers => "\n\v\f"}, friends => ["sandy"], name => "bob"}}

    dotto:replace(Data, [data, numbers, 2], 42).

    % {ok,#{age => 29, data => #{numbers => "\n\v*"}, friends => ["sandy","patrick"], name => "bob"}}

    % <<"-">> means append at the end, see json patch rfc
    dotto:add(Data, [friends, <<"-">>], "plankton").

    % {ok,#{age => 29, data => #{numbers => "\n\v\f"}, friends => ["sandy","patrick","plankton"], name => "bob"}}

API
---

this are the direct implementations of the RFC methods::

    add(Obj, Path, Val): add value at Path to Val
    remove(Obj, Path): remove value in Path
    replace(Obj, Path, Val): set value in Path to Val if set
    move(Obj, FromPath, ToPath): move value from FromPath to ToPath (remove + add)
    copy(Obj, FromPath, ToPath): copy value from FromPath to ToPath (fetch + add)
    test(Obj, Path, Val): tests that Path contains Val, {ok, true} if equal, {ok, false} if not equal, {error, Reason} if error.

extra methods::

    fetch(Obj, Path): {ok, Value} if found, {error, Reason} otherwise
    fetch(Obj, Path, Default): {ok, Value} if found, {ok, Default} otherwise

License
-------

MPL 2.0
