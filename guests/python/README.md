# Python Guest

## Build
Use:

```console
just debug
```

or

```console
just release
```

## Python Version
We currently bundle [Python 3.14.0rc2].

## Python Standard Library
In contrast to a normal Python installation there are a few notable public[^public] modules **missing** from the [Python Standard Library]:

- [`curses`](https://docs.python.org/3/library/curses.html)
- [`ensurepip`](https://docs.python.org/3/library/ensurepip.html)
- [`fcntl`](https://docs.python.org/3/library/fcntl.html)
- [`grp`](https://docs.python.org/3/library/grp.html)
- [`idlelib`](https://docs.python.org/3/library/idle.html)
- [`mmap`](https://docs.python.org/3/library/mmap.html)
- [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html)
- [`pip`](https://pip.pypa.io/)
- [`pwd`](https://docs.python.org/3/library/pwd.html)
- [`readline`](https://docs.python.org/3/library/readline.html)
- [`resource`](https://docs.python.org/3/library/resource.html)
- [`syslog`](https://docs.python.org/3/library/syslog.html)
- [`termios`](https://docs.python.org/3/library/termios.html)
- [`tkinter`](https://docs.python.org/3/library/tkinter.html)
- [`turtledemo`](https://docs.python.org/3/library/turtle.html#module-turtledemo)
- [`venv`](https://docs.python.org/3/library/venv.html)
- [`zlib`](https://docs.python.org/3/library/zlib.html)

Some modules low level modules like [`os`](https://docs.python.org/3/library/os.html) may not offer all methods, types, and constants.

## Dependencies
We do not bundle any additional libraries at the moment. It is currently NOT possible to install your own dependencies.

## Methods
Currently we only support [Scalar UDF]s. One can write it using a simple Python function:

```python
def add_one(x: int) -> int:
    return x + 1
```

You may register multiple methods in one Python source text. Imported methods and private methods starting with `_` are ignored.

## Types
Types are mapped to/from [Apache Arrow] as follows:

| Python       | Arrow       |
| ------------ | ----------- |
| [`bool`]     | [`Boolean`] |
| [`datetime`] | [`Timestamp`] w/ [`Microsecond`] and NO timezone |
| [`float`]    | [`Float64`] |
| [`int`]      | [`Int64`]   |
| [`str`]      | [`Utf8`]    |

Additional types may be supported in the future.

## NULLs
NULLs are rather common in database contexts and a first-class citizen in [Apache Arrow] and [Apache DataFusion]. If you do not want to deal with it, just define your method with simple scalar types and we will skip NULL rows for you:

```python
def add_simple(x: int, y: int) -> int:
    return x + y
```

However, you can opt into full NULL handling. In Python, NULLs are expressed as optionals:

```python
def add_nulls(x: int | None, y: int | None) -> int | None:
    if x is None or y is None:
        return None
    return x + y
```

or via the older syntax:

```python
from typing import Optional

def add_old(x: Optional[int], y: Optional[int]) -> Optional[int]:
    if x is None or y is None:
        return None
    return x + y
```

You may also partially opt into NULL handling for one parameter:

```python
def add_left(x: int | None, y: int) -> int | None:
    if x is None:
        return None
    return x + y

def add_right(x: int, y: int | None) -> int | None:
    if y is None:
        return None
    return x + y
```

Note that if you define the return type as non-optional, you MUST NOT return `None`. Otherwise, the execution will fail.

To give you a better idea when a Python method is called, consult this table:

| `x`    | `y`    | `add_simple` | `add_nulls` | `add_left` | `add_right` |
| ------ | ------ | ------------ | ----------- | ---------- | ----------- |
| `None` | `None` | 𐄂            | ✓           | 𐄂          | 𐄂           |
| `None` | some   | 𐄂            | ✓           | ✓          | 𐄂           |
| some   | `None` | 𐄂            | ✓           | 𐄂          | ✓           |
| some   | some   | ✓            | ✓           | ✓          | ✓           |

You may find this feature helpful when you want to control default values for NULLs:

```python
def half(x: float | None) -> float:
    # zero might be a sensible default
    if x is None:
        return 0.0

    return x / 2.0
```

or if you want turn a value into NULLs:

```python
def add_one_limited(x: int) -> int | None:
    # do not go beyond 100
    if x >= 100:
        return None

    return x + 1
```

## Default Parameters and Kwargs
Default parameters, `*args`, and `**kwargs` are currently NOT supported. So these method will be rejected:

```python
def m1(x: int = 1) -> int:
    return x + 1

def m2(*x: int) -> int:
    return x + 1

def m3(*, x: int) -> int:
    return x + 1

def m4(**x: int) -> int:
    return x + 1
```

## State
We give no guarantees on the lifetime of the Python VM, but you may use state in your Python methods for performance reasons (e.g. to cache results):

```python
_cache = {}

def compute(x: int) -> int:
    try:
        return _cache[x]
    except ValueError:
        y = x * 100
        _cache[x] = y
        return x
```

You may also use a builtin solution like [`functools.cache`]:

```python
from functools import cache

@cache
def compute(x: int) -> int:
    return x * 100
```

## I/O
There is NO I/O available that escapes the sandbox. The [Python Standard Library] is mounted as a read-only filesystem.


[^public]: Modules not starting with a `_`.

[Apache Arrow]: https://arrow.apache.org/
[Apache DataFusion]: https://datafusion.apache.org/
[`bool`]: https://docs.python.org/3/library/stdtypes.html#boolean-type-bool
[`Boolean`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Boolean
[`datetime`]: https://docs.python.org/3/library/datetime.html#datetime.datetime
[`float`]: https://docs.python.org/3/library/stdtypes.html#numeric-types-int-float-complex
[`Float64`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Float64
[`functools.cache`]: https://docs.python.org/3/library/functools.html#functools.cache
[`int`]: https://docs.python.org/3/library/stdtypes.html#numeric-types-int-float-complex
[`Int64`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Int64
[`Microsecond`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.TimeUnit.html#variant.Microsecond
[Python 3.14.0rc2]: https://www.python.org/downloads/release/python-3140rc2/
[Python Standard Library]: https://docs.python.org/3/library/index.html
[Scalar UDF]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html
[`str`]: https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str
[`Timestamp`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Timestamp
[`Utf8`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Utf8
