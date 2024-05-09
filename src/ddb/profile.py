"""Performance profiling support.
"""
from typing import Final, Iterable, Callable, Any
from functools import wraps
import time

from .globals import ANSI

class ProfileException(Exception):
    """Exceptions thrown by functions in the :mod:`.profile` module.
    """
    pass

class ProfileStat:
    """A "collector" that collects and compute profiling statistics for a call or a generator.
    The collector's behavior can be further customized by defining a subclass.

    Note that for a generator,
    total times / numbers below refer to the total across all uses of this generator,
    including its opening, iterations, and closing (pauses between these events are not added to the total times).

    Attributes:
        oid: integer id of the object from which method is dispatched.
        method_name: qualified name of the method (including class).
        module_name: module name (useful to differentiate identically named class.method from different modules).
        caller: a reference to caller's ``ProfileStat``, or ``None`` if that's not tracked.
        ts: time when call started (``time.monotonic_ns()``).
        ns_thread: total thread time (in ns) spent, including system and user CPU time but not time elapsed during sleep.
        ns_elapsed: total elapsed time (in ns).
        num_next_calss: number of ``next()`` calls on a generator (including the last one signifying the end).
        num_blocks_read: number of disk blocks read.
        num_blocks_written: number of disk block written.
    """

    def __init__(self, method: Callable, obj: Any, caller: 'ProfileStat | None',
                 *call_args, **call_kw) -> None:
        """Initialize the "collector" for a ``method`` call on ``obj`` from ``caller`` (if any),
        and start the timer.  The actual call should follow immediately,
        then by :meth:`.finalize()` (for a regular method call) or
        by :meth:`.stop()` (for a generator).
        """
        self.oid: Final[int] = id(obj)
        self.method_name: Final[str] = method.__qualname__
        self.module_name: Final[str] = method.__module__
        self.caller: Final = caller
        self.ts: Final = time.monotonic_ns()
        self.ns_thread: int = 0
        self.ns_elapsed: int = 0
        self.num_next_calls: int = 0 # only applicable to generators
        self.num_blocks_read: int = 0
        self.num_blocks_written: int = 0
        self.start()
        return

    def start(self) -> None:
        """Start or restart (not reset) the timer.
        """
        self._ts = time.monotonic_ns()
        self._ts_thread = time.thread_time_ns()
        return

    def stop(self) -> None:
        """Stop or pause (not reset) the timer.
        """
        self.ns_thread += time.thread_time_ns() - self._ts_thread
        self.ns_elapsed += time.monotonic_ns() - self._ts
        return

    def next_start(self) -> None:
        """For interator only: register a ``next()`` call and restart the timer.
        The actual ``next()`` call on the generator should follow immediately,
        and then by this object's :meth:`.next_stop()`.
        """
        self.start()
        self.num_next_calls += 1
        return

    def next_stop(self, result: Any) -> None:
        """For interator only: stop the timer after completing a ``next()`` call and restarts the timer.
        The actual ``next()`` call should follow immediately.
        """
        self.stop()
        return

    def finalize(self, result: Any) -> None:
        """Mark the finish of the call or closing of a generator and stop the timer.
        In the case of a generator, ``close()`` should have been called explicitly
        and the timer should have been restarted right before that.
        """
        self.stop()
        return

class ProfileContext:
    """A context for hold profiling information during execution of a complex call graph.
    Currently we support only invocation of member methods.

    Attributes:

        call_stack: current call stack (each element is a :class:`.ProfileStat`).
        stats: a list of all :class:`.ProfileStat` objects, in the order of creation time.
    """

    def __init__(self) -> None:
        """Initalize the context.
        """
        self.call_stack: Final[list[ProfileStat]] = list()
        self.stats: Final[list[ProfileStat]] = list()
        return

    def call_begin(self, stat_cls: type[ProfileStat], method: Callable, obj: Any, *call_args, **call_kw) -> ProfileStat:
        """Mark the beginning of an invocation of ``method`` on ``obj`` by ``caller`` (if any),
        construct (and return) an object of the given ``stat_cls`` to track statistics on this invocation,
        and register it in the member attributes ``stats`` and ``call_stack``.
        """
        caller = None if len(self.call_stack) == 0 else self.call_stack[-1]
        stat = stat_cls(method, obj, caller, *call_args, **call_kw)
        self.call_stack.append(stat)
        self.stats.append(stat)
        return stat

    def call_end(self, stat: ProfileStat, result: Any) -> None:
        """Mark the end of the current invocation and the collection of its statistics."""
        if self.call_stack[-1] != stat:
            raise ProfileException('call stack integrity error')
        self.call_stack[-1].finalize(result)
        self.call_stack.pop()
        return

    def gen_construct_begin(self, stat_cls: type[ProfileStat], method: Callable, obj: Any, *call_args, **call_kw) -> ProfileStat:
        """Same as :meth:`.call_begin()`, but mark the beginning of the generator construction call.
        """
        return self.call_begin(stat_cls, method, obj, *call_args, **call_kw)
    
    def gen_construct_end(self, stat: ProfileStat, result: Any) -> None:
        """Same as :meth:`.call_end()`, but mark the end of the generator construction call.
        """
        if self.call_stack[-1] != stat:
            raise ProfileException('call stack integrity error')
        # do not finalize yet, but stop the timer nonetheless:
        stat.stop()
        self.call_stack.pop()
        return

    def gen_next_begin(self, stat: ProfileStat) -> None:
        """Same as :meth:`.call_begin()`, but mark the beginning of a generator ``next()`` call.
        """
        self.call_stack.append(stat)
        stat.next_start()
        return

    def gen_next_end(self, stat: ProfileStat, result: Any) -> None:
        """Same as :meth:`.call_end()`, but mark the end of a generator ``next()`` call.
        """
        if self.call_stack[-1] != stat:
            raise ProfileException('call stack integrity error')
        stat.next_stop(result)
        self.call_stack.pop()
        return

    def gen_close_begin(self, stat: ProfileStat) -> None:
        """Same as :meth:`.call_begin()`, but mark the beginning of a generator ``close()`` call.
        """
        self.call_stack.append(stat)
        stat.start()
        return

    def gen_close_end(self, stat: ProfileStat) -> None:
        """Same as :meth:`.call_end()`, but mark the end of a generator ``close()`` call.
        """
        # the following will finalize:
        self.call_end(stat, None)
        return

    def pstr_stats(self, caller: ProfileStat | None = None, indent: int = 0) -> Iterable[str]:
        """Produce a sequence of lines, "pretty-print" style, for summarizing the collected stats.
        """
        for stat in sorted(filter(lambda s: s.caller == caller, self.stats), key = lambda s: s.ts):
            prefix = '' if indent == 0 else '    ' * (indent-1) + '\\___'
            class_name, method_name = stat.method_name.rsplit('.', 1)
            yield f'{prefix}{ANSI.EMPH}{class_name}{ANSI.END}[{hex(stat.oid)}].{method_name}'
            prefix = '    ' * indent + '| '
            s = f'{stat.num_next_calls} next() calls; ' if stat.num_next_calls != 0 else ''
            yield f'{prefix}{s}elapsed: {stat.ns_elapsed/1000000}ms; thread: {stat.ns_thread/1000000}ms'
            if stat.num_blocks_read + stat.num_blocks_written > 0:
                yield f'{prefix}{stat.num_blocks_read} block reads; {stat.num_blocks_written} block writes'
            yield from self.pstr_stats(stat, indent+1)
        return

profile_context = ProfileContext()

def new_profile_context() -> ProfileContext:
    """Set a new profile context and return it.
    From this point on, all profiling information will be held in this new context.
    Any existing profile context will be discarded.

    TODO: This method of setting profile context globally will NOT work when we have concurrent transactions.
    At the very least we might consider using a thread-global object.
    """
    global profile_context
    profile_context = ProfileContext()
    return profile_context

def profile(stat_cls: type[ProfileStat] = ProfileStat):
    """Decorate a member method of some class to enable collecting statistics on its invocations.
    The argument ``stat_cls`` specifies a class whose objects are used to collect statistics.
    """
    def _profile(method):
        @wraps(method)
        def wrap(self, *args, **kw):
            stat = profile_context.call_begin(stat_cls, method, self, *args, **kw)
            result = method(self, *args, **kw)
            profile_context.call_end(stat, result)
            return result
        return wrap
    return _profile

def profile_generator(stat_cls: type[ProfileStat] = ProfileStat):
    """Decorate a generator member method of some class to enable collecting statistics on its invocations.
    The argument ``stat_cls`` specifies a class whose objects are used to collect statistics.
    """
    def _profile_generator(generator_method: type):
        @wraps(generator_method)
        def wrap(self, *args, **kw):
            stat = profile_context.gen_construct_begin(stat_cls, generator_method, self, *args, **kw)
            try:
                # contruct the generator object:
                it = generator_method(self, *args, **kw)
                profile_context.gen_construct_end(stat, it)
                # start iterations:
                while True:
                    value = None
                    try:
                        profile_context.gen_next_begin(stat)
                        value = next(it)
                        profile_context.gen_next_end(stat, value)
                    except StopIteration: # catch natural termination of the wrapped generator
                        profile_context.gen_next_end(stat, None)
                        break
                    yield value
            finally: # catch the case that caller may stop early and call close()
                profile_context.gen_close_begin(stat)
                it.close() # close the wrapped generator too
                profile_context.gen_close_end(stat)
            return
        return wrap
    return _profile_generator