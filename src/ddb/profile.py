"""Performance profiling support.
"""
from typing import Final, Iterable, Callable, Any
from functools import wraps
import time

from .globals import ANSI
from .util import MinMaxSum

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
        num_next_calls: number of ``next()`` calls on a generator (including the last one signifying the end).
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

    def summarize_block_stats_for_execute(self, stat: ProfileStat) -> tuple[int, int, int]:
        """Given ``stat``, summarize stats about block I/Os incurred by this method and it call graph descendants.
        The components returned, in order, are:
        number of blocks read by this method and its descendants (excluding any ``execute()`` descendants);
        number of blocks written by this method and its descendants (excluding any ``execute()`` descendants);
        overall number of I/Os by this method and its descendants.
        NOTE: We assume there is no recursion in the call graph.
        """
        num_blocks_read = stat.num_blocks_read
        num_blocks_written = stat.num_blocks_written
        desc_blocks = 0
        for stat2 in self.stats:
            if stat2.caller is not None and stat2.caller == stat:
                child_read, child_written, child_overall = self.summarize_block_stats_for_execute(stat2)
                if stat2.method_name.endswith('.execute'):
                    desc_blocks += child_overall
                else: # count toward self:
                    num_blocks_read += child_read
                    num_blocks_written += child_written
                    desc_blocks += child_overall - child_read - child_written
        return num_blocks_read, num_blocks_written, desc_blocks + num_blocks_read + num_blocks_written

    def summarize_stats(self, obj: Any) ->\
        tuple[int, MinMaxSum[int], MinMaxSum[int], MinMaxSum[int], MinMaxSum[int], MinMaxSum[int]]:
        """Given ``obj``, a ``Pop`` object, summarize stats about its ``execute()`` calls.
        The components returned, in order, are:
        number of times ``execute()`` is called;
        min/max/total number of ``next()`` calls over these `execute()`` calls;
        min/max/total elapsed time (in ns) over these calls;
        min/max/total number of blocks read over these `execute()`` calls (excluding descendant ``Pop``s);
        min/max/total number of blocks written over these `execute()`` calls (excluding descendant ``Pop``s);
        min/max/total number of block I/Os over these `execute()`` calls (including descendant ``Pop``s).
        NOTE: We assume there is no recursion in the call graph.
        """
        num_stats = 0
        next_calls = MinMaxSum[int]()
        ns_elapsed = MinMaxSum[int]()
        blocks_read = MinMaxSum[int]()
        blocks_written = MinMaxSum[int]()
        blocks_overall = MinMaxSum[int]()
        for stat in self.stats:
            if (obj is None or stat.oid == id(obj))\
            and stat.method_name.endswith('.execute'):
                num_stats += 1
                next_calls.add(stat.num_next_calls)
                ns_elapsed.add(stat.ns_elapsed)
                num_blocks_read, num_blocks_written, num_blocks_overall = self.summarize_block_stats_for_execute(stat)
                blocks_read.add(num_blocks_read)
                blocks_written.add(num_blocks_written)
                blocks_overall.add(num_blocks_overall)
        return num_stats, next_calls, ns_elapsed, blocks_read, blocks_written, blocks_overall

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

profile_context = None

def new_profile_context() -> ProfileContext:
    """Create a new profile context and set it globally for subsequent execution.
    TODO: This method of getting the profile context through a global variable will NOT work when we have concurrent transactions.
    At the very least we might consider using a thread-global object.
    The alternative of passing in the context in every call would complicate the API too much.
    """
    global profile_context
    profile_context = ProfileContext()
    return profile_context

def get_profile_context() -> ProfileContext | None:
    """Locate the appropriate profile context object for current execution.
    TODO: This method of getting the profile context through a global variable will NOT work when we have concurrent transactions.
    The alternative of passing in the context in every call would complicate the API too much.
    """
    global profile_context
    return profile_context
    # """Here is an alternative method by by inspecting the current call stack.
    # Specifically, look for the lowest ancestor caller with ``self`` and
    # whose ``self.context.profile_context`` is a :class:`.ProfileContext`
    # (i.e., it comes from a ``Pop``'s ``StatementContext``).
    # NOTE: This is a very Pythonic hack, to some extent to get around circular imports.
    # This is a bit better than the global variable hack, but ispecting the stack adds too much runtime overhead.
    # """
    # current_stack = stack()
    # for frame_info in current_stack[::-1]:
    #     if (obj := frame_info.frame.f_locals.get('self', None)) is not None\
    #         and (context := getattr(obj, 'context', None)) is not None\
    #         and (profile_context := getattr(context, 'profile_context', None)) is not None\
    #         and isinstance(profile_context, ProfileContext):
    #         return profile_context
    # return None

def profile(stat_cls: type[ProfileStat] = ProfileStat):
    """Decorate a member method of some class to enable collecting statistics on its invocations.
    The argument ``stat_cls`` specifies a class whose objects are used to collect statistics.
    """
    def _profile(method):
        @wraps(method)
        def wrap(self, *args, **kw):
            profile_context = get_profile_context()
            if profile_context is not None:
                stat = profile_context.call_begin(stat_cls, method, self, *args, **kw)
            result = method(self, *args, **kw)
            if profile_context is not None:
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
            profile_context = get_profile_context()
            if profile_context is not None:
                stat = profile_context.gen_construct_begin(stat_cls, generator_method, self, *args, **kw)
            try:
                # construct the generator object:
                it = generator_method(self, *args, **kw)
                if profile_context is not None:
                    profile_context.gen_construct_end(stat, it)
                # start iterations:
                while True:
                    value = None
                    try:
                        if profile_context is not None:
                            profile_context.gen_next_begin(stat)
                        value = next(it)
                        if profile_context is not None:
                            profile_context.gen_next_end(stat, value)
                    except StopIteration: # catch natural termination of the wrapped generator
                        if profile_context is not None:
                            profile_context.gen_next_end(stat, None)
                        break
                    yield value
            finally: # catch the case that caller may stop early and call close()
                if profile_context is not None:
                    profile_context.gen_close_begin(stat)
                it.close() # close the wrapped generator too
                if profile_context is not None:
                    profile_context.gen_close_end(stat)
            return
        return wrap
    return _profile_generator
