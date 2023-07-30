import os
import tokenize as std_tokenize

_ASYNC_TO_SYNC = {
    "__aenter__": "__enter__",
    "__aexit__": "__exit__",
    "__aiter__": "__iter__",
    "__anext__": "__next__",
    "asynccontextmanager": "contextmanager",
    "AsyncIterable": "Iterable",
    "AsyncIterator": "Iterator",
    "AsyncGenerator": "Generator",
    "rma": "rms",
    # TODO StopIteration is still accepted in Python 2, but the right change is 'raise
    # StopAsyncIteration' -> 'return' since we want to use unasynced code in Python 3.7+
    "StopAsyncIteration": "StopIteration",
}


def tokenize(f):
    last_end = (1, 0)
    for tok in std_tokenize.tokenize(f.readline):
        if tok.type == std_tokenize.ENCODING:
            continue
        if last_end[0] < tok.start[0]:
            yield ("", std_tokenize.STRING, " \\\n")
            last_end = (tok.start[0], 0)

        space = ""
        if tok.start > last_end:
            assert tok.start[0] == last_end[0]
            space = " " * (tok.start[1] - last_end[1])
        yield (space, tok.type, tok.string)

        last_end = tok.end
        if tok.type in [std_tokenize.NEWLINE, std_tokenize.NL]:
            last_end = (tok.end[0] + 1, 0)


class Unasync:
    def __init__(self, additional_replacements=None):
        self.token_replacements = _ASYNC_TO_SYNC.copy()
        self.token_replacements.update(additional_replacements or {})

    def _unasync_name(self, name):
        if name in self.token_replacements:
            return self.token_replacements[name]
        # Convert classes prefixed with 'Async' into 'Sync'
        elif len(name) > 5 and name.startswith("Async") and name[5].isupper():
            return "Sync" + name[5:]
        elif len(name) > 6 and name.endswith("_async"):
            return name[:-6] + "_sync"
        return name

    def unasync_tokens(self, tokens):
        used_space = None
        for space, toknum, tokval in tokens:
            if tokval in ["async", "await"]:
                # When removing async or await, we want to use the whitespace that was before
                # async/await before the next token so that `print(await stuff)` becomes
                # `print(stuff)` and not `print( stuff)`
                used_space = space
            else:
                if toknum == std_tokenize.NAME:
                    tokval = self._unasync_name(tokval)
                elif toknum == std_tokenize.STRING:
                    left_quote, name, right_quote = tokval[0], tokval[1:-1], tokval[-1]
                    tokval = left_quote + self._unasync_name(name) + right_quote
                if used_space is None:
                    used_space = space
                yield (used_space, tokval)
                used_space = None

    def unasync_content(self, f):
        tokens = tokenize(f)
        return "".join(space + tokval for space, tokval in self.unasync_tokens(tokens))


def main():
    fromdir, todir = "tests", "tests"
    for name in ["method", "generator", "attribute"]:
        filepath = f"test_{name}_async.py"
        with open(os.path.join(fromdir, filepath), "rb") as f:
            result = Unasync(additional_replacements={"_async": "_sync"}).unasync_content(f)
        for value, replacement in [
            ("""proxy.setattr("attribute", new_value)""", "proxy.attribute = new_value"),
            ("\npytestmark = pytest.mark.anyio\n\n", ""),
        ]:
            result = result.replace(value, replacement)
        open(os.path.join(todir, f"test_{name}_sync.py"), "wb").write(result.encode("utf-8"))


if __name__ == "__main__":
    main()
