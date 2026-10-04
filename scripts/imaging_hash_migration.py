"""Mechanical patch helper for the approved imaging ownership migration.

All source changes are applied through apply_patch, never direct file writes.
"""
from pathlib import Path
import difflib
import re
import subprocess

changes = {}

def edit(path, transform):
    old = changes.get(path, (None, None))[1]
    if old is None:
        old = Path(path).read_text()
        original = old
    else:
        original = changes[path][0]
    new = transform(old)
    if original != new:
        changes[path] = (original, new)

def cut(text, first, last):
    start = text.index(first)
    end = text.index(last, start)
    return text[:start] + text[end:]

def remove_function(text, name):
    match = re.search(r"(?m)^([ \t]*)(?:pub(?:\([^)]*\))? )?(?:const )?fn " + re.escape(name) + r"\b", text)
    if not match:
        return text
    start = match.start()
    # Include doc comments and attributes belonging to the removed function.
    while start > 0:
        prior = text.rfind('\n', 0, start - 1) + 1
        line = text[prior:start].strip()
        if line.startswith('///') or line.startswith('#['):
            start = prior
        else:
            break
    body = text.index('{', match.end())
    depth = 1
    end = body + 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    if end < len(text) and text[end] == '\n':
        end += 1
    return text[:start] + text[end:]

def remove_field(text, name):
    """Remove a Rust named field and its possibly multiline initializer/type."""
    pattern = re.compile(r'(?m)^[ \t]*(?:pub(?:\([^)]*\))? )?' + re.escape(name) + r': ')
    while match := pattern.search(text):
        end = match.end()
        depth = 0
        quoted = False
        escaped = False
        while end < len(text):
            char = text[end]
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char in '([{':
                depth += 1
            elif char in ')]}':
                depth -= 1
            elif char == ',' and depth == 0:
                end += 1
                break
            end += 1
        if end < len(text) and text[end] == '\n':
            end += 1
        text = text[:match.start()] + text[end:]
    return text

def remove_braced(text, pattern):
    """Remove a specifically matched block; use only for inspected code blocks."""
    while match := re.search(pattern, text, re.M):
        start = match.start()
        body = text.index('{', match.end() - 1)
        depth, end = 1, body + 1
        while depth:
            depth += (text[end] == '{') - (text[end] == '}')
            end += 1
        if end < len(text) and text[end] == '\n':
            end += 1
        text = text[:start] + text[end:]
    return text

def apply():
    patch = '*** Begin Patch\n'
    for path, (old, new) in changes.items():
        diff = list(difflib.unified_diff(old.splitlines(True), new.splitlines(True), n=3))
        chunks = re.sub(r'(?m)^@@.*@@.*$', '@@', ''.join(diff[2:]))
        patch += '*** Update File: ' + path + '\n' + chunks
    patch += '*** End Patch\n'
    result = subprocess.run(['apply_patch'], input=patch, text=True, capture_output=True)
    print(result.stdout, result.stderr)
    result.check_returncode()

def map_call_args(text, pattern, transform):
    """Rewrite arguments at explicitly selected call sites, respecting nesting."""
    for match in reversed(list(re.finditer(pattern, text))):
        start = match.end()
        stack, quoted, escaped = [], False, False
        args, prior, end = [], start, start
        while end < len(text):
            char = text[end]
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char in '([{':
                stack.append(char)
            elif char in ')]}':
                if char == ')' and not stack:
                    tail = text[prior:end].strip()
                    if tail:
                        args.append(tail)
                    break
                stack.pop()
            elif char == ',' and not stack:
                args.append(text[prior:end].strip())
                prior = end + 1
            end += 1
        new = transform(args)
        if new is None:
            end += 1
            if text[end:end+1] == ';':
                end += 1
            if text[end:end+1] == '\n':
                end += 1
            text = text[:match.start()] + text[end:]
        elif new != args:
            text = text[:start] + ', '.join(new) + text[end:]
    return text
