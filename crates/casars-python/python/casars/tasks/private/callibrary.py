# SPDX-License-Identifier: LGPL-3.0-or-later
"""Build and exchange calibration libraries with CASA's Python helper API."""

import ast


_SELECTIONS = ('field', 'intent', 'spw', 'obs')
_INTERPOLATION = ('tinterp', 'finterp', 'reach')
_MAPPINGS = ('obsmap', 'fldmap', 'spwmap', 'antmap')


def _per_table(value):
    return value if isinstance(value, list) else [value]


def _per_table_mapping(value):
    if len(value) and not isinstance(value[0], list):
        return [value]
    return value


def _quoted(name, value):
    return f"{name}='{value}'"


def _literal_value(node):
    try:
        return ast.literal_eval(node)
    except ValueError:
        for item in ast.walk(node):
            if isinstance(item, ast.Name):
                raise NameError(f"name '{item.id}' is not defined") from None
        raise ValueError('calibration library values must be Python literals') from None


def _record_literal(line):
    # The historical helper preprocesses whitespace before parsing the line.
    text = ' '.join(line.split()).replace(' =', '=').replace('= ', '=')
    text = text.replace(' ', ',').replace(',,', ',')
    expression = 'record(' + text + ')'
    for short, value in [('T', 'True'), ('F', 'False')]:
        expression = expression.replace(f'calwt={short},', f'calwt={value},')
        expression = expression.replace(f'calwt={short})', f'calwt={value})')
    node = ast.parse(expression, filename='<string>', mode='eval').body
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != 'record':
        raise SyntaxError('invalid calibration library expression')
    args = [_literal_value(argument) for argument in node.args]
    fields = {}
    for argument in node.keywords:
        if argument.arg is None:
            raise SyntaxError('expected a named calibration library field')
        if argument.arg in fields:
            raise SyntaxError(f'keyword argument repeated: {argument.arg}',
                              ('<string>', 1, argument.col_offset + 1, expression))
        fields[argument.arg] = _literal_value(argument.value)
    return args, fields


class callibrary:
    """Maintain the live dictionary of calibration tables and their selections."""

    def __init__(self):
        self.cld = {}

    def __repr__(self):
        return '<callibrary>'

    def cldefinstance(self):
        """Return defaults for a single application of a calibration table."""
        return {
            **dict.fromkeys(_SELECTIONS, ''),
            **{name: [] for name in ('fldmap', 'obsmap', 'spwmap', 'antmap')},
            **dict.fromkeys(_INTERPOLATION, ''),
        }

    def clear(self):
        self.__init__()

    def len(self):
        return len(self.cld)

    def addold(self, field='', spw='', intent='', gaintable='', gainfield='',
               interp='', spwmap=[], calwt=False):
        """Translate task-style apply arguments to calibration-library entries."""
        if len(gaintable) < 1:
            raise Exception('Please specify at least a gaintable.')
        fields = _per_table(gainfield)
        interpolations = _per_table(interp)
        weights = _per_table(calwt)
        mappings = _per_table_mapping(spwmap) if isinstance(spwmap, list) else []
        for index, table in enumerate(_per_table(gaintable)):
            tokens = interpolations[index].split(',') if index < len(interpolations) else []
            time, frequency = 'linear', ''
            if tokens and tokens[0]:
                time = tokens[0]
                frequency = tokens[1] if len(tokens) > 1 else ''
            self.add(
                table, field=field, spw=spw, intent=intent, tinterp=time,
                finterp=frequency, calwt=weights[min(index, len(weights) - 1)],
                fldmap=fields[index] if index < len(fields) else '',
                spwmap=mappings[index] if index < len(mappings) else [],
            )

    def add(self, caltable, field='', intent='', spw='', obs='', tinterp='',
            finterp='', reach='', calwt=True, obsmap=[], fldmap=[], spwmap=[], antmap=[]):
        """Add a slice to a table, or add separate slices to a list of tables."""
        fields = dict(zip(_SELECTIONS + _INTERPOLATION,
                          (field, intent, spw, obs, tinterp, finterp, reach), strict=True))
        mappings = dict(zip(_MAPPINGS, (obsmap, fldmap, spwmap, antmap), strict=True))
        if not isinstance(caltable, list):
            self.parsetorec(caltable, calwt=calwt, **fields, **mappings)
            return
        fields = {key: _per_table(value) for key, value in fields.items()}
        mappings = {key: _per_table_mapping(value) for key, value in mappings.items()}
        weights = _per_table(calwt)
        for index, table in enumerate(caltable):
            selection = {key: value[index] if index < len(value) else ''
                         for key, value in fields.items()}
            remap = {key: value[index] if index < len(value) else []
                     for key, value in mappings.items()}
            self.parsetorec(table, calwt=weights[min(index, len(weights) - 1)],
                            **selection, **remap)

    def parsetorec(self, caltable, field='', intent='', spw='', obs='', tinterp='linear',
                   finterp='', reach='', calwt=True, obsmap=[], fldmap=[], spwmap=[], antmap=[]):
        """Add a normalized slice record, preserving its supplied values."""
        entry = self.cldefinstance()
        entry.update(field=field, intent=intent, spw=spw, obs=obs, tinterp=tinterp,
                     finterp=finterp, reach=reach, obsmap=obsmap, fldmap=fldmap,
                     spwmap=spwmap, antmap=antmap)
        self.addrec({caltable: entry}, calwt)

    def addrec(self, crec, calwt):
        """Keep the supplied record live and append it to its first table."""
        name = list(crec.keys())[0]
        if name in self.cld:
            table = self.cld[name]
            initial = table['calwt']
            if calwt != initial:
                print("WARNING: For caltable='" + name + f"' using already-set calwt={initial}.")
        else:
            table = {'calwt': calwt}
            self.cld[name] = table
        table[str(len(table) - 1)] = crec[name]

    def list(self):
        print(f'There are {len(self.cld)} caltables in the cal library:')
        for name in sorted(self.cld):
            table = self.cld[name]
            prefix = name + ':'
            print(prefix + f" calwt={table['calwt']} ({len(table) - 1} instance[s]):")
            for entry in table.values():
                if isinstance(entry, dict):
                    for key in _SELECTIONS:
                        print(' ' + _quoted(key, entry[key]), end='\n' if key == 'obs' else ' ')
                    print('  ' + _quoted('tinterp', entry['tinterp']), end=' ')
                    print(' ' + _quoted('finterp', entry['finterp']))
                    for key in _MAPPINGS:
                        prefix = '  ' if key == 'obsmap' else ' '
                        print(prefix + f'{key}={entry[key]}', end='\n' if key == 'antmap' else ' ')

    def write(self, filename, append=False):
        """Write slices in CASA's lexical order with its legacy whitespace."""
        if len(filename) < 1:
            raise Exception('Please specify a filename')
        if len(self.cld) < 1:
            raise Exception('There is no cal library to write')
        with open(filename, 'a' if append else 'w') as stream:
            for name in sorted(self.cld):
                table = self.cld[name]
                for key in sorted(table):
                    entry = table[key]
                    if not isinstance(entry, dict):
                        continue
                    print("caltable='" + name + "'", end=' ', file=stream)
                    print(f"calwt={table['calwt']}", end=' ', file=stream)
                    for key in _SELECTIONS + _INTERPOLATION + _MAPPINGS:
                        value = entry[key]
                        if len(value) == 0:
                            continue
                        quoted = key not in _MAPPINGS or (key == 'fldmap' and isinstance(value, str))
                        text = _quoted(key, value) if quoted else f'{key}={value}'
                        print(text, end=' ', file=stream)
                    print(file=stream)

    def read(self, callibr):
        """Read a list of record lines or a calibration-library file."""
        if isinstance(callibr, list):
            self._read_lines(callibr)
        else:
            with open(callibr) as stream:
                self._read_lines(stream)

    def _read_lines(self, lines):
        for line in lines:
            content = line.strip()
            if not content:
                continue
            if content.startswith('#'):
                print('Found comment (not parsed): ', content)
                continue
            try:
                args, fields = _record_literal(content)
                self.parsetorec(*args, **fields)
            except Exception as error:
                self.clear()
                print('Error: ', error)
                print(f'Problem parsing cal library line (check for typos): "{line}"')
                raise Exception('Problem parsing cal library line (check for typos): ' + line) from None

    def compare(self, other):
        return self.cld == other.cld


def applycaltocallib(filename, append=False, field='', spw='', intent='', gaintable='',
                     gainfield='', interp='', spwmap=[], calwt=True):
    """Write task-style apply arguments as a calibration-library file."""
    if len(filename) < 1:
        raise Exception('Please specify a filename')
    if len(gaintable) < 1:
        raise Exception('No caltable specified in gaintable')
    library = callibrary()
    library.addold(field, spw, intent, gaintable, gainfield, interp, spwmap, calwt)
    library.write(filename, append)
