# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# **************************************************************************

import glob
import os
import re
import ast


class JobValidationError(Exception):
    """Raised when job parameter values fail form validation."""

    def __init__(self, errors):
        if isinstance(errors, str):
            if '\n' in errors:
                errors = [line.strip() for line in errors.splitlines() if line.strip()]
            else:
                errors = [errors]
        else:
            errors = list(errors)
        self.errors = [str(e).strip() for e in errors if str(e).strip()]
        super().__init__('\n'.join(self.errors))


def _coerce_token(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if text.lower() == 'true':
        return True
    if text.lower() == 'false':
        return False
    return text


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ''
    if isinstance(value, (list, tuple)):
        return len(value) == 0 or all(_is_empty(v) for v in value)
    return False


def _get_param_value(param_def, params):
    name = param_def.get('name')
    if not name:
        return None
    if name in params:
        return params[name]
    if 'default' in param_def:
        return param_def.get('default')
    return ''


def _eval_atom(params, atom):
    atom = atom.replace('(', '').replace(')', '').strip()
    neg = False
    if re.match(r'^not\s+', atom, flags=re.I):
        neg = True
        atom = re.sub(r'^not\s+', '', atom, flags=re.I).strip()
    elif atom.startswith('!'):
        neg = True
        atom = atom[1:].strip()

    match = re.match(r'^(.*?)\s*(==|!=|>=|<=|>|<|=)\s*(.*)$', atom)
    if match:
        left_raw, op_raw, right_raw = match.groups()
        left = _coerce_token(params.get(left_raw.strip(), ''))
        op = '==' if op_raw == '=' else op_raw
        right = _coerce_token(right_raw.replace('(', '').replace(')', '').strip().strip("'\""))

        if op == '==':
            result = left == right
        elif op == '!=':
            result = left != right
        elif op == '>':
            result = left > right
        elif op == '<':
            result = left < right
        elif op == '>=':
            result = left >= right
        elif op == '<=':
            result = left <= right
        else:
            result = False
    else:
        value = _coerce_token(params.get(atom, ''))
        result = bool(value is True or value == 'True' or value == 1 or value == '1')

    return not result if neg else result


def _normalize_condition(condition):
    if condition is None:
        return None
    expr = str(condition).strip()
    if not expr or expr.lower() == 'null':
        return None
    return expr


def _eval_condition(params, condition):
    """Evaluate a form condition expression against flat param values."""
    condition = _normalize_condition(condition)
    if condition is None:
        return True
    expr = condition
    if not expr:
        return True

    expr = (expr.replace('(', ' ')
            .replace(')', ' ')
            .replace(' and ', ' && ')
            .replace(' AND ', ' && ')
            .replace(' or ', ' || ')
            .replace(' OR ', ' || '))
    expr = re.sub(r'\s+', ' ', expr).strip()
    if not expr:
        return True

    for or_part in expr.split('||'):
        and_parts = [part.strip() for part in or_part.split('&&') if part.strip()]
        if and_parts and all(_eval_atom(params, part) for part in and_parts):
            return True
    return False


def _resolve_path(path, project_path):
    if not isinstance(path, str):
        return path
    path = path.strip()
    if not path:
        return path
    if os.path.isabs(path):
        return path
    if project_path:
        return os.path.join(project_path, path)
    return path


def _param_class(param_def):
    return param_def.get('paramClass') or 'StringParam'


def _normalize_scalar_param_class(param_class):
    if param_class == 'BoolParam':
        return 'BooleanParam'
    return param_class


def _table_columns(param_def):
    columns = []
    for col in param_def.get('params') or []:
        name = col.get('name')
        if not name:
            continue
        columns.append({
            'name': name,
            'label': col.get('label') or name,
            'paramClass': _normalize_scalar_param_class(_param_class(col)),
            'default': col.get('default', ''),
        })
    return columns


def _is_table_row_empty(row, columns):
    if not isinstance(row, dict):
        return True
    for col in columns:
        value = row.get(col['name'], '')
        if value is None:
            continue
        text = str(value).strip()
        if text == '':
            continue
        default = col.get('default', '')
        if default is not None and str(value) == str(default):
            continue
        return False
    return True


def _quote_star_json_string(value):
    """Serialize a string using single quotes, without raw double quotes."""
    text = str(value)
    text = (text.replace('\\', '\\\\')
                .replace("'", "\\'")
                .replace('"', '\\u0022'))
    return f"'{text}'"


def _serialize_star_json(value):
    """Serialize list/dict params using single quotes (STAR-safe, no \")."""
    if isinstance(value, list):
        return '[' + ','.join(_serialize_star_json(v) for v in value) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(
            f"{_quote_star_json_string(k)}:{_serialize_star_json(v)}"
            for k, v in value.items()) + '}'
    if isinstance(value, bool):
        return repr(value)
    if value is None:
        return 'None'
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return _quote_star_json_string(value)


def _encode_json_param_value(value):
    if value is None:
        return '[]'
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return '[]'
        return text
    return _serialize_star_json(value)


def _parse_star_json_literal(text):
    """Parse single-quote encoded params stored in job.star."""
    return ast.literal_eval(text)


def _parse_json_param_value(raw_value, label='JSON value', expect_list=True):
    if raw_value is None:
        return [] if expect_list else None
    if isinstance(raw_value, list):
        return raw_value
    if not isinstance(raw_value, str):
        raise ValueError(f'{label} must be an encoded string or list.')

    text = raw_value.strip()
    if not text:
        return [] if expect_list else None

    try:
        parsed = _parse_star_json_literal(text)
    except (SyntaxError, ValueError) as exc:
        raise ValueError(f'{label} is not valid encoded data ({exc}).') from exc

    if expect_list and not isinstance(parsed, list):
        raise ValueError(f'{label} must be an encoded array.')
    return parsed


def _parse_table_param_value(raw_value):
    return _parse_json_param_value(raw_value, 'Table value', expect_list=True)


def _parse_multi_pointer_param_value(raw_value):
    parsed = _parse_json_param_value(
        raw_value, 'Multi-pointer value', expect_list=True)
    return [str(v).strip() for v in parsed if str(v).strip()]


def _validate_table_cell(value, column):
    param_class = column['paramClass']
    text = '' if value is None else str(value).strip()
    if not text:
        return None

    if param_class == 'IntParam':
        if not _is_valid_int(value):
            return f"invalid integer value '{value}'"
        return None

    if param_class == 'FloatParam':
        if not _is_valid_float(value):
            return f"invalid float value '{value}'"
        return None

    return None


def _allows_empty(param_def, param_class):
    if 'allowsEmpty' in param_def:
        return bool(param_def['allowsEmpty'])
    return param_class == 'StringParam'


def _is_valid_int(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    text = str(value).strip()
    return bool(text) and re.fullmatch(r'[-+]?\d+', text) is not None


def _container_visible(container_def, params, parent_visible=True):
    """Return whether a form container and its descendants should be active."""
    if not parent_visible:
        return False
    if container_def.get('paramClass') not in ('Group', 'Line'):
        return parent_visible
    condition = _normalize_condition(container_def.get('condition'))
    if condition is None:
        return parent_visible
    return _eval_condition(params, condition)


def _is_valid_float(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    text = str(value).strip()
    if not text:
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


class JobForm:
    """Utilities for job form definitions (JSON) and parameter values."""

    @staticmethod
    def iter_params(job_form):
        """Iterate over all params in sections, groups, and lines."""
        def _iter_params(container_def):
            if _param_class(container_def) == 'TableParam':
                yield container_def
                return

            if params := container_def.get('params', None):
                for p in params:
                    yield from _iter_params(p)
            else:
                yield container_def

        for section_def in job_form['sections']:
            yield from _iter_params(section_def)

    @staticmethod
    def decode_multi_pointer_params(job_form, params):
        """Decode MultiPointerParam JSON strings into lists of paths."""
        if not job_form or not params:
            return params

        decoded = dict(params)
        for param_def in JobForm.iter_params(job_form):
            if _param_class(param_def) != 'MultiPointerParam':
                continue
            name = param_def.get('name')
            if not name or name not in decoded:
                continue
            value = decoded[name]
            if isinstance(value, list):
                continue
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    decoded[name] = []
                    continue
                try:
                    decoded[name] = _parse_multi_pointer_param_value(value)
                except ValueError:
                    decoded[name] = []
            elif value is None:
                decoded[name] = []
        return decoded

    @staticmethod
    def encode_multi_pointer_params(job_form, params):
        """Encode MultiPointerParam lists as JSON strings for job.star storage."""
        if not job_form or not params:
            return params

        encoded = dict(params)
        for param_def in JobForm.iter_params(job_form):
            if _param_class(param_def) != 'MultiPointerParam':
                continue
            name = param_def.get('name')
            if not name or name not in encoded:
                continue
            value = encoded[name]
            if isinstance(value, list):
                encoded[name] = _encode_json_param_value(value)
            elif value is None:
                encoded[name] = '[]'
            elif isinstance(value, str) and value.strip():
                encoded[name] = value.strip()
        return encoded

    @staticmethod
    def decode_table_params(job_form, params):
        """Decode TableParam JSON strings into lists of row dicts."""
        if not job_form or not params:
            return params

        decoded = dict(params)
        for param_def in JobForm.iter_params(job_form):
            if _param_class(param_def) != 'TableParam':
                continue
            name = param_def.get('name')
            if not name or name not in decoded:
                continue
            value = decoded[name]
            if isinstance(value, list):
                continue
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    decoded[name] = []
                    continue
                try:
                    decoded[name] = _parse_table_param_value(value)
                except ValueError:
                    continue
            elif value is None:
                decoded[name] = []
        return decoded

    @staticmethod
    def encode_table_params(job_form, params):
        """Encode TableParam lists as JSON strings for job.star storage."""
        if not job_form or not params:
            return params

        encoded = dict(params)
        for param_def in JobForm.iter_params(job_form):
            if _param_class(param_def) != 'TableParam':
                continue
            name = param_def.get('name')
            if not name or name not in encoded:
                continue
            value = encoded[name]
            if isinstance(value, list):
                encoded[name] = _encode_json_param_value(value)
            elif value is None:
                encoded[name] = '[]'
            elif isinstance(value, str) and value.strip():
                encoded[name] = value.strip()
        return encoded

    @staticmethod
    def get_values(job_form, all=False):
        """Return a dict of param names to default values from the form."""
        values = {}
        for param_def in JobForm.iter_params(job_form):
            v = param_def.get('default', None)
            name = param_def.get('name', None)
            if name and (v or all):
                values[name] = v
        return values

    @staticmethod
    def validate_params(job_form, params, project_path=None, skip_path_exists=None):
        """Validate job params against a form definition.

        Returns a list of error messages. An empty list means validation passed.

        Args:
            skip_path_exists: optional callable(value) -> bool; when it returns True
                for a path value, existence on disk is not checked.
        """
        if not job_form:
            return []

        merged = {}
        for param_def in JobForm.iter_params(job_form):
            name = param_def.get('name')
            if name:
                merged[name] = _get_param_value(param_def, params)

        merged.update(params or {})
        errors = []

        def _validate_def(container_def, parent_visible=True):
            visible = _container_visible(container_def, merged, parent_visible)
            param_class = _param_class(container_def)

            if param_class == 'TableParam':
                if not JobForm._should_validate_param(container_def, merged, visible):
                    return

                name = container_def['name']
                label = container_def.get('label') or name
                columns = _table_columns(container_def)
                raw_value = merged.get(name, '[]')

                try:
                    rows = _parse_table_param_value(raw_value)
                except ValueError as exc:
                    errors.append(f"{label}: invalid table value ({exc}).")
                    return

                for row_index, row in enumerate(rows):
                    if _is_table_row_empty(row, columns):
                        continue
                    if not isinstance(row, dict):
                        errors.append(
                            f"{label}: row {row_index + 1} must be an object.")
                        continue
                    for column in columns:
                        cell_error = _validate_table_cell(
                            row.get(column['name'], ''), column)
                        if cell_error:
                            col_label = column.get('label') or column['name']
                            errors.append(
                                f"{label}, row {row_index + 1}, {col_label}: {cell_error}.")
                return

            if param_class == 'MultiPointerParam':
                if not JobForm._should_validate_param(container_def, merged, visible):
                    return

                name = container_def['name']
                label = container_def.get('label') or name
                min_items = container_def.get('min')
                try:
                    items = _parse_multi_pointer_param_value(merged.get(name, '[]'))
                except ValueError as exc:
                    errors.append(f"{label}: invalid multi-pointer value ({exc}).")
                    return

                if min_items is not None and len(items) < int(min_items):
                    errors.append(
                        f"{label}: at least {int(min_items)} input(s) are required.")
                    return

                for index, item in enumerate(items):
                    if _is_empty(item):
                        errors.append(f"{label}: input {index + 1} is empty.")
                        continue
                    if skip_path_exists and skip_path_exists(item):
                        continue
                    path = _resolve_path(str(item), project_path)
                    if not os.path.exists(path):
                        errors.append(
                            f"{label}: file or directory '{item}' does not exist.")
                return

            if container_def.get('params'):
                for child in container_def['params']:
                    _validate_def(child, visible)
                return

            if not JobForm._should_validate_param(container_def, merged, visible):
                return

            param_class = _param_class(container_def)
            if param_class not in (
                    'PathParam', 'FilesPatternParam',
                    'IntParam', 'FloatParam', 'StringParam'):
                return

            name = container_def['name']
            label = container_def.get('label') or name
            allows_empty = _allows_empty(container_def, param_class)
            value = merged.get(name, '')

            if param_class == 'PathParam':
                if _is_empty(value):
                    if not allows_empty:
                        errors.append(f"{label} is required.")
                elif skip_path_exists and skip_path_exists(value):
                    pass
                else:
                    path = _resolve_path(str(value), project_path)
                    if not os.path.exists(path):
                        errors.append(
                            f"{label}: file or directory '{value}' does not exist.")
            elif param_class == 'FilesPatternParam':
                if _is_empty(value):
                    if not allows_empty:
                        errors.append(f"{label} is required.")
                elif skip_path_exists and skip_path_exists(value):
                    pass
                elif project_path:
                    pattern = _resolve_path(str(value), project_path)
                    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]
                    if not matches:
                        errors.append(
                            f"{label}: no files match pattern '{value}'.")
            elif param_class == 'IntParam':
                if _is_empty(value):
                    if not allows_empty:
                        errors.append(f"{label} is required.")
                elif not _is_valid_int(value):
                    errors.append(f"{label}: invalid integer value '{value}'.")
            elif param_class == 'FloatParam':
                if _is_empty(value):
                    if not allows_empty:
                        errors.append(f"{label} is required.")
                elif not _is_valid_float(value):
                    errors.append(f"{label}: invalid float value '{value}'.")
            elif param_class == 'StringParam':
                if _is_empty(value):
                    if not allows_empty:
                        errors.append(f"{label} is required.")

        for section_def in job_form['sections']:
            _validate_def(section_def)

        return errors

    @staticmethod
    def _should_validate_param(param_def, params, parent_visible=True):
        if not parent_visible:
            return False
        if not param_def.get('name'):
            return False
        if param_def.get('hidden'):
            return False
        if param_def.get('paramClass') in ('LabelParam', 'Group', 'Line', 'Section'):
            return False
        return _eval_condition(params, param_def.get('condition'))
