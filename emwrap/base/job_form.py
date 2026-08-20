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
            if params := container_def.get('params', None):
                for p in params:
                    yield from _iter_params(p)
            else:
                yield container_def

        for section_def in job_form['sections']:
            yield from _iter_params(section_def)

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
