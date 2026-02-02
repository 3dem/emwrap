# How to Define New Forms

This guide explains how to create new graphical forms using a JSON structure. Based on the provided examples, here is a breakdown of the file structure and the supported parameter types.

## File Structure

A form is defined by a single JSON file. The root of the JSON object must contain a `sections` array.

```json
{
    "sections": [
        // ... one or more section objects
    ]
}
```

### Sections

The `sections` array contains one or more section objects. Each section represents a grouping of parameters in the UI, often displayed as a separate tab or a framed box.

A section object has the following properties:

*   `label` (string): The title of the section, displayed in the UI.
*   `params` (array): An array of parameter objects that will be displayed in this section.

```json
{
    "label": "Section Title",
    "params": [
        // ... one or more parameter objects
    ]
}
```

## Parameter Definitions

Each object in the `params` array defines a single graphical parameter or UI element.

### Common Attributes

Most parameter types share these common attributes:

| Attribute | Type | Description |
| --- | --- | --- |
| `name` | string | The internal name of the parameter. This is used to identify the parameter and its value. |
| `label` | string | The display name for the parameter in the UI. |
| `default`| any | The default value for the parameter. |
| `help` | string | A help text or tooltip that provides more information about the parameter. |
| `paramClass`| string | The type of the graphical parameter. See supported parameter types below. |
| `pointerClass`| string | (Optional) Special attribute to define a "class" (beyond the actual value) that this parameter represents, kind of a "meta class". For example, a StringParam that points to an input particles STAR file can define its pointerClass as 'Particles'.|

### Additional Attributes

Besides the common attributes, some other attributes can be defined:

| Attribute | Type | Description |
| --- | --- | --- |
| `condition` | string | Condition to be evaluated with other  |
| `validators` | string | Most of the params have default validators (see sections below), but additional validations can be defined. |
| `allowsEmpty`| boolean | By default, a value should be set for each param. If this property is `true`, empty values are allowed. |
| `readOnly` | boolean | If `true`, the param will be displayed in the UI, but the user can not be modified. |
| `hidden`| boolean | Param will not be displayed, but it will be included in the job values dictionary. |
| `tableMin, tableMax`| integer | Define these two attributes to create a Table of the given parameter type.  |

---

## Supported Parameter Types

The `paramClass` attribute determines the type of graphical control to display. Here are the supported types:

### `LabelParam`

A simple text label, used for displaying information or titles within a form section.

**Example:**
```json
{
    "name": "label_input",
    "label": "Input / Reference",
    "paramClass": "LabelParam"
}
```

### `StringParam`

A standard text input field.

| Attribute | Type | Description |
| --- | --- | --- |
| `pattern` | string | Can be used to specify a file-browsing pattern, e.g., `"STAR Files (*.star)"`. |

**Example:**
```json
{
    "name": "fn_in_raw",
    "label": "Raw input files:",
    "paramClass": "StringParam",
    "default": "Micrographs/*.tif",
    "pattern": "Movie or Image (*.{mrc,mrcs,tif,tiff,eer})",
    "help": "Provide a Linux wildcard that selects all raw movies or micrographs to be imported."
}
```

### `IntParam`

A field for entering integer values.

**Example:**
```json
{
    "name": "log_diam_min",
    "label": "Min. diameter for LoG filter (A)",
    "default": 200,
    "paramClass": "IntParam",
    "help": "The smallest allowed diameter for the blob-detection algorithm."
}
```

### `FloatParam`

A field for entering floating-point values.

**Example:**
```json
{
    "name": "acq.cs",
    "label": "Spherical Aberration (mm)",
    "default": 2.7,
    "valueClass": "Float",
    "paramClass": "FloatParam",
    "help": ""
}
```

### `BooleanParam`

A checkbox for true/false values.

**Example:**
```json
{
    "name": "do_log",
    "label": "OR: use Laplacian-of-Gaussian?",
    "default": false,
    "valueClass": "Boolean",
    "paramClass": "BooleanParam",
    "help": "If set to Yes, a Laplacian-of-Gaussian blob detection will be used."
}
```

### `EnumParam`

A dropdown menu (combobox) for selecting from a list of options.

| Attribute | Type | Description |
| --- | --- | --- |
| `choices` | array or dict| If choices is a list, the items will be displayed and used as the selected value. If it is a dict, the keys will be used for the value. |
| `display` | string | The display style, e.g., `"combo"`. |

**Example:**
```json
{
    "name": "ts_export_type",
    "label": "Export type: ",
    "paramClass": "EnumParam",
    "choices": ["2d", "3d"],
    "display": "combo"
}
```

## Grouping elements

### `Line`

A special parameter that groups other parameters to be displayed on a single line in the UI.

| Attribute | Type | Description |
| --- | --- | --- |
| `params` | array | An array of parameter objects to be displayed on the same line. |

**Example:**
```json
{    
    "paramClass": "Line",
    "label": "Resolution range:",
    "params": [
        {
            "name": "ts_ctf.range_low",
            "label": "low",
            "default": 30,            
            "paramClass": "FloatParam",
            "help": "Minimum resolution in Angstrom to consider in fit"
        },
        {
            "name": "ts_ctf.range_high",
            "label": "high",
            "default": 4,
            "paramClass": "FloatParam",
            "help": "Maximum resolution in Angstrom to consider in fit"
        }
    ]
}
```

### `Group`

A special parameter that groups other parameters to be displayed on a grouped section. 

| Attribute | Type | Description |
| --- | --- | --- |
| `params` | array | An array of parameter objects to be displayed on the group. |

**Example:**
```json
{    
    "paramClass": "Group",
    "label": "Match binning:",
    "params": [
    {
        "name": "binningTM",
        "label": "Transformation matrix binning",
        "expertLevel": 0,
        "condition": null,
        "paramClass": "IntParam",
        "help": "Binning of the tilt series at which the transformation matrices were calculated.",
        "default": 13
    },
    {
        "name": "binningTS",
        "label": "Tilt-series binning",
        "expertLevel": 0,
        "condition": null,
        "paramClass": "IntParam",
        "help": "Binning of the tilt-series.",
        "default": 1
    }
    ]
}
```

---

## Special Cases

### Unnamed Parameters

In some cases, you might find objects in the `params` array that do not have a `name` or `paramClass`. These seem to be used for passing direct command-line arguments or setting default values. The keys of the object are treated as the argument flags.

**Example:**
This object sets default values for `--tomogram-ctf-model` and `--rng-seed`.
```json
{                    
    "--tomogram-ctf-model": "phase-flip",
    "--rng-seed": 420
}
```

### Empty Objects

An empty object `{}` can be used as a spacer in the UI to create a visual separation between parameters.
