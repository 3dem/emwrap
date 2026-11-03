# Job form wizards

A **wizard** is a dialog that helps the user set some parameters of a job
form interactively: it reads the current (unsaved) form values, may load data
from the project (e.g. the pixel size of the input tilt-series), lets the
user choose values with immediate feedback, and on **OK** writes new values
back into the form.

A wizard may also compute new data while it is open (`preview`), e.g. a
tentative mask for the values being chosen.

Wizards never save or launch the job. OK only fills form params; the user
still clicks Save or Launch, so the usual validation applies.

The wizards so far:

- `ExportTomoParticlesWizard`: box and pixel size of the particles exported
  from tilt-series, used by Warp `emw-warp-export_particles` and Relion
  `relion.pseudosubtomo`.
- `VolumeMaskWizard`: the mask of a map (a mask map or a sphere), used by
  Relion `relion.class3d.tomo` and `relion.refine3d.tomo`.
- `MaskCreateWizard`: the parameters of a mask created from a map, with a
  tentative mask computed on the server (preview), used by
  `emw-relion-mask_create`.

## Architecture

A wizard has four parts. The logic and the adaptors are Python in emwrap, so
they can be tested without a browser and reused by other clients (MCP, CLI).
EMhub only exposes two endpoints, and emflow renders the dialog.

| Part | Where | What it does |
| --- | --- | --- |
| `Wizard` | `emwrap/base/wizards.py` (or `emwrap/<pkg>/wizards.py`) | Tool-agnostic logic. `prepare(inputs)` validates and resolves data when the dialog opens. `finalize(outputs, prepared)` validates the user's values on OK. `ui` names the emflow component. |
| `WizardAdaptor` | `emwrap/<pkg>/wizards.py` | Maps one job type's params to the wizard inputs (`toWizard`) and the wizard outputs back to params (`fromWizard`). The same wizard has one adaptor per job type. |
| Form spec | `config/forms/<jobtype>.json`, `wizards` | Which adaptor to use and which params show the wizard button. |
| UI component | `emflow/src/components/wizards/<Ui>.tsx` | Renders the wizard's body inside `WizardDialog` and reports the selected values. |

```
 ProtocolForm (emflow)            EMhub api.py                  emwrap
 ─────────────────────            ────────────                  ──────
 wand button on a param
   svc.openWizard(params) ──────► /api/wizard_open ───────────► Wizards.open()
                                                                  adaptor.toWizard(params)
                                                                  wizard.prepare(inputs)
   errors? → ValidationErrorsDialog ◄── {errors} or {ui, inputs, data, warnings}
   WizardDialog + <ui> component
   OK: svc.applyWizard(params, outputs) ► /api/wizard_apply ───► Wizards.apply()
                                                                  wizard.prepare(...)   (again, server side)
                                                                  wizard.finalize(outputs, prepared)
                                                                  adaptor.fromWizard(outputs, params)
   set returned params in the form ◄── {errors} or {params, warnings}
   (not saved)

 while open, if the wizard has a preview:
   svc.previewWizard(params, outputs) ► /api/wizard_preview ──► Wizards.preview()
                                                                  wizard.prepare(...)
                                                                  wizard.preview(outputs, prepared)
   merged into the component's data ◄── {errors} or {data, warnings}
```

### Rules

- **The wizard only fills the form.** `fromWizard` returns
  `{param_name: value}`; the job is never saved by a wizard.
- **Refuse early.** If a required input is missing or unreadable, raise
  `WizardError` from the adaptor or from `prepare()`. The dialog does not open
  and the user sees the errors. On OK, a `WizardError` from `finalize()` keeps
  the dialog open with the errors shown.
- **Don't trust read-only values from the client.** `Wizards.apply` runs
  `prepare()` again on the server and passes the result to `finalize()`, so
  values such as the input pixel size come from the project, not the browser.
- **Warnings are not errors.** Return them from `prepare()` (shown in the
  dialog), `finalize()`, or append them to `adaptor.warnings` (shown as toasts
  after OK).
- **Not in the workflow template editor.** Wizards need project data. The
  editor's service has no `openWizard`/`applyWizard`, so the wand button is
  hidden there.
- **One `wizards.py` per subpackage.** The framework and the generic wizards
  are in `emwrap/base/wizards.py`. Each tool's adaptors (and tool-specific
  wizards) go in its own subpackage: `emwrap/warp/wizards.py`,
  `emwrap/relion/wizards.py`, and so on.

## Wizard contract

```python
from emwrap.base.wizards import Wizard, WizardError, to_float, to_int

class MyWizard(Wizard):
    ui = 'MyWizard'            # emflow component name (WIZARD_COMPONENTS)
    label = 'My wizard'        # default dialog title

    def prepare(self, inputs):
        # inputs: canonical names, produced by the adaptor
        # self.project.join(path) / self.join(path) resolve project paths
        if not inputs.get('something'):
            raise WizardError("Select something first.")
        return {
            'inputs': {...},    # initial values of the editable fields
            'data': {...},      # read-only values shown in the UI
            'warnings': [],
        }

    def finalize(self, outputs, prepared):
        # outputs: what the UI component reported with onChange
        # prepared: the result of prepare(), computed again on the server
        return {'outputs': {...}, 'warnings': []}

    # Optional: new data while the wizard is open (UI prop `preview`)
    preview_on_open = False    # True: preview(inputs) data comes with open()

    def preview(self, outputs, prepared):
        return {'data': {...}, 'warnings': []}
```

The UI decides when to ask for a preview (`MaskCreateWizard`: a
Re-calculate button), but keep it fast: `MaskCreateWizard` works on the map downscaled to about 100 px.

Keep the wizard's input and output names **tool-agnostic** (units included in
the name or the docstring), so another job type only needs a new adaptor.

## Adaptor contract

```python
from emwrap.base.wizards import WizardAdaptor

class MyJobAdaptor(WizardAdaptor):
    wizard = MyWizard

    def toWizard(self, params):
        # params: current form values (strings/numbers/bools, not saved)
        return {'something': params.get('my_param')}

    def fromWizard(self, outputs, params):
        # outputs: the result of finalize()['outputs']
        # optionally: self.warnings.append("...")
        return {'my_param': outputs['something']}
```

Adaptors convert units and conventions, not just names. For example, Relion's
`pseudosubtomo` takes the box in Å plus a binned box in pixels, while Warp
takes an output pixel size plus a box in pixels.

## Form spec

Add a `wizards` list to the job form JSON (validated by
`config/forms/form-schema.json`):

```json
"wizards": [
    {
        "id": "export_box",
        "label": "Box and pixel size wizard",
        "adaptor": "emwrap.warp.wizards.WarpExportParticlesAdaptor",
        "params": ["ts_export_particles.output_angpix", "ts_export_particles.box"]
    }
],
```

- `id`: unique within the form. It is sent back as `wizard_id`.
- `label`: the dialog title and the button tooltip.
- `adaptor`: full Python path of the `WizardAdaptor` subclass. It is imported
  on demand and never sent to the browser.
- `params`: the params that show the wand button. EMhub
  (`get_form_definition` → `Wizards.tag_form`) adds `"wizard": "<id>"` to
  these params and copies `{id, label, params}` to the served form. A param
  inside a `Line` puts the button on the Line's row.

## UI component (emflow)

1. Create `emflow/src/components/wizards/<Ui>.tsx`, exporting a component
   that takes `WizardComponentProps`:
   ```ts
   { inputs: Record<string, any>;   // prepare()['inputs']
     data: Record<string, any>;     // prepare()['data'], read-only
     onChange: (outputs) => void }  // report the current values on every change
   ```
2. Register it in `WIZARD_COMPONENTS` in `WizardDialog.tsx` under the same
   name as `Wizard.ui`.
3. Follow the emflow rules (`.claude/skills/emflow-components`): use MUI,
   which portals into the overlay root, stay inside `.emflow-root`, and make
   no network calls of your own.
4. For testing without a server, add a fake to `dev/fakeService.js`
   (`openWizard`/`applyWizard`) and a protocol that uses it (see "Wizards
   (dev)" in the dev protocols drawer).

`WizardDialog` provides the title, warnings, errors, and the Cancel/OK
buttons. `ProtocolForm` sets the returned params (matched by name) and shows
warnings as toasts.

## Service and API contract

emflow `ProjectService` (both methods optional; without them the wizards are
disabled):

```ts
openWizard?(projectId, protocolId | null, protocolClassName, wizardId, params)
  -> { errors } | { id, label, ui, inputs, data, warnings }
applyWizard?(projectId, protocolId | null, protocolClassName, wizardId, params, outputs)
  -> { errors } | { params, warnings }
previewWizard?(projectId, protocolId | null, protocolClassName, wizardId, params, outputs)
  -> { errors } | { data, warnings }       // given to the component as `preview`
```

EMhub (`templates/project_widget_svc.html` → `blueprints/api.py`):

| Endpoint | attrs | result (`wizard`) |
| --- | --- | --- |
| `POST /api/wizard_open` | project args, `run_id` (null for a new job), `job_type`, `wizard_id`, `params` | `Wizards.open(...)` |
| `POST /api/wizard_apply` | same + `outputs` | `Wizards.apply(...)` |
| `POST /api/wizard_preview` | same + `outputs` | `Wizards.preview(...)` |

A `WizardError` is a `JobValidationError`, so `_handle_item` answers
`{errors: [...], status: 1}`. The svc returns these errors (it does not show
them) and emflow displays them.

## Example: ExportTomoParticlesWizard

- **Wizard** (`emwrap/base/wizards.py`): the input pixel size is the
  tilt-series pixel size (`rlnTomoTiltSeriesPixelSize` of the tomograms STAR;
  a warning if the tilt-series differ). Inputs: `tomograms`, `in_box` (or
  `in_box_angst`), `out_angpix`, `out_box`, `diameter`. The particle diameter
  (Å) defaults to half of the input box when the form has none. Outputs:
  `in_box`, `out_angpix`, `out_box`, `diameter`, plus the derived
  `in_angpix`, `in_box_angst`, `out_box_angst` and `binning`.
- **UI** (`ExportTomoParticlesWizard.tsx`): an Input pane (read-only pixel
  size, box), a centre column with the binning factor (output pixel size =
  input × binning; editing either one updates the other) and the particle
  diameter in Å, and an Output pane (pixel size, box, "Match input"). The two
  box sizes are independent. Each pane draws its box at a common Å scale, with
  the other box dashed and the particle diameter as an orange reference
  circle that follows the diameter field. Each pane shows the diameter in px
  (at that pane's pixel size) and in Å. An empty diameter field falls back to
  half of the input box, which is shown as its placeholder.
- **Warp adaptor** (`emwrap/warp/wizards.py`): tomograms come from
  `input_tomograms` or the optimisation set in `input_coordinates`. The
  diameter comes from `ts_export_particles.diameter`. OK sets
  `ts_export_particles.output_angpix`, `ts_export_particles.box` and
  `ts_export_particles.diameter`. Warp has no input box, so the wizard derives
  it as the same size in Å.
- **Relion adaptor** (`emwrap/relion/wizards.py`): tomograms come from
  `in_tomograms` (direct entries) or `in_optimisation`. `box_size_angst` maps
  to and from the input box, and `binned_size_pix` to and from the output box.
  Relion derives the output pixel size as `box_size_angst / binned_size_pix`,
  so OK adds a warning when that differs from the wizard's pixel size. The
  form has no diameter: the wizard uses half of the box and OK does not save
  it.

## Example: VolumeMaskWizard

- **Wizard** (`emwrap/base/wizards.py`): inputs `volume` (required), `mask`
  (a mask map, optional) and `diameter` (Å, the spherical mask). The map and
  the mask must exist and have the same dimensions (`read_volume_header`,
  from the MRC header). A mask map takes precedence over the sphere; the
  diameter defaults to 80% of the box. `data.volume` and `data.mask` are
  `{kind: 'volume', path, dimensions, voxelSize}`. On OK, a sphere needs a
  positive diameter (a warning if it is larger than the box); with a mask
  map the diameter is not changed. Outputs: `mask_type`, `mask`, `diameter`.
- **Volumes in the data**: a data item with `kind: 'volume'` is a volume the
  UI shows. EMhub's `/api/wizard_open` adds `density` to it (the request of
  its density grid, `volume_density_source`), which emflow's VolumeViewer
  loads. Other wizards that show volumes can use the same convention.
- **UI** (`VolumeMaskWizard.tsx`): a VolumeViewer with the Slices and 3D
  volume tabs and the mask (map or sphere) drawn over both. The sphere
  diameter is changed in the viewer's mask panel (px and Å).
- **Relion adaptor** (`RelionReferenceMaskAdaptor`, Class3D and Refine3D):
  the volume is `fn_ref`, the mask `fn_mask` and the diameter
  `particle_diameter`. The wand is on `fn_mask` and `particle_diameter`.
  Without a mask, OK sets `particle_diameter`; with one, it sets nothing.

## Example: MaskCreateWizard

- **Wizard** (`emwrap/base/wizards.py`): inputs `volume` (required),
  `threshold`, `lowpass` (Å), `extend` and `soft_edge` (px of the map),
  `invert` and `angpix`. `prepare()` reads the map downscaled by an integer
  block mean to at most 100 px (`volume_mask.load_binned_map`, cached) and
  sends its value range for the threshold slider; without a threshold it
  starts at mean + 3σ. `preview()` (also on open) computes the tentative
  mask with numpy (`volume_mask.compute_mask`: lowpass with a raised cosine
  edge, threshold, extend or shrink by dilation with a ball through FFT
  convolutions, soft edge from dilations in half-pixel steps, invert) and
  sends it inline (`data.mask`, a VolumeDensity) with the share of the box
  it covers (`mask_fraction`); an empty or full mask adds a warning.
  It is a guide only: coarser, with approximate edges; the job still runs
  `relion_mask_create`.
- **UI** (`MaskCreateWizard.tsx`): the values on the left (threshold field
  and slider over the map values, lowpass, extend, soft edge, invert) and
  the map in a VolumeViewer (3D volume first, with the surface at the first
  threshold, and Slices) with the tentative mask over it. A new mask is
  asked with the Re-calculate button (not on each change), greyed out until
  a value changes and highlighted in orange then, with a spinner
  and a loading overlay on the viewer while it is computed.
- **Relion adaptor** (`RelionMaskCreateAdaptor`): only for masks from a
  volume (De novo creation refuses to open it). It maps
  `relion_mask_create.volume.*`: `i`, `ini_threshold`, `lowpass` (-1: none),
  `extend_inimask`, `width_soft_edge`, `invert` and `angpix`. The wand is on
  the threshold, extend, soft edge and lowpass.

## Tests

```bash
python -m unittest emwrap.tests.test_wizards
```

The tests build a temporary project with STAR and MRC files and check form
tagging, the adaptors, and the error cases. Add a test for each new wizard and
adaptor.

## Adding a wizard: checklist

1. Write the `Wizard` subclass, generic in `emwrap/base/wizards.py` or
   tool-specific in `emwrap/<pkg>/wizards.py`.
2. Write a `WizardAdaptor` for each job type in that tool's `wizards.py`.
3. Add the `wizards` entry to each job form JSON.
4. Add the emflow component and register it in `WIZARD_COMPONENTS`.
5. Add unit tests (emwrap) and a fake in `dev/fakeService.js` (emflow).
6. Rebuild emflow and copy the bundle to EMhub (`./build_and_copy.sh`).
7. Update this page and the emflow-components skill.
