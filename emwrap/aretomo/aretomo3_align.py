import os
import shutil

from .aretomo3_modular import Aretomo3ModularBase
from .aretomo3 import AreTomo3

class AreTomo3AlignPipeline(Aretomo3ModularBase):
    name = 'emw-aretomo3-align'

    def __init__(self, args, output):
        super().__init__(args, output)
        self.ctf_mode = self._args.get('ctf_mode', 'estimate')
        self.use_previous_alignment = self._args.get('UsePreviousAlignment', False)

    def _install_previous_alignment(self, batch, ts_name):
        resolved = self._resolve_previous_alignment(ts_name, required=('stack', 'tlt'))
        for key, filename in (('stack', f'{ts_name}.mrc'), ('tlt', f'{ts_name}_TLT.txt')):
            destination = batch.join(filename)
            shutil.copy2(resolved[key], destination)

        for suffix in ('_ODD', '_EVN'):
            src = os.path.join(os.path.dirname(resolved['stack']), f'{ts_name}{suffix}.mrc')
            if os.path.exists(src):
                dst = batch.join(f'{ts_name}{suffix}.mrc')
                if os.path.abspath(src) != os.path.abspath(dst):
                    shutil.copy2(src, dst)
        return resolved

    def get_aretomo3_proc(self, gpu):
        def process(batch):
            ts_name = batch['tsName']
            batch.create()
            row = self._input_row(ts_name)
            previous = None
            if self.use_previous_alignment:
                previous = self._install_previous_alignment(batch, ts_name)
                full_stack = batch.join(f'{ts_name}.mrc')
                table = None
            else:
                table, full_stack, _ = self._stage_stack_and_tlt(batch, ts_name, row) # Not using aligned angles
            # Cmd 1 writes alignment metadata but not the output MRC stacks.
            # Populate its output directory with the stacks we composed from
            # the RELION input so the normal result collector can register them.
            batch.mkdir('output')
            shutil.copy2(full_stack, batch.join('output', f'{ts_name}.mrc'))

            args = dict(self._args)
            if self.ctf_mode == 'preserve':
                args['aretomo3.CorrCTF'] = False
            at3 = AreTomo3(self.acq, **args)

            # Only if coming from a non-previous alignment path do we have a table of images to write half-set stacks from.
            have_half_sets = table is not None and (
                self._has_complete_image_column(table, 'rlnMicrographNameOdd')
                and self._has_complete_image_column(table, 'rlnMicrographNameEven')
            )
            if have_half_sets:
                for column, suffix in (
                    ('rlnMicrographNameOdd', '_ODD'),
                    ('rlnMicrographNameEven', '_EVN'),
                ):
                    stack = batch.join(f'{ts_name}{suffix}.mrc')
                    self._write_stack_from_images(stack, table, column)
                    shutil.copy2(stack, batch.join('output', f'{ts_name}{suffix}.mrc'))
            # else:
                # A full summed image cannot be split into independent halves.
                # Do not create fake half-sets or let AreTomo3 expect them.
                # at3.args['-SplitSum'] = 0
                # batch.log(
                    # f'WARNING: {ts_name}: no complete odd/even input image columns; '
                    # 'running with -SplitSum 0.',
                    # flush=True,
                # )

            # Cmd 1 is the alignment-only public job.  Set this after form
            # serialization so ExtraArgs cannot accidentally request a volume.
            at3.args['-VolZ'] = 0
            at3.process_batch(batch, gpu=gpu, cmd=1, input_prefix=f'./{ts_name}',
                              input_suffix='.mrc', input_skips='_ODD,_EVN,_Vol,_CTF',
                              ts_name=ts_name)

            tlt_src = (previous['tlt'] if previous else batch.join(f'{ts_name}_TLT.txt'))
            if os.path.exists(tlt_src):
                tlt_dst = batch.join('output', f'{ts_name}_TLT.txt')
                if os.path.abspath(tlt_src) != os.path.abspath(tlt_dst):
                    shutil.copy2(tlt_src, tlt_dst)
            
                batch['results'][0]['at3MappingFile'] = tlt_dst

            # TODO: We need to copy the half-set stacks from the batch root to the batch/output and register them so the normal result collector can bring them to the main output directory.

            return batch
        return process

    def _setAretomo3Params(self, tilt_dict, result, **kwargs):
        if self.ctf_mode != 'preserve':
            return super()._setAretomo3Params(tilt_dict, result, **kwargs)
        # Keep CTF values exactly as supplied but still write alignment fields.
        ctf_columns = {key: tilt_dict.get(key, '') for key in self._individual_tilt_series_extra_cols()
                       if key.startswith('rlnCtf') or key.startswith('rlnDefocus')}
        result_dict = super()._setAretomo3Params(tilt_dict, result, **kwargs)
        result_dict.update(ctf_columns)
        return result_dict

if __name__ == '__main__':
    AreTomo3AlignPipeline.main()
