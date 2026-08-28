import shutil

from .aretomo3_modular import Aretomo3ModularBase
from emtools.jobs import TsStarBatchManager
from .aretomo3 import AreTomo3

class AreTomo3AlignPipeline(Aretomo3ModularBase):
    name = 'emw-aretomo3-align'

    def __init__(self, args, output):
        super().__init__(args, output)
        self.ctf_mode = self._args.get('ctf_mode', 'estimate')

    def get_aretomo3_proc(self, gpu):
        def process(batch):
            ts_name = batch['tsName']
            batch.create()
            row = next(row for row in self.inputTsTable if row.rlnTomoName == ts_name)
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

            have_half_sets = (
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
            else:
                # A full summed image cannot be split into independent halves.
                # Do not create fake half-sets or let AreTomo3 expect them.
                at3.args['-SplitSum'] = 0
                batch.log(
                    f'WARNING: {ts_name}: no complete odd/even input image columns; '
                    'running with -SplitSum 0.',
                    flush=True,
                )

            # Cmd 1 is the alignment-only public job.  Set this after form
            # serialization so ExtraArgs cannot accidentally request a volume.
            at3.args['-VolZ'] = 0
            at3.process_batch(batch, gpu=gpu, cmd=1, input_prefix=f'./{ts_name}',
                              input_suffix='.mrc', input_skips='_ODD,_EVN,_Vol,_CTF',
                              ts_name=ts_name)
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

    def _output(self, batch):
        ts_name = batch['tsName']
        if batch.error:
            self._allResults[ts_name] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}
            self._allResults[ts_name] = self._copy_result(result, ts_name)
        batch.info['tsName'] = ts_name
        self._registerOutputs()
        self.updateBatchInfo(batch)
        return batch

    # -------- Pipeline lifecycle ---------- 
    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs =  self._args['input_tiltseries']
        print(f"Input tilt-series: {len(self.inputTsTable)}")  

        if self.registerOnly:
            self._register_existing_final_outputs()
            return
        
        self.mkdir(self.outputTsDir)
        
        batchMgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        g = self.addGenerator(batchMgr.generate)
        
        self.addGpuProcessors(g, self.get_aretomo3_proc, self._output)


if __name__ == '__main__':
    AreTomo3AlignPipeline.main()
