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

from emtools.jobs import Workflow
from emtools.metadata import Table, StarFile, StarMonitor

from emwrap.base import Acquisition


class RelionStar:

    @staticmethod
    def optics_table(acq, opticsGroup=1, opticsGroupName="opticsGroup1",
                     mtf=None, originalPixelSize=None):
        origPs = originalPixelSize or acq['pixel_size']

        values = {
            'rlnOpticsGroupName': opticsGroupName,
            'rlnOpticsGroup': opticsGroup,
            'rlnMicrographOriginalPixelSize': origPs,
            'rlnVoltage': acq['voltage'],
            'rlnSphericalAberration': acq['cs'],
            'rlnAmplitudeContrast': acq.get('amplitude_constrast', 0.1),
            'rlnMicrographPixelSize': acq['pixel_size']
        }
        if mtf:
            values['rlnMtfFileName'] = mtf
        return Table.fromDict(values)

    @staticmethod
    def movies_table(**kwargs):
        extra_cols = kwargs.get('extra_cols', [])
        return Table([
            'rlnMicrographMovieName',
            'rlnOpticsGroup'
        ] + extra_cols)

    @staticmethod
    def micrograph_table(**kwargs):
        extra_cols = kwargs.get('extra_cols', [])
        return Table([
            'rlnMicrographName',
            'rlnOpticsGroup',
            'rlnCtfImage',
            'rlnDefocusU',
            'rlnDefocusV',
            'rlnCtfAstigmatism',
            'rlnDefocusAngle',
            'rlnCtfFigureOfMerit',
            'rlnCtfMaxResolution'
        ] + extra_cols)

    @staticmethod
    def coordinates_table(**kwargs):
        return Table(['rlnMicrographName', 'rlnMicrographCoordinates'])

    @staticmethod
    def get_acquisition(inputTableOrFile):
        """ Load acquisition parameters from an optics table
        or a given input STAR file.
        """
        if isinstance(inputTableOrFile, Table):
            tOptics = inputTableOrFile
        else:
            with StarFile(inputTableOrFile) as sf:
                tOptics = sf.getTable('optics')

        o = tOptics[0]._asdict()  # get first row

        return Acquisition(
            pixel_size=o.get('rlnMicrographPixelSize',
                             o['rlnMicrographOriginalPixelSize']),
            voltage=o['rlnVoltage'],
            cs=o['rlnSphericalAberration'],
            amplitude_constrast=o.get('rlnAmplitudeContrast', 0.1)
        )

    @staticmethod
    def pipeline_tables():
        return {
            'processes': Table(['rlnPipeLineProcessName',
                                'rlnPipeLineProcessAlias',
                                'rlnPipeLineProcessTypeLabel',
                                'rlnPipeLineProcessStatusLabel']),
            'nodes': Table(['rlnPipeLineNodeName',
                            'rlnPipeLineNodeTypeLabel',
                            'rlnPipeLineNodeTypeLabelDepth']),
            'output_edges': Table(['rlnPipeLineEdgeProcess',
                                   'rlnPipeLineEdgeToNode']),
            'intput_edges': Table(['rlnPipeLineEdgeFromNode',
                                   'rlnPipeLineEdgeProcess'])
        }

    @staticmethod
    def write_pipeline(pipeline_star, jobCounter=1, tables=None):
        with StarFile(pipeline_star, 'w') as sf:
            sf.writeTimeStamp()
            tGeneral = Table(['rlnPipeLineJobCounter'])
            tGeneral.addRowValues(jobCounter)
            sf.writeTable('pipeline_general', tGeneral, singleRow=True)

            if tables:
                for name, t in tables.items():
                    if len(t):
                        sf.writeTable(f"pipeline_{name}", t)

    @staticmethod
    def pipeline_to_workflow(pipelineStar):
        """ Read the Relion pipeline star file and build the proper Workflow. """
        wf = Workflow()
        with StarFile(pipelineStar) as sf:
            tables = sf.getTableNames()

            def _table(name):
                fullname = f"pipeline_{name}"
                return sf.getTable(fullname) if fullname in tables else None

            if tGeneral := _table('general'):
                wf.jobNextIndex = int(tGeneral[0].rlnPipeLineJobCounter)
            else:
                wf.jobNextIndex = 1

            if tProc := _table('processes'):
                for row in tProc:
                    wf.registerJob(row.rlnPipeLineProcessName,
                                   alias=row.rlnPipeLineProcessAlias,
                                   status=row.rlnPipeLineProcessStatusLabel,
                                   jobtype=row.rlnPipeLineProcessTypeLabel)

            if tNodes := _table('nodes'):
                nodes = {row.rlnPipeLineNodeName: row.rlnPipeLineNodeTypeLabel
                         for row in tNodes}
            else:
                nodes = {}

            if tOutput := _table('output_edges'):
                for row in tOutput:
                    job = wf.getJob(row.rlnPipeLineEdgeProcess)
                    nodeName = row.rlnPipeLineEdgeToNode
                    job.registerOutput(nodeName, datatype=nodes[nodeName])

            if tInput := _table('input_edges'):
                for row in tInput:
                    job = wf.getJob(row.rlnPipeLineEdgeProcess)
                    job.addInputs([wf.getData(row.rlnPipeLineEdgeFromNode)])

        return wf

    @staticmethod
    def workflow_to_pipeline(wf, pipelineStar):
        """ Write the input workflow as the expected Relion pipeline STAR file. """
        tables = RelionStar.pipeline_tables()
        tProc = tables['processes']
        tNodes = tables['nodes']
        for job in wf.jobs():
            tProc.addRowValues(
                rlnPipeLineProcessName=job.id,
                rlnPipeLineProcessAlias=job['alias'],
                rlnPipeLineProcessStatusLabel=job['status'],
                rlnPipeLineProcessTypeLabel=job['jobtype']
            )

            for o in job.outputs:
                tNodes.addRowValues(
                    rlnPipeLineNodeName=o.id,
                    rlnPipeLineNodeTypeLabel=o['datatype'],
                    rlnPipeLineNodeTypeLabelDepth=1
                )

            # if tOutput := _table('output_edges'):
            #     for row in tOutput:
            #         job = wf.getJob(row.rlnPipeLineEdgeProcess)
            #         nodeName = row.rlnPipeLineEdgeToNode
            #         job.registerOutput(nodeName, datatype=nodes[nodeName])
            #
            # if tInput := _table('input_edges'):
            #     for row in tInput:
            #         job = wf.getJob(row.rlnPipeLineEdgeProcess)
            #         job.addInputs([wf.getData(row.rlnPipeLineEdgeFromNode)])

        RelionStar.write_pipeline(pipelineStar, wf.jobNextIndex, tables)