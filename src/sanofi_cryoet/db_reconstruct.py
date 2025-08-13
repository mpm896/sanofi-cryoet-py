# Matthew Martinez
# Sanofi - US Dept. of Large Molecules Research, Protein Engineering Group, Structural Biology

# Reconstruct tomograms during pipeline processing
# This script will be passed the data processing parameters by the main shell script 

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import sys
import subprocess
import tomllib

from .const import CONFIG_ARGS, PROC_DIR
from .utils import chdir, get_one_mdoc, read_mdoc, typewriter

@dataclass
class Config:
    """ Cryo-ET pipeline configuration object """
    setup: dict
    data: dict
    mc: dict
    imod: dict
    denoise: dict
    dirs: ConfigDirs = field(init=False)

    def __post_init__(self):
        data_dir = Path(self.setup['data']['RAW_DATA_DIR'].split('/')[-1])
        out_dir = Path(PROC_DIR)
        self.dirs = ConfigDirs(
            DATA_DIR=data_dir,
            SUBFRAME_DIR=Path(data_dir / 'Frames'),
            OUT_DIR=out_dir,
            WATCH_DIR=data_dir,  # Redundant
            PROC_DIR=Path(data_dir / 'Processed'),
            THUMB_DIR=Path(out_dir / 'alignedJPG'),
        )

    
    @classmethod
    def from_toml(cls, toml: Path) -> Config:
        with open(toml, "rb") as f:
            data = tomllib.load(f)
        status = cls._validate(data)
        if status[0] != 0:
            raise ValueError(f"Invalid entries in the config TOML. The following entries must be specified: {status[1]}")
        return cls(**data)


    @staticmethod
    def _validate(data: dict) -> tuple[int, list[str]]:
        """
        Ensure valid entries into the config TOML
        
        Params
        ______
        conf [dict]: loaded TOML

        Returns
        -------
        tuple[int, str]: error code (0 if ok, 1 if error) and list of values causing the error
        """
        errors = []
        for val in CONFIG_ARGS['setup']:
            if val not in data['setup']:
                errors.append(val)
        
        for val in CONFIG_ARGS['setup_data']:
            if val not in data['setup']['data']:
                errors.append(val)

        if data['setup']['data']['READ_MDOC'] == 0:
            for val in CONFIG_ARGS['data']:
                if val not in data['data']:
                    errors.append(val)

        if len(errors) > 0:
            return 1, errors
        return 0, errors


@dataclass
class ConfigDirs:
    DATA_DIR: Path
    SUBFRAME_DIR: Path
    OUT_DIR: Path
    WATCH_DIR: Path
    PROC_DIR: Path
    THUMB_DIR: Path


def setup_serieswatcher(config: Config) -> tuple[Path, Path]:
    """
    Construct the COM and ADOC files to to run serieswatcher

    :param: config
        Config containing tipeline arguments passed from the config TOML

    :return: tuple of files
        master com file, master adoc file
    """
    # Get those parameters which can be read from the mdoc - pixel size, exposure, tilt angles
    mdoc_info = read_mdoc(get_one_mdoc(config.dirs.OUT_DIR))

    master_com = Path.cwd() / Path('coms/BRT_MASTER.com')
    master_com.parent.mkdir(parents=True, exist_ok=True)
    master_adoc = Path.cwd() / Path('coms/BRT_MASTER.adoc')
    master_adoc.parent.mkdir(parents=True, exist_ok=True)

    if not config.data['PIXEL_SIZE']:
        config.data['PIXEL_SIZE'] == mdoc_info['Pixel Size']
    if config.imod['reconstruction']['THICKNESS_BINNED']:
        config.imod['reconstruction']['THICKNESS_UNBINNED'] = config.imod['reconstruction']['THICKNESS_BINNED'] * config.imod['final_alignment']['FINAL_BIN']
    do_sirt = 1 if config.imod['RECONSTRUCT_METHOD'] == 2 else 0

    # Construct the com file
    master_com.write_text(
        '$batchruntomo -StandardInput\n'
        'NamingStyle     1\n'
        'MakeSubDirectory\n'
        f'CPUMachineList  localhost:{config.setup["CPUS"]}\n'
        f'GPUMachineList  {config.setup["GPUS"]}\n'
        'NiceValue       15\n'
        'EtomoDebug      0\n'
        f'DirectiveFile   {master_adoc}\n'
        f'CurrentLocation {config.dirs.OUT_DIR}\n'
        'BypassEtomo\n'
    )
    
    # Construct the adoc file
    master_adoc.write_text(
        'setupset.systemTemplate = /usr/local/IMOD/SystemTemplate/cryoSample.adoc\n'
        f'runtime.Preprocessing.any.removeXrays = {config.imod['REMOVE_XRAYS']}\n'
        f'comparam.prenewst.newstack.BinByFactor = {config.imod['PREALIGN_BIN']}\n'
        f'runtime.Fiducials.any.trackingMethod = {config.imod['tracking']['TRACK_METHOD']}\n'
        f'setupset.copyarg.gold = {config.imod['tracking']['SIZE_GOLD']}\n'
        f'runtime.AlignedStack.any.binByFactor = {config.imod['final_alignment']['FINAL_BIN']}\n'
        f'runtime.Reconstruction.any.useSirt = {do_sirt}\n'
        'runtime.Trimvol.any.scaleFromZ = \n'
        f'runtime.Postprocess.any.doTrimvol = {config.imod['postprocess']['DO_TRIMVOL']}\n'
        f'setupset.copyarg.pixel = {config.data['PIXEL_SIZE']}\n'
        f'setupset.copyarg.rotation = {config.setup['TILTAXIS']}\n'
        f'setupset.copyarg.dosesym = {config.imod['dose_weight']['DOSE_SYM']}\n'
        f'setupset.copyarg.voltage = {config.imod['ctf']['VOLTAGE']}\n'
        f'setupset.copyarg.Cs = {config.imod['ctf']['CS']}\n'
        'comparam.prenewst.newstack.AntialiasFilter = 4\n'
        'comparam.newst.newstack.AntialiasFilter = 4\n'
        f'runtime.Trimvol.any.reorient = {config.imod['postprocess']['REORIENT']}\n'
        f'comparam.tilt.tilt.THICKNESS = {config.imod['reconstruction']['THICKNESS_UNBINNED']}\n'
    )

    if config.imod['tracking']['TRACK_METHOD'] == 0:
        # Fiducial tracking
        with master_adoc.open("a") as f:
            f.write(
                'runtime.Fiducials.any.seedingMethod = 1\n'
                f'comparam.track.beadtrack.SobelFilterCentering = {config.imod['tracking']['fiducial']['USE_SOBEL']}\n'
                f'comparam.autofidseed.autofidseed.TargetNumberOfBeads = {config.imod['tracking']['fiducial']['NUM_BEADS']}\n'
            )
        
        if int(config.imod['tracking']['fiducial']['USE_SOBEL']) == 1:
            with master_adoc.open("a") as f:
                f.write(
                    f'comparam.track.beadtrack.KernelSigmaForSobel = {config.imod['tracking']['fiducial']['SOBEL_SIGMA']}\n'
                )
    elif config.imod['tracking']['TRACK_METHOD'] == 1:
        # Patch tracking
        with master_adoc.open("a") as f:
            f.write(
                f'comparam.xcorr_pt.tiltxcorr.SizeOfPatchesXandY = {config.imod['tracking']['patch']['PATCH_SIZE_X']},{config.imod['tracking']['patch']['PATCH_SIZE_Y']}\n'
                f'comparam.xcorr_pt.tiltxcorr.OverlapOfPatchesXandY = {config.imod['tracking']['patch']['PATCH_OVERLAP_X']},{config.imod['tracking']['patch']['PATCH_OVERLAP_Y']}\n'
            )
    else:
        raise ValueError(f"Tracking method of {config.imod['tracking']['TRACK_METHOD']} is not supported")
    
    if config.imod['final_alignment']['DO_CTF'] == 1:
        with master_adoc.open("a") as f:
            f.write(
                f'runtime.AlignedStack.any.correctCTF = {config.imod['final_alignment']['DO_CTF']}\n'
                f'comparam.ctfplotter.ctfplotter.ScanDefocusRange = {config.imod['ctf']['DEFOCUS_RANGE_LOW']},{config.imod['ctf']['DEFOCUS_RANGE_HIGH']}\n'
                f'runtime.CTFplotting.any.autoFitRangeAndStep = {config.imod['ctf']['AUTOFIT_RANGE']},{config.imod['ctf']['AUTOFIT_STEP']}\n'
                'comparam.ctfplotter.ctfplotter.BaselineFittingOrder = 4\n'
                'comparam.ctfplotter.ctfplotter.SearchAstigmatism = 1\n'
            )

    if do_sirt == 0:
        with master_adoc.open("a") as f:
            f.write(
                f'comparam.tilt.tilt.FakeSIRTiterations = {config.imod['reconstruction']['FAKE_SIRT_ITERS']}'
            )
            
    return master_com, master_adoc




    

if __name__ == '__main__':
    if len(sys.argv) < 34:
        raise ValueError("Not enough arguments passed to DB_RECONSTRUCT")

    pipeline_args = {
        'cpus': sys.argv[1], 
        'gpus': sys.argv[2],
        'out_dir': sys.argv[3], 
        'read_mdoc': sys.argv[4], 
        'remove_xrays': sys.argv[5],
        'prealign_bin': sys.argv[6],
        'track_method': sys.argv[7], 
        'size_gold': sys.argv[8],
        'final_bin': sys.argv[9], 
        'do_sirt': sys.argv[10],
        'do_trimvol': sys.argv[11], 
        'pixel_size': sys.argv[12], 
        'tiltaxis': sys.argv[13],
        'dose_sym': sys.argv[14], 
        'voltage': sys.argv[15], 
        'cs': sys.argv[16], 
        'reorient': sys.argv[17], 
        'thickness_binned': sys.argv[18], 
        'thickness_unbinned': sys.argv[19],
        'use_sobel': sys.argv[20],
        'num_beads': sys.argv[21],
        'sobel_sigma': sys.argv[22],
        'patch_size': [sys.argv[23], sys.argv[24]],
        'patch_overlap': [sys.argv[25], sys.argv[26]],
        'do_ctf': sys.argv[27],
        'defocus_range': [sys.argv[28], sys.argv[29]],
        'autofit_range': sys.argv[30],
        'autofit_step': sys.argv[31],
        'tune_fitting_sample': sys.argv[32],
        'fake_sirt_iters': sys.argv[33]
    }
    assert 'out_dir' in pipeline_args
    pipeline_args['out_dir'] = Path.cwd() / Path(pipeline_args['out_dir'])

    print(pipeline_args)

    com, adoc = setup_serieswatcher(**pipeline_args)
    brt_pipeline = "brt_pipeline"
    with chdir(pipeline_args['out_dir']):
        # cmd = f'tmux new-session -d -s {brt_pipeline}'
        # subprocess.run(cmd, shell=True)

        # cmd = f'tmux send-keys "serieswatcher -com {com} -adoc {adoc}" C-m'
        cmd = f'serieswatcher -com {com} -adoc {adoc}'
        subprocess.run(cmd, shell=True)
        
        print(f"CHECK STATUS OF PIPELINE RECONSTRUCTION WITH COMMAND: tmux a -t {brt_pipeline}")
        print("DETACH SESSION (i.e. still running, but now longer watching it) with: control-b d")
        print(f"KILL SESSION: tmux -t {brt_pipeline} kill-session")
        print("KILL ALL SESSIONS: tmux kill-server")