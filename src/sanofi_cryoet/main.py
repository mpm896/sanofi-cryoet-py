# Matthew Martinez
# Sanofi - US Dept. of Large Molecules Research, Protein Engineering Group, Structural Biology
# 
# Entry point into the cryo-ET data processing pipeline

import argparse
import logging
import os
from pathlib import Path
import pprint
import sys
import subprocess
import time
import tomllib
from typing import Optional

from .const import DB_DIR, ID_DB, PROC_DIR, TIMEOUT, EXT, BIN, GPU
from .db_reconstruct import Config, setup_serieswatcher
from .db_transfer import CryoETDB
from .utils import typewriter

logging.basicConfig(
    level=logging.DEBUG,
    format="{asctime} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    encoding="utf-8"
)
formatter = logging.Formatter(
    fmt="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M"
)
logger = logging.getLogger(__name__)

def main():
    conf_toml = Path('/Users/U1036725/Documents/Projects/Cryo-ET_Setup/Pipeline_PyProject/sanofi-cryoet/src/sanofi_cryoet/config.toml')
    configs = Config.from_toml(conf_toml)
    user_id = configs.setup['USER_DB_ID']
    pipeline_setup(configs)

    # db = CryoETDB(user_id, ID_DB, DB_DIR)
    
    

    # start = time.time()
    # while True:
    #     db.initialize_datasets(PROC_DIR)
    #     new_procs: int = db.watch_for_completion(PROC_DIR)

    #     logger.info("NEW PROCS: %s" % new_procs)

    #     if new_procs:
    #         db.transfer(PROC_DIR, subframe_path)
    #         start = time.time()  # Reset the timer

    #     end = time.time()
    #     if (end - start) > TIMEOUT:
    #         fw_pipeline = 'fw_pipeline'
    #         brt_pipeline = 'brt_pipeline'
    #         transfer_pipeline = 'db_pipeline'
            
    #         kill_tmux([fw_pipeline, brt_pipeline, transfer_pipeline])
    #         break
    #     time.sleep(60)


def pipeline_setup(conf: Config) -> None:
    """ Initialize and setup the pipeline """
    data_dir = Path(conf.setup['data']['RAW_DATA_DIR'].split('/')[-1])
    subframe_dir = Path(data_dir / 'Frames')
    subframe_dir.mkdir(parents=True, exist_ok=True)

    if conf.setup['data']['TRANSFER_RAW_DATA'] == 1:
        typewriter([
            "===================================",
            "Copying data from the Glacios PC...",
            "==================================="
        ])

        transfer_raw_data(conf)

        # Modify mdoc files
        if conf.setup['SOFTWARE'] == 1:
            typewriter(["===== Data collected with serialEM ====="])
        else:
            tiltaxis = -90 - conf.setup['TILTAXIS']
            typewriter([
                "===== Data collected with Thermo Scientific Tomography 5 =====",
                f" -> New tilt axis is {tiltaxis}",
                "===== Renaming the mdocs with the extension .mrc.mdoc =====",
                "===== Adjusting the tilt axis in the mdoc ====="
            ], delay=0.02)
            mdocs = [f for f in Path(data_dir).rglob('*.mdoc')]

            # Remove duplicates and rename the mdoc files with proper file extension
            for mdoc in mdocs.copy():
                if conf.setup['data']['MDOC_DUPLICATE'] in mdoc.name:
                    mdoc.unlink()
                    mdocs.remove(mdoc)
                else:
                    mdoc.rename(mdoc.with_suffix('.mdoc.mrc'))

                    # Use shell command to change the tiltaxis in the mdoc file -- too lazy to figure this out with Python right now
                    cmd = f'head {mdoc.as_posix()} | sed -i -E "/TiltAxisAngle *= */s/-?[0-9]+\.[0-9]+/{tiltaxis}/" {mdoc.as_posix()}'
                    _call(cmd)
            
            # Copy all the frames to a subdirectory called "Frames"
            typewriter(["Moving frames to subdirectory..."], delay=0.02)
            for frame in Path(data_dir).glob(f'*{conf.setup['data']['FRAMES_NAME']}*'):
                frame.rename(subframe_dir / frame.name)


def transfer_raw_data(conf: Config) -> None:
    """ Transfer raw data from microscope to local directory """
    if conf.setup['PIPE_CLI'] == 1:
        n = (2 if 4 <= conf.setup['CPUS'] < 8 
                else 4 if conf.setup['CPUS'] >= 8
                else 0)
        magellan_dir = f"cp://its{'/'.join(conf.setup['data']['RAW_DATA_DIR'].split("its")[1:])}"
        cmd = (f"pipe storage cp -r --force --skip-existing -n {n} cp://{magellan_dir} {PROC_DIR}" if n > 0
                else f"pipe storage cp -r --force --skip-existing cp://{magellan_dir} {PROC_DIR}")
    else:
        cmd = f"rsync --progress --ignore-existing -avr {conf.setup['data']['RAW_DATA_DIR']}/ {PROC_DIR}"

    if _call(cmd) != 0:
        logger.warning("FAILED TO TRANSFER RAW DATA. EXIT CODE %d")


def kill_tmux(sessions: list[str], logger: Optional[logging.Logger]=None) -> None:
    """ Kill specified tmux sessions """
    for session in sessions:
        cmd = f'tmux kill-session -t {session}'
        if _call(cmd) != 0:
            logger.warning("FAILED TO KILL TMUX SESSION %s. EXIT CODE %d")


def _call(cmd: str, shell=True, *args, **kwargs) -> int:
    """ Call an external command to run with subprocess.run """
    exit = subprocess.run(cmd, shell=shell).returncode
    return exit