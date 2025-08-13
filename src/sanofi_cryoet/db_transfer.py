# Matthew Martinez
# Sanofi - US Dept. of Large Molecules Research, Protein Engineering Group, Structural Biology

# Transfer actively processing data to the database and a centralized location for all cryo-ET data at Sonofi US
# This script will be passed the data processing directory by the main script 

import logging
import os
from pathlib import Path
import sys
import subprocess
import time
from typing import Optional

import polars as pl

from .const import ID_DB, DB_DIR, TIMEOUT

logging.basicConfig(
    level=logging.INFO,
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


class CryoETDB:

    def __init__(self, id: int, filename: str, db_dir: str | Path) -> None:
        self.id: int = id
        self.id_database: str = filename
        self.parse_ids()
        self.user: str = self.get_user()
        self.initials: str = self.get_initials()
        if isinstance(db_dir, str):
            self.db_dir: Path = Path(db_dir)
        else:
            self.db_dir: Path = db_dir
        self.completed: dict[Optional[str], Optional[str]] = {}
        self.not_processed: Optional[list[str]] = []
        self.logger = logging.getLogger(__name__)

    
    def __repr__(self) -> str:
        return f"ID: {self.id}, User: {self.user}"
    

    def parse_ids(self) -> None:
        """ Get the Database IDs for each user. """
        self.all_ids: pl.DataFrame = pl.read_csv(self.id_database, truncate_ragged_lines=True)

    
    def get_user(self) -> str:
        """ Get the user based on the databse ID """
        return [row["name"] for row in self.all_ids.iter_rows(named=True)
                if row["id"] == self.id][0]
    

    def get_initials(self) -> str:
        """ Get initials from the user """
        return "".join(x[0].lower() for x in self.user.split())
    

    def get_procdirs(self, proc_dir: str | Path) -> list[Path]:
        """ Get processing directories to incorporate into the database """
        return [x for x in proc_dir.iterdir() if x.is_dir and list(x.glob(EXT))]
    

    def get_mdocs(self, dirs: list[Path]) -> list[Path]:
        """ Get mdoc files for each processing directory """
        return [x for d in dirs for x in list(d.glob('*.mdoc'))]
    

    def get_mdoc_dates(self, mdocs: list[Path]) -> list[str]:
        """ Get dataset dates from the mdocs, based on the Jensen Lab / Chang Lab pipelines """
        return [time.strftime('%Y-%m-%d', time.gmtime(os.path.getmtime(m))) for m in mdocs]
    

    def search_db_identicals(self, entry_ids: list[str]) -> list[int]:
        """ Search database for given initial/date ID, return number currently existing """
        # Initially do this without SQL -> Just scan the database directory for matching IDs
        return {
            e: len(list(self.db_dir.glob(f'{e}*'))) 
            for e in entry_ids
        }


    def set_logger(
            self, 
            filename: str, 
            name: Optional[str]=None, 
            level: int=logging.INFO, 
            head: bool=False
    ) -> Optional[logging.Logger]:
        """ Set the log handler to log to the appropriate file """
        assert filename[-4:] == '.log'

        if not Path(filename).exists():
            handler = logging.FileHandler(filename)
            handler.setFormatter(formatter)
            if head is True:
                self.logger.setLevel(level)
                self.logger.addHandler(handler)
            else:
                logger = logging.getLogger(name)
                logger.setLevel(level)
                logger.addHandler(handler)

                return logger


    def initialize_datasets(self, proc_dir: str | Path) -> pl.DataFrame:
        """ Setup a Polars DataFrame containing each processing directory, the mdoc, and new database ID """
        if isinstance(proc_dir, str):
            proc_dir = Path(proc_dir)
        if not proc_dir.exists():
            raise ValueError("Given processing directory does not exist")
        
        if not self.logger.handlers:
            filename = f'{time.strftime("%Y%m%d_%H%M", time.localtime())}_TRANSFERS.log'
            self.set_logger(f'{Path.cwd() / proc_dir}/{filename}', head=True)

        # Get processing directories and mdocs
        dirs: list[Path] = self.get_procdirs(proc_dir=proc_dir)
        mdocs: list[Path] = self.get_mdocs(dirs=dirs)

        # Get dataset names, mdoc names, and dates to assemble into DataFrame
        dates: list[str] = self.get_mdoc_dates(mdocs)
        dataset_names: list[str] = [d.name.split('/')[-1] for d in dirs]
        mdoc_names: list[str] = [m.name.split('/')[-1] for m in mdocs]

        # Get current Database entries for these initials and year
        all_entry_ids = [f"{self.initials}{date}" for date in dates]
        unique_entry_ids = list(set(all_entry_ids))
        num_entries: dict[str, int] = self.search_db_identicals(entry_ids=unique_entry_ids)

        for entry_id in num_entries:
            for i, e in enumerate(all_entry_ids, num_entries[entry_id]):
                all_entry_ids[i-num_entries[entry_id]] = f"{e}-{i+1}" 

        assert len(dates) == len(dataset_names) == len(mdoc_names) == len(all_entry_ids)

        self.logger.info('%d processing datasets' % len(dataset_names))

        self.df: pl.DataFrame = pl.LazyFrame({
            "id": all_entry_ids,
            "dataset": sorted(dataset_names),
            "mdoc": sorted(mdoc_names),
            "date": dates
        }).collect()  # Returns a DataFrame


    def transfer(self, proc_dir: str | Path, subframe_path: Optional[str | Path]=None) -> int:
        """
        Transfer processing directores to the database location when completed
        Then move processing directories to 'Done' subdirectory
        
        :return: int (0 for success, 1 for error -> Check ChangLab pipeline errors)
        """
        if isinstance(proc_dir, str):
            proc_dir = Path(proc_dir)
        if not proc_dir.exists():
            raise ValueError(f"Processing directory {proc_dir} does not exist")

        done_dir = proc_dir / 'Done'
        cmd = f"mkdir -p {done_dir}"
        subprocess.run(cmd, shell=True)

        print(self.df)
        
        # Iterate over each completed dataset, first transferring the frames and
        # processed data to the database location, then moving to the 'Done' subdir
        for dataset in sorted(self.completed):
            print(f"DATASET: {dataset}")
            try:
                row = self.df.row(
                    by_predicate=(pl.col('dataset') == dataset),
                    named=True
                )
                print(row)
            except pl.exceptions.NoRowsReturnedError:
                self.not_processed.append(dataset)
                continue

            self.dataset_logger = self.set_logger(
                filename=f'{Path.cwd() / proc_dir}/{time.strftime("%Y%m%d_%H%M", time.localtime())}_{row["dataset"]}-{row["id"]}_transfer.log',
                name=f'{row["id"]}',
            )
            
            if DOSE_FRACTIONS:  # equals 1
                if subframe_path:
                    if isinstance(subframe_path, str):
                        subframe_path = Path(subframe_path)
                    
                    # Fetch the mdoc and construct the transfer command
                    mdoc = proc_dir / Path(f"{row['dataset']}/{row['mdoc']}")
                    transfer_path_frames = Path(f"{self.db_dir}/{row['id']}/Frames")
                    cmd = f"mkdir -p {transfer_path_frames}" # Here, the mdoc is already in the processing directory for this dataset. May need to change this

                    # Transfer the raw frames first for this dataset
                    cmd_frames = self._transfer_rawframes(
                        subframe_path=subframe_path, 
                        transfer_path=transfer_path_frames,
                        mdoc=mdoc,
                        cmd=cmd
                    )
                    self.dataset_logger.info('TRANSFERRING FRAMES FOR %s -> %s' % (row['dataset'], row['id']))
                    print(f"TRANSFERRING FRAMES FOR {row['dataset']} -> {row['id']}")
                    proc_frames = subprocess.Popen(cmd_frames, shell=True)
                else:
                    self.dataset_logger.warning('NO SUBFRAME PATH. FRAMES WILL NOT BE TRANSFERRED TO THE DATABASE')

            # Transfer the dataset
            transfer_path_set = Path(f"{self.db_dir}/{row['id']}")
            dataset_dir = proc_dir / Path(f"{row['dataset']}")
            cmd_dataset = self._transfer_dataset(
                transfer_path=transfer_path_set,
                dataset_dir=dataset_dir
            )
            self.dataset_logger.info('TRANSFERRING DATASET FOR %s -> %s' % (row['dataset'], row['id']))
            proc_dataset = subprocess.Popen(cmd_dataset, shell=True)

            # Wait for transferring of frames and dataset to complete before going to next dataset
            if DOSE_FRACTIONS and subframe_path:
                proc_frames.communicate()
                if (exit_code := proc_frames.returncode) != 0:
                    self.dataset_logger.error('ERROR transferring frames for %s' % row['id'])
                    self.dataset_logger.error('ERROR CODE %d' % exit_code)

            proc_dataset.communicate()
            if (exit_code := proc_dataset.returncode) != 0:
                self.dataset_logger.error('ERROR transferring processed data for %s' % row['id'])
                self.dataset_logger.error('ERROR CODE %d' % exit_code)
            
            swbrt_log = f"{proc_dir}/swbrt_{row['dataset']}*.log"
            cmd = f"mv {dataset_dir} {swbrt_log} {done_dir}"
            self.dataset_logger.info('MOVING DATASET %s TO %s' % (row['dataset'], done_dir))
            if (exit_code := subprocess.run(cmd, shell=True).returncode) != 0:
                self.dataset_logger.error('ERROR moving dataset %s to %s' % (row['dataset'], done_dir))
                self.dataset_logger.error('ERROR CODE %d' % exit_code)

    
    def _transfer_rawframes(
            self,
            subframe_path: Path,
            transfer_path: Path,
            mdoc: Path,
            cmd: str
    ) -> str:
        """
        Get the raw frames from the mdocs and transfer them to a rawframes subdirectory 
        :return: str - command for transferring a dataset's raw frames
        """
        if not subframe_path.exists():
            raise ValueError(f"Subframe directory {subframe_path} does not exist")

        if (exit_code := subprocess.run(cmd, shell=True).returncode) != 0:
            self.dataset_logger.error('ERROR creating frames directory %s/Frames in the database' % transfer_path)
            self.dataset_logger.error('Error code %d' % exit_code)
            return exit_code  

        # Get frames from mdoc
        with open(mdoc, 'r') as f:
            files = []
            tiffmdocs = []
            for line in f:
                if 'SubFramePath' in line:
                    strip_line = line.rstrip()
                    file = strip_line.split('\\')[-1]
                    ext = file.split('.')[-1]
                    if ext == 'mrc':
                        mrcfile = f"{subframe_path}/{file}"
                        cmd = f"rsync -a --ignore-existing {mrcfile} {transfer_path}"

                        files.append(mrcfile)
                    elif ext in ['tiff', 'tif']:
                        tiff = f"{subframe_path}/{file}"
                        tiff_mdoc = f"{file}.mdoc"

                        try:
                            with open(tiff_mdoc, 'r') as f:
                                cmd = f"rsync --ignore-existing -a {tiff} {tiff_mdoc} {transfer_path}"
                                tiffmdocs.append(tiff_mdoc)
                        except FileNotFoundError:
                                pass
                        finally:
                            files.append(tiff)
                    else:
                        self.dataset_logger.error('ERROR: Do not recognize frames file extenstion .%s' % ext)
                        return 1
                    
            if not tiffmdocs:
                self.dataset_logger.warning('No TIFF mdocs found in %s. If there is just one mdoc for the tilt series, no need to worry' % subframe_path)
                    
            cmd = f"rsync --ignore-existing -a {' '.join(file for file in files)} {' '.join(file for file in tiffmdocs)} {transfer_path}"
            return cmd


    def _transfer_dataset(
            self,
            transfer_path: Path,
            dataset_dir: Path
    ) -> str:
        """
        Transfer dataset with key files to the database directory
        Files to exclude:
            *~
            *.log*
            *.com*
            *.adoc
            dfltcoms/
            origcoms/
            *_ali.mrc (the final aligned)
            *_preali.mrc
            *_full_rec.mrc

        :return: str - command for transferring processing dataset
        """
        if not dataset_dir.exists():
            raise ValueError(f"Dataset directory {dataset_dir} does not exist")
        
        # Special case: Check for two *_ali.mrc files, because alignframes adds _ali.mrc onto motion corrected dataset
        # If there are 2 -> get *_ali_ali.mrc
        ali_files: list[str] = list(dataset_dir.glob('*_ali.mrc'))
        ali: str = max(ali_files).name if len(ali_files) > 1 else ""
        
        if ali  == "":
            cmd = f"rsync --ignore-existing -a --exclude={{'*~','*.log*','*.adoc','dfltcoms','origcoms','*_preali.mrc','*_full_rec.mrc','*.out'}} {dataset_dir}/ {transfer_path}"
        else:
            cmd = f"rsync --ignore-existing -a --exclude={{'*~','*.log*','*.adoc','dfltcoms','origcoms','{ali}','*_preali.mrc','*_full_rec.mrc','*.out'}} {dataset_dir}/ {transfer_path}"
        return cmd
    

    def watch_for_completion(self, proc_dir: str | Path) -> int:
        """
        Watch for completed processing directories 
        Move to 'Done' subdirectory when completed and when done being transferred
            run self.transfers() -> This will go until the rsync to the db is done
            then run mv()

        For now, only works when running with IMOD serieswatcher

        :return: int, 1 if there are log files, 0 if no log files
        """
        if isinstance(proc_dir, str):
            proc_dir = Path(proc_dir)
        if not proc_dir.exists():
            raise ValueError(f"Processing directory {proc_dir} does not exist")
        
        # Get logs
        sw_logs = list(proc_dir.glob("swbrt*.log"))

        # If no logs detected, return 0
        if not sw_logs:
            return 0
        
        # Open each log, check for competion, add to self.completed if dataset is completed
        completed_success = 0
        completed_error = 0
        for logfile in sw_logs:
            # Keep track of number of ERROR, ABORT, and SUCCESSFULLY COMPLETED
            num_error, num_abort, num_success = 0, 0, 0
            with open(logfile, 'r') as f:
                for line in f:
                    strip_line = line.rstrip()
                    if "ERROR" in strip_line:
                        num_error += 1
                    if "ABORT" in strip_line:
                        num_abort += 1
                    if "SUCCESSFULLY COMPLETED" in strip_line:
                        num_success += 1
                
                # Determine if processing is ongoing, terminated early, or successfully completed
                # 0 for success, -1 for error, 1 for ongoing
                status: int = (
                    -1 if num_abort > 1 or num_error > 0 
                    else 0 if num_success == 1 
                    else 1 
                )
                
                if status < 1:
                    basename = f.name.split('swbrt_')[-1].split('.')[0]
                    self.completed[basename] = 'completed' if status == 0 else 'error'
                    if self.completed[basename] == 'completed':
                        completed_success += 1
                    elif self.completed[basename] == 'error':
                        completed_error += 1
        self.logger.info(
            '%d processing directories: %d completed SUCCESSFULLY, %d completed with ERROR, %d ONGOING' % (
                len(sw_logs), 
                completed_success, 
                completed_error, 
                len(sw_logs)-completed_success-completed_error
            )
        )
        return 1


if __name__ == '__main__':
    if len(sys.argv) < 6:
        raise ValueError("Not enough arguments passed to DB_TRANSFER")
    PROC_DIR = Path(sys.argv[1])
    SUBFRAME_PATH = Path(sys.argv[2])  # This will be setup by shell script
    EXT = sys.argv[3]
    ID = int(sys.argv[4])
    DOSE_FRACTIONS = int(sys.argv[5])
    assert DOSE_FRACTIONS in [0, 1]

    if not PROC_DIR.is_dir():
        raise ValueError(f"{PROC_DIR} is not a valid directory.")
    if not SUBFRAME_PATH.is_dir():
        raise ValueError(f"{SUBFRAME_PATH} is not a valid directory.")

    db = CryoETDB(id=ID, filename=ID_DB, db_dir=DB_DIR)

    start = time.time()
    while True:
        db.initialize_datasets(PROC_DIR)
        new_procs: int = db.watch_for_completion(PROC_DIR)

        print(f'NEW PROCS: {new_procs}')

        if new_procs:
            db.transfer(PROC_DIR, SUBFRAME_PATH)
            start = time.time()  # Reset the timer

        end = time.time()
        if (end - start) > TIMEOUT:
            fw_pipeline = 'fw_pipeline'
            brt_pipeline = 'brt_pipeline'
            transfer_pipeline = 'db_pipeline'
            
            cmd = f'tmux kill-session -t {fw_pipeline}'  # Recontruction and transfer being run in a tmux session
            subprocess.run(cmd, shell=True)
            cmd = f'tmux kill-session -t {brt_pipeline}'
            subprocess.run(cmd, shell=True)
            cmd = f'tmux kill-session -t {transfer_pipeline}'
            subprocess.run(cmd, shell=True)

            break
        time.sleep(60)
