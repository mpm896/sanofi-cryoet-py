# Matthew Martinez
# Sanofi - US Dept. of Large Molecules Research, Protein Engineering Group, Structural Biology
#
# Constants for the pipeline, such as the database directory, etc.

DB_DIR = "/root/cloud-data/its-cmo-darwin-magellan-workspaces-folders/WS_Cryoem/CX_LMR/Project_directories/cryo-et-data"
ID_DB = "/root/cloud-data/its-cmo-darwin-magellan-workspaces-folders/WS_Cryoem/CX_LMR/Project_directories/cryo-et-pipeline/ids.csv"
PROC_DIR = 'Aligned'
TIMEOUT = 3600  # Time until time out, in seconds
EXT = '*_rec.mrc'  # Suffix of completed tomogram
BIN = 6  # Bin factor
GPU = 0  # Number GPU device - 0 for best

CONFIG_ARGS = {
    'setup': ['CPUS', 'GPUS', 'PIPE_CLI', 'SOFTWARE', 'TILTAXIS', 'USER_DB_ID'],
    'setup_data': ['FRAMES_NAME', 'GAIN_PATH', 'MDOC_DUPLICATE', 'RAW_DATA_DIR', 'READ_MDOC', 'TRANFER_RAW_DATA',],
    'data': ['EXPOSURE', 'EXTENSION', 'PIXEL_SIZE'],
    'mc': ['DOSE_FRACTIONS', 'DO_MC_DOSEWEIGHT', 'DROP_MEAN', 'RUN_FRAMEWATCHER'],
    'imod': ['PREALIGN_BIN', 'REMOVE_XRAYS'],
    'imod_tracking': ['SIZE_GOLD', 'TRACK_METHOD'],
    'imod_tracking_fiducial': ['NUM_BEADS', 'SOBEL_SIGMA', 'USE_SOBEL'], 
    'imod_tracking_patch': ['PATCH_OVERLAP_X', 'PATCH_OVERLAP_Y', 'PATCH_SIZE_X', 'PATCH_SIZE_Y'],  
    'imod_final_alignment': ['DO_CTF', 'DO_DOSE_WEIGHTING ', 'FINAL_BIN',],
    'imod_ctf': ['AUTOFIT_RANGE', 'AUTOFIT_STEP', 'CS', 'DEFOCUS_RANGE_HIGH', 'DEFOCUS_RANGE_LOW', 'TUNE_FITTING_SAMPLING', 'VOLTAGE',],
    'imod_dose_weight': ['DOSE_SYM'],
    'imod_reconstruction': ['FAKE_SIRT_ITERS', 'RECONSTRUCT_METHOD', 'SIRT_ITERS', 'THICKNESS_BINNED', 'THICKNESS_UNBINNED',],
    'imod_postprocess': ['DO_TRIMVOL', 'REORIENT'],
    'denoise': ['DO_DENOISING']
}