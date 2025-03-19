# Matthew Martinez
# Sanofi - US Dept. of Large Molecules Research, Protein Engineering Group, Structural Biology
# 
# Helper functions

import contextlib
import curses
from itertools import zip_longest
import os
from pathlib import Path
import sys
import time
from typing import Generator


@contextlib.contextmanager
def chdir(path: str | Path) -> Generator[None, None, None]:
    """ Changed working directory and returns to the previous on exit """
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)

         
def typewriter(strings: list[str], delay=0.05):
    max_length = max(len(s) for s in strings)

    # Print empty lines for each string (to reserve space)
    for _ in strings:
        sys.stdout.write("\n")
    
    for i in range(max_length):
        sys.stdout.write("\033[F" * len(strings))  # Move cursor up to overwrite

        for s in strings:
            sys.stdout.write(s[:i + 1] + "\n")  # Print progressively longer substring
        
        sys.stdout.flush()
        time.sleep(delay)



def get_mdoc(p: Path) -> Path:
    """ Get mdoc files for each processing directory """
    while True: 
        mdocs = [x for x in list(p.rglob('*.mdoc'))]
        if mdocs:
            break
        time.sleep(60)
    return mdocs[0]


def read_mdoc(mdoc: Path) -> dict[str, any]:
    """ 
    Get additional information from mdocs, including:
        mag, pixel size, defocus, tilt angles, tilt increment

    For every mrc file, return a dict like:
    mdoc_dict = {
        "mag": [int] mag,
        "pixel size": [float] pixSize,
        "defocus": [list[float]] defocus,
        "defocus avg": [float] avg_defocus,
        "tilt angles": [list[float]] [tilt angles],
        "tilt min": [float] tilt_min,
        "tilt max": [float] tilt_max
        "tilt increment": [int] tilt increment
    }
    """
    assert mdoc.exists()

    header_info = {}
    # Open each file and extract the relevant information
    with open(mdoc, 'r') as f:
        header_info['Tilt Angles'] = []
        header_info['Defocus'] = []
        for line in f:
            strip_line = line.rstrip()
            if 'TiltAngle' in strip_line:
                index = strip_line.find('=')
                angle = round(float(strip_line[index+2:]))
                header_info['Tilt Angles'].append(angle)
            if 'Defocus' in strip_line and 'Target' not in strip_line:
                index = strip_line.find('=')
                header_info['Defocus'].append(float(strip_line[index+2:]))
            if 'Magnification' in strip_line:
                index = strip_line.find('=')
                header_info['Magnification'] = strip_line[index+2:]
            if 'PixelSpacing' in strip_line:
                index = strip_line.find('=')
                header_info['Pixel Size'] = str(round(float(strip_line[index+2:]),2)/10)

    header_info['Tilt Min'] = min(header_info['Tilt Angles'])
    header_info['Tilt Max'] = max(header_info['Tilt Angles'])
    header_info['Tilt Step'] = round(abs(
        (header_info['Tilt Max'] - header_info['Tilt Min'])
        / len(header_info['Tilt Angles'])
    )) 
    header_info['Defocus Avg'] = round(
        sum(header_info['Defocus']) / float(
            len(header_info['Defocus'])
        ), 2)
    
    return header_info