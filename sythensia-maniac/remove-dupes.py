#!/usr/bin/env python3

import os
from hashlib import md5

CHECKER_INSTANCES = 4
MIDI_ROOT_DIR = os.path.abspath('/mnt/media/Music/goonMidi')

def main():
    hashes = map_hashes(MIDI_ROOT_DIR)
    for file in os.listdir(MIDI_ROOT_DIR):
        full_path = os.path.abspath(os.path.join(MIDI_ROOT_DIR, file))
        if os.path.isfile(full_path) and os.path.splitext(file)[1].lower() == '.mid':
            with open(full_path, 'rb') as infile:
                md5sum = md5(infile.read()).hexdigest()
            if md5sum in hashes:
                print(f'Removing {full_path}')
                os.remove(full_path)

def map_hashes(root_dir=MIDI_ROOT_DIR):
    hashes=[]
    subdirs = []
    for file in os.listdir(root_dir):
        full_path = os.path.abspath(os.path.join(root_dir, file))
        if os.path.isdir(full_path):
            subdirs.append(full_path)
    
    def recurse_subdir(subdir):
        for file in os.listdir(subdir):
            full_path = os.path.abspath(os.path.join(root_dir, file))
            if os.path.isdir(full_path):
                recurse_subdir(full_path)
            elif os.path.isfile(full_path) and os.path.splitext(file)[1].lower() == '.mid':
                with open(full_path, 'rb') as infile:
                    hashes.append(md5(infile.read()).hexdigest())
    
    for subdir in subdirs:
        recurse_subdir(subdir)

    return hashes

if __name__ == '__main__':
    main()