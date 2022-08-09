import os
from pathlib import Path
from functools import reduce
from multiprocessing import Pool, cpu_count
from itertools import compress
import numpy as np
from pandas import Series, DataFrame, concat
import dask.dataframe as dd
import h5py
from hextofloader.config_parser import configParser

class lab(configParser):
    def __init__(self, fileNames, config) -> None:
        super().__init__(config)
        self.channels = self.availableChannels
        self.filenames = fileNames
        
    @property
    def availableChannels(self):
        """Returns the channel names that are available for use, defined by the json file"""
        available_channels = list(self.all_channels.keys())
        return available_channels

    def parseH5Keys(self, h5_file, prefix=''):
        fileChannelList = []

        for key in h5_file.keys():
            try:
                [fileChannelList.append(s) for s in self.parseH5Keys(h5_file[key], prefix=prefix + '/' + key)]
            except:
                fileChannelList.append(prefix + '/' + key)

        return fileChannelList
        
    def createDataframePerFormat(self, h5_file, format_):
        valid_names = [each_name
            for each_name in self.channels
            if each_name in self.all_channels]  # filters for valid channels

        if format_ is not None:
            channels = [each_name
                for each_name in valid_names
                if self.all_channels[each_name]["format"] == format_]
        else:
            channels = [each_name for each_name in valid_names]
            
        if format_ == "slow":
            electronID = np.cumsum([0,*h5_file['DLD/NumOfEvents'][:-1]])
            
        elif format_ == "electron":
            if 'time' in self.all_channels:
                timeGroupName = self.all_channels['time']["group_name"]
                electronID = np.arange(len(h5_file[timeGroupName]))
                print(timeGroupName + ' channel was used')
            else:
                try:
                    electronID = np.arange(len(h5_file['DLD/DLD/dldTime']))
                    print('DLD/DLD/dldTime channel was used')
                except:
                    electronID = np.arange(len(h5_file['DLD/DLD/times']))
                    print('DLD/DLD/times channel was used')

        fileChannelList = self.parseH5Keys(h5_file)
        channelsNotPresent = []
        channelsPresent = []
        for channel in channels:
            channelName = self.all_channels[channel]['group_name']
            if channelName not in fileChannelList:
                channelsNotPresent.append(channel)
            else:
                channelsPresent.append(channel)
        if len(channelsNotPresent) > 0:
            print(f"WARNING: skipped channels missing in h5 file: {[self.all_channels[channel]['group_name'] for channel in channelsNotPresent]}")

        dataframes = (Series(h5_file[self.all_channels[channel]['group_name']], 
                        name = channel, 
                        index = electronID)
            .to_frame() for channel in channelsPresent)

        return reduce(DataFrame.combine_first, dataframes)
    
    def readFile(self, filename):
        with h5py.File(filename) as h5_file:
            dataframe = concat((self.createDataframePerFormat(h5_file, format_) 
                                for format_ in ['slow', 'electron']), axis=1)
        return dataframe
    
    def h5_to_parquet(self, h5, prq):
        try:
            (self.readFile(h5).reset_index()
            .to_parquet(prq, compression = None, index = False))
        except ValueError as e:
            self.failed_str.append(f'{prq}: {e}')
            self.prq_names.remove(prq)

    def fillNA(self):
        """Routine to fill the NaN values with intrafile forward filling. """
        # First use forward filling method to fill each file's pulse resolved channels.
        for i in range(len(self.dfs)):
            self.dfs[i] = self.dfs[i].fillna(method="ffill")

        # This loop forward fills between the consective files. 
        # The first run file will have NaNs, unless another run before it has been defined.
        for i in range(1, len(self.dfs)):
            subset = self.dfs[i]
            is_null = subset.loc[0].isnull().values.compute() # Find which column(s) contain NaNs.
            # Statement executed if there is more than one NaN value in the first row from all columns
            if is_null.sum() > 0: 
                # Select channel names with only NaNs
                channels_to_overwrite = list(is_null[0])
                # Get the values for those channels from previous file
                values = self.dfs[i-1].tail(1).values[0]
                # Fill all NaNs by those values
                subset[channels_to_overwrite] = subset[channels_to_overwrite].fillna(
                                                                dict(zip(channels_to_overwrite, values)))
                # Overwrite the dataframes with filled dataframes
                self.dfs[i] = subset

    def readData(self):
        # prepare a list of names for the files to read and parquets to write
        try: 
            files_str = f'Files {self.filenames[0]} to {self.filenames[-1]}'
        except TypeError:
            files_str = f'File {self.filenames}'
            self.filenames = [self.filenames]

        self.prq_names = [f'{self.DATA_PARQUET_DIR}/{filename}' for filename in self.filenames]
        self.filenames = [f'{self.DATA_RAW_DIR}/{filename}.h5' for filename in self.filenames]

        missing_files = []
        missing_prq_names = []
        # only read and write files which were not read already 
        for i in range(len(self.prq_names)): 
            if not Path(self.prq_names[i]).exists():
                missing_files.append(self.filenames[i])
                missing_prq_names.append(self.prq_names[i])
        
        print(f'Reading {files_str}: {len(missing_files)} new files of {len(self.filenames)} total.')
        self.failed_str = []

        # Set cores for multiprocessing
        N_CORES = len(missing_files)
        if N_CORES > cpu_count() - 1:
            N_CORES = cpu_count() - 1
        
        # Read missing files using multiple cores
        if len(missing_files) > 0:
            with Pool(processes=N_CORES) as pool:    
                pool.starmap(self.h5_to_parquet, tuple(zip(missing_files, missing_prq_names)))
                            
        if len(self.failed_str)>0:
            print(f'Failed reading {len(self.failed_str)} files of{len(self.filenames)}:')
            for s in self.failed_str:
                print(f'\t- {s}')
        if len(self.prq_names)==0:
            raise ValueError('No data available. Probably failed reading all h5 files')
        else:
            print(f'Loading {len(self.prq_names)} dataframes. Failed reading {len(self.filenames)-len(self.prq_names)} files.')  
            self.dfs = [dd.read_parquet(fn) for fn in self.prq_names]
            self.fillNA()

        self.dd  = dd.concat(self.dfs).repartition(npartitions=len(self.prq_names))
