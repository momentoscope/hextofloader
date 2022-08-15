"""
An abstract class for a set of methods to read data from source. 
Common funtionality is implemented here, whereas the 
abstract methods exist so as to serve implementation
in the subclasses depending on source specific requirements.
"""
from abc import ABC, abstractmethod
from hextofloader.config_parser import configParser


class sourceReader(ABC, configParser):
    @abstractmethod
    def __init__(self, fileNames, config) -> None:
        super().__init__(config)
        self.fileNames = fileNames
        self.channels = self.availableChannels

    @property
    @abstractmethod
    def availableChannels(self):
        """Returns the channel names that are available for use,
        defined by the json file"""
        return list(self.all_channels.keys())

    @abstractmethod
    def parseH5Keys(self, h5_file, prefix=""):
        """Helper method which prases the channels present in the h5 file"""
        fileChannelList = []

        for key in h5_file.keys():
            try:
                [
                    fileChannelList.append(s)
                    for s in self.parseH5Keys(h5_file[key], prefix=prefix + "/" + key)
                ]
            except Exception:
                fileChannelList.append(prefix + "/" + key)

        return fileChannelList
