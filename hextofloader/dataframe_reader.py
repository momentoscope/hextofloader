"""
Interface class for preprocessing data from any defined source.
Currently, this works for Flash and Lab data sources.
"""
import yaml

from hextofloader.data_sources.flash import flash
from hextofloader.data_sources.lab import lab

class readData():
    """
    The class inherits attributes from the FlashLoader and LabLoader classes
    and calls them depending on predefined source value in config file.
    The dataframe is stored in self.dd.
    """

    def __init__(self, config, fileNames=None):
        # Parse the source value to choose the necessary class
        with open(config) as file:
            config_ = yaml.load_all(file, Loader=yaml.FullLoader)
            for doc in config_:
                if "general" in doc.keys():
                    self.source = doc["general"]["source"]

        sourceClassName=globals()[self.source]

        sourceClass=sourceClassName(fileNames, config)
        sourceClass.readData()

        self.dd = sourceClass.dd

        self.sourceClass = sourceClass # debugging purposes
