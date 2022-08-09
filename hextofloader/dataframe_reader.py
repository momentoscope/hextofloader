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

    def __init__(self, config, runNumbers=None, fileNames=None):
        # Parse the source value to choose the necessary class
        with open(config) as file:
            config_ = yaml.load_all(file, Loader=yaml.FullLoader)
            for doc in config_:
                if "general" in doc.keys():
                    self.source = doc["general"]["source"]

        sourceClassName=globals()[self.source]

        if self.source == 'lab':
            if fileNames is None:
                raise ValueError("Must provide a file name or a list of file names")
            if runNumbers:
                raise ValueError("runNumber is not valid for Lab data. Please provide fileNames.")
        
        if self.source == 'flash':
            if runNumbers is None:
                raise ValueError("Must provide a run or a list of runs")
            if fileNames:
                raise ValueError("fileNames not valid for Flash data. Please provide runNumber.")
        data = [data for data in [runNumbers, fileNames] if data]
        sourceClass=sourceClassName(data, config)
        sourceClass.readData()

        self.dd = sourceClass.dd
