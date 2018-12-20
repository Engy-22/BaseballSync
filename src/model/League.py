from File import File
from Translator import Translator

class League:

    def __init__(self, homeTeam, year):
        self.__homeTeam = homeTeam
        self.getLeague(year)

    def getLeague(self, year):
        file = open(File.teamYearInfo(year))
        for line in file:
            tempLine = line.split(",")
            if (self.__homeTeam == tempLine[0]):
                self.__league = Translator.abvToLeague(tempLine[1])
                break

    def __str__(self):
        return (self.__league)
