from utilities.connections.baseball_data_connection import DatabaseConnection


class Team:

    def __init__(self, team_id: str, year: int):
        self.teamId = team_id
        self.year = year
        self.roster = []
        self.wins = 0
        self.loses = 0
        self.offRank_year = None
        self.defRank_year = None
        self.ovrRank_year = None
        self.offRank_ovr = None
        self.defRank_ovr = None
        self.ovrRank_ovr = None
        self.imageURL = ""


    def addPlayerToRoster(self, playerId):
        self.roster.append(playerId)

### Getters ###
    def getTeamId(self):
        return self.teamId


    def getYear(self):
        return self.year


    def getRoster(self):
        return self.roster


    def getWins(self):
        return self.wins


    def getLoses(self):
        return self.loses


    def getOffRank_year(self):
        return self.offRank_year


    def getDefRank_year(self):
        return self.defRank_year


    def getOvrRank_year(self):
        return self.ovrRank_year


    def getOffRank_ovr(self):
        return self.offRank_ovr


    def getDefRank_ovr(self):
        return self.defRank_ovr


    def getOvrRank_ovr(self):
        return self.ovrRank_ovr


    def getTeamImage(self):
        return self.imageURL
### Getters ###

### SETTERS ###
    def setRoster(self, sandbox_mode):
        db = DatabaseConnection(sandbox_mode)
        team_id = db.read("select TY_uniqueidentifier from team_years where teamId = '" + self.teamId + "' and year = "
                          + str(self.year) + ";")
        print(team_id)
        #data = db.read("select playerId from playerPositions where TY_uniqueidentifier = " + str(team_id))
### SETTERS ###


# team = Team("CLE", 2017)
# team.setRoster(True)
