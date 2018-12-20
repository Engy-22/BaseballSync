from Model.DB_Connect import DB_Connect
import web.py

class Player:
    
    def __init__(self, player_id: str, team_id: str, year: int):
        self.player_id = player_id
        self.year = year
        self.primary_position = self.import_primary_position(player_id, team_id)
        self.secondary_positions = []
        self.throws_with = ""
        self.bats_with = ""
        self.battingStats = {}
        self.pitchingStats = {}
        self.fieldingStats = {}
        self.imageURL = self.create_image_url(player_id)

    def import_primary_position(self, player_id, team_id, year="ALL"):
        db, cursor = DB_Connect.grab("baseballData")
        if year == "ALL":
            position = DB_Connect.read(cursor, 'select primaryPosition from players where playerId = "' + player_id
                                               + '";')[0][0]
        else:
            position = DB_Connect.read(cursor, 'select positions from player_positions where playerId = ' + player_id
                                               + ' and TY_uniqueidentifier = (select TY_uniqueidentifier from '
                                               + 'team_years where teamId = ' + team_id + ' and year = ' + str(year)
                                               + ');')[0][0]
        return position

### Getters ###
    def get_player_id(self):
        return self.player_id

    def get_year(self):
        return self.year

    def get_primary_position(self):
        return self.primary_position

    def get_image_url(self):
        return 'images/players/' + self.player_id
### End Getters ###

#     def getSecondaryPositions(self):
#         return self.secondaryPositions
#
#     def getThrowHandedness(self):
#         return self.throwsWith
#
#     def getBatHandedness(self):
#         return self.batsWith
#
#     def getBattingStats(self, stat):
#         if stat == "ALL":
#             return self.battingStats
#         else:
#             return self.battingStats[stat]
#
#     def getPitchingStats(self, stat):
#         if stat == "ALL":
#             return self.pitchingStats
#         else:
#             return self.pitchingStats[stat]
#
#     def getFieldingStats(self, stat):
#         if stat == "ALL":
#             return self.fieldingStats
#         else:
#             return self.fieldingStats[stat]
#
#     def getPlayerImage(self):
#         return self.imageURL
