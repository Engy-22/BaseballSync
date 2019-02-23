from model.teams.lineup_creator.get_starting_pitcher import get_starting_pitcher
from model.teams.lineup_creator.reorganize_data import reorganize_batting_spots
import random


def create_lineup(team_id, year, roster, game_num, league):
    starting_pitcher = get_starting_pitcher(team_id, year, game_num)
    batting_order = []
    print('\n')
    for place in range(9):
        batting_order.append(get_player_randomly(roster, place+1))
        print(batting_order[-1].get_player_id())


def get_player_randomly(players_starts, spot):
    options = reorganize_batting_spots(players_starts, spot)
    games = 0
    for player, starts in options.items():
        games += starts
    picker = random.randint(1, games)
    temp_count = 0
    for player, starts in options.items():
        if picker <= temp_count + starts:
            return player
        else:
            temp_count += starts

# class LineupCreator:
#
#     def __init__(self, team, year, gameNumber, league):
#         self.setLineup(self.controller(index, team, year, gameNumber, str(league), players, starts, startingPitcher))
#
#     def controller(self, index, team, year, gameNumber, league, players, starts, startingPitcher):
#         place = 0
#         batters = []
#         positions = []
#         while place <= 8:
#             batter = self.getPlayerRandomly(players[place], starts[place])
#             tempBatters = self.ensurePlayerUniqueness(place, batter, batters, players)
#             if (tempBatters != "restart"):
#                 batters = tempBatters
#             else:
#                 #print ("restart")
#                 return (self.controller(index, team, year, gameNumber, league, players, starts, startingPitcher))
#             if (place == 8 and "SP" not in positions and (self.__homeYear < 1973 or league == "National League")): #if it has come to the last spot in the order of a lineup under National League rules and the pitcher has not yet been add.
#                 position = "SP" #the next player must be a pitcher
#             else:
#                 #print ("is " + batters[place]  + " in")
#                 #print (self.__addedPlayers[index])
#                 if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
#                     #print ("no\n")
#                     position = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), 1, league, self.getShowFunctions()) #get their primary position
#                 else: #if this is an added player to the team
#                     #print ("yes\n")
#                     for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
#                         if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
#                             fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
#                             break
#                         else: #if this player is not the player in question
#                             continue #increment
#                     position = Importer.getPositionIncrementally(batters[place], fieldingFile, 1, league, self.getShowFunctions())
#             batters, positions = self.ensurePositionUniqueness(index, 2, position, place, players, self.getBattingFile(), self.getFieldingFile(), league, batters, positions, year)
#             if not (batters == "restart" and positions == "restart"):
#                 place += 1
#             else:
#                 #print ("restart")
#                 return (self.controller(index, team, year, gameNumber, league, players, starts, startingPitcher))
#         if (self.__homeYear < 1973 or league == "National League"):
#             batters = self.addStartingPitcherToLineup(batters, positions, startingPitcher)
#         else:
#             pass
#         output = []
#         output.append(batters)
#         output.append(positions)
#         return (output)
#
#     def getPlayers(self, index, team, battingFile):
#         hitters = []
#         starts = []
#         if not (team == "CTM"): #if this team is not a custom team
#             file = open(battingFile)
#             for line in file: #there's only one "line" in the batting order file, so this is a way to get a hold of it.
#                 batters = line
#             file.close()
#             battingList = batters.split(';')
#             del battingList[-1] #get rid of the last element which is just a newline character returned by the split method.
#             for i in range(len(battingList)): #loop through the 9 spots in the order.
#                 hitters.append([])
#                 starts.append([])
#                 tempBattingList = battingList[i].split(',') #split each spot in the order by its commas.
#                 for j in range(len(tempBattingList)): #loop through the list that was create from the split in the line above.
#                     if (j % 2 == 0): #even numbers will return players.
#                         hitters[i].append(tempBattingList[j])
#                     else: #odd numbers will return starts.
#                         starts[i].append(tempBattingList[j])
#         else: #if this team is a custom team
#             for s in range(9): #add sublists for the part below to use
#                 hitters.append([])
#                 starts.append([])
#         if (len(self.__addedPlayers[index]) == 0): #if there are no added players
#             pass #do nothing
#         else: #if there are added players
#             for i in range(len(self.__addedPlayers[index])): #loop through all the added players for this team
#                 player = self.__addedPlayers[index][i] #nail down the player we're looking at
#                 teams = Importer.findPlayerTeam(player, self.__additionalPlayers_YearList[index][i].get(), False, self.getShowFunctions()) #determine which teams they played on during this year
#                 for team in teams: #loop through those teams
#                     file = open(File.teamBatting(team, self.__additionalPlayers_YearList[index][i].get())) #open this team's batting file
#                     spots = file.read().split(';')[:-1] #split the whole file into individual spots in the lineup
#                     for j in range(len(spots)): #loop through each spot in the lineup
#                         spot = spots[j].split(',') #split this spot in the lineup in order to search through it for this player
#                         for k in range(len(spot)//2): #loop through each player in this spot in the lineup
#                             if (spot[k*2] == player): #if there's a match
#                                 higher = True #initiate the while loop
#                                 counter = 0 #initiate a counter
#                                 while (higher): #loop through the starts list
#                                     if not (counter == len(starts[j])): #if the loop hasn't traversed the entire list yet
#                                         if (int(starts[j][counter]) <= int(spot[(k*2)+1])): #if the player's number of starts at this spot in the lineup are >= this player's starts
#                                             hitters[j].insert(counter, spot[k*2]) #place the added player to this part of the lineup
#                                             starts[j].insert(counter, spot[(k*2)+1]) #place the added player's number of starts into the starts list
#                                             higher = False #break the loop
#                                         else: #otherwise
#                                             counter += 1 #increment
#                                     else: #if the loop has traversed the entire list
#                                         hitters[j].append(spot[k*2]) #append the player to the end of the hitters list at this spot in the lineup
#                                         starts[j].append(spot[(k*2)+1]) #append the player's starts to the end of the starts lsit at this spot in the lineup
#                                         higher = False #break the loop
#                             else: #if there's not a match
#                                 continue #go back to the top
#                     file.close()
#             hitters, starts = self.evenOutBattingOrder(hitters, starts)
#         return (hitters, starts)
#
#     def evenOutBattingOrder(self, hitters, starts):
#         for j in range(9):
#             if (len(hitters[j]) < 5): #if there are less than 5 players at this spot in the lineup
#                 if not (j == 0): #if this is not the leadoff spot
#                     direction = -1
#                 else: #if this is the leadoff spot
#                     direction = 1
#                 counter = 0
#                 pullFromList = 0
#                 while (len(hitters[j]) < 5):
#                     if (abs(pullFromList) < 8):
#                         pullFromList += 1
#                         pullFromList *= direction
#                         for i in range(len(hitters[j+pullFromList])):
#                             if (hitters[j+pullFromList][-(counter+1)] not in hitters[j]):
#                                 hitters[j].append(hitters[j+pullFromList][-(counter+1)])
#                                 starts[j].append(0)
#                             else:
#                                 pass
#                             counter += 1
#                     else:
#                         break
#             else:
#                 continue
#         return (hitters, starts)
#
#     def ensurePlayerUniqueness(self, place, player, batters, players):
#         if player in batters:
#             player = self.getPlayerIncrementally(players[place], player)
#             if (player != "restart"):
#                 return (self.ensurePlayerUniqueness(place, player, batters, players))
#             else:
#                 return ("restart")
#         else:
#             pass
#         if (place == len(batters)):
#             batters.append(player)
#         else:
#             batters[place] = player
#         return (batters)
#
#     def getPlayerIncrementally(self, playerList, currentPlayer):
#         currentPlayerHere = False
#         stepper = 0
#         while not (currentPlayerHere):
#             if (playerList[stepper] == currentPlayer):
#                 currentPlayerHere = True
#             else:
#                 stepper += 1
#         if (stepper < len(playerList)-1):
#             return (playerList[stepper+1])
#         else:
#             return ("restart")
#
#     def ensurePositionUniqueness(self, index, attempt, position, place, players, battingFile, positionFile, league, batters, positions, year):
#         if ((position in positions) or (position in {"none", "RP"}) or (self.__homeYear >= 1973 and league == "American League" and position == "SP")):
#             #print ("is " + batters[place]  + " in")
#             #print (self.__addedPlayers[index])
#             if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
#                 #print ("no\n")
#                 position = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), attempt, league, self.getShowFunctions())
#             else: #if this is an added player to the team
#                 #print ("yes\n")
#                 for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
#                     if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
#                         fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
#                         break
#                     else: #if this player is not the player in question
#                         continue #increment
#                 position = Importer.getPositionIncrementally(batters[place], fieldingFile, attempt, league, self.getShowFunctions())
#             if (position == "none"):
#                 newBatter = self.getPlayerIncrementally(players[place], batters[place])
#                 if (newBatter != "restart"):
#                     batters = self.ensurePlayerUniqueness(place, newBatter, batters, players)
#                     if (batters != "restart"):
#                         attempt = 1
#                         #print ("is " + batters[place]  + " in")
#                         #print (self.__addedPlayers[index])
#                         if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
#                             #print ("no\n")
#                             newPlayerPosition = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), attempt, league, self.getShowFunctions())
#                         else: #if this is an added player to the team
#                             #print ("yes\n")
#                             for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
#                                 if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
#                                     fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
#                                     break
#                                 else: #if this player is not the player in question
#                                     continue #increment
#                             newPlayerPosition = Importer.getPositionIncrementally(batters[place], fieldingFile, attempt, league, self.getShowFunctions())
#                         return (self.ensurePositionUniqueness(index, 2, newPlayerPosition, place, players, battingFile, positionFile, league, batters, positions, year))
#                     else:
#                         return ("restart", "restart")
#                 else:
#                     return ("restart", "restart")
#             else:
#                 return (self.ensurePositionUniqueness(index, attempt+1, position, place, players, battingFile, positionFile, league, batters, positions, year))
#         else:
#             if (place == len(positions)):
#                 positions.append(position)
#             else:
#                 positions[place] = position
#         return (batters, positions)
#
#     def addStartingPitcherToLineup(self, batters, positions, startingPitcher):
#         for i in range(len(positions)):
#             if (positions[-(i+1)] == "SP"):
#                 batters[-(i+1)] = startingPitcher
#                 break
#             else:
#                 continue
#         return (batters)