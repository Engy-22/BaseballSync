from Importer import Importer
import random
from File import File
from Translator import Translator
from Utility import Utility


class LineupCreator:

    def __init__(self, index, team, year, gameNumber, league, additionalPlayers, additionalPlayers_YearList, homeYear, showFunctions):
        self.setShowFunctions(showFunctions)
        self.__homeYear = homeYear
        self.__additionalPlayers_YearList = additionalPlayers_YearList
        self.setBattingFile(File.teamBatting(Translator.abvToTeam(team), year))
        self.setPitchingFile(File.teamPitching(Translator.abvToTeam(team), year))
        self.setFieldingFile(File.teamFielding(Translator.abvToTeam(team), year))
        self.__addedPlayers = self.unpackAddedPlayers(additionalPlayers)
        players, starts = self.getPlayers(index, team, self.getBattingFile()) #get the team's batting orders from that year in two corresponding lists.
#        print (players)
#        print (starts)
        startingPitcher = self.getStartingPitcher(index, self.getPitchingFile(), gameNumber, year, team)
        self.setLineup(self.controller(index, team, year, gameNumber, str(league), players, starts, startingPitcher))

    def controller(self, index, team, year, gameNumber, league, players, starts, startingPitcher):
        if (self.getShowFunctions()):
            print ("LineupCreator.controller")
        else:
            pass
        place = 0
        batters = []
        positions = []
        while place <= 8:
            batter = self.getPlayerRandomly(players[place], starts[place])
            tempBatters = self.ensurePlayerUniqueness(place, batter, batters, players)
            if (tempBatters != "restart"):
                batters = tempBatters
            else:
                #print ("restart")
                return (self.controller(index, team, year, gameNumber, league, players, starts, startingPitcher))
            if (place == 8 and "SP" not in positions and (self.__homeYear < 1973 or league == "National League")): #if it has come to the last spot in the order of a lineup under National League rules and the pitcher has not yet been add.
                position = "SP" #the next player must be a pitcher
            else:
                #print ("is " + batters[place]  + " in")
                #print (self.__addedPlayers[index])
                if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
                    #print ("no\n")
                    position = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), 1, league, self.getShowFunctions()) #get their primary position
                else: #if this is an added player to the team
                    #print ("yes\n")
                    for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
                        if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
                            fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
                            break
                        else: #if this player is not the player in question
                            continue #increment
                    position = Importer.getPositionIncrementally(batters[place], fieldingFile, 1, league, self.getShowFunctions())
            batters, positions = self.ensurePositionUniqueness(index, 2, position, place, players, self.getBattingFile(), self.getFieldingFile(), league, batters, positions, year)
            if not (batters == "restart" and positions == "restart"):
                place += 1
            else:
                #print ("restart")
                return (self.controller(index, team, year, gameNumber, league, players, starts, startingPitcher))
        if (self.__homeYear < 1973 or league == "National League"):
            batters = self.addStartingPitcherToLineup(batters, positions, startingPitcher)
        else:
            pass
        output = []
        output.append(batters)
        output.append(positions)
        return (output)

    def getStartingPitcher(self, index, pitcherFile, gameNumber, year, team):
        if (self.getShowFunctions()):
            print ("LineupCreator.getStartingPitcher")
        else:
            pass
        def getGame(gameNumber, index, year, team):
            if (self.getShowFunctions()):
                print ("LineupCreator.getStartingPitcher.getGame")
            else:
                pass
            #add up the team's wins and losses from that year to determine how many games they played.
            yearFile = open(File.teamYearInfo(year))
            for line in yearFile:
                if team in line:
                    tempLine = line.split(',')
                    seasonGames = int(tempLine[3]) + int(tempLine[4]) #this is how many games they played that year.
                    break
            #if the gameNumber is higher than the number of games that team played that year, subtract the number of games from that season until the number is <= games played that year.
            while (gameNumber > seasonGames):
                gameNumber -= seasonGames
            return (gameNumber)
        if not (team == "CTM"):
            thisGame = str(getGame(gameNumber, index, year, team))
            file = open(pitcherFile)
            for line in file:
                if (thisGame in line):
                    tempLine = line
                    break
            file.close()
            startingPitcher = tempLine.split(',')[1].split(';')[0]
        else:
            spList = []
            for i in range(len(self.__addedPlayers[index])):
                positionList = []
                playerTeams = Importer.findPlayerTeam(self.__addedPlayers[index][i], self.__additionalPlayers_YearList[index][i].get(), False, self.getShowFunctions())
                for team in playerTeams:
                    positionList += Importer.getPositionIncrementally(self.__addedPlayers[index][i], File.teamFielding(team, self.__additionalPlayers_YearList[index][i].get()), 1, "American League", self.getShowFunctions())
                    if (Utility.removeDuplicates(positionList, self.getShowFunctions())[0] == "SP"):
                        spList.append(self.__addedPlayers[index][i])
                    else:
                        continue
            #gameNumber -= 1 #the first game should be zero so it starts with the first starting pticher in the list
            seasonGames = len(spList)
            while (gameNumber > seasonGames):
                gameNumber -= seasonGames
            startingPitcher = spList[gameNumber-1]
        return (startingPitcher)

    def getPlayers(self, index, team, battingFile):
        if (self.getShowFunctions()):
            print ("LineupCreator.getPlayers")
        else:
            pass
        hitters = []
        starts = []
        if not (team == "CTM"): #if this team is not a custom team
            file = open(battingFile)
            for line in file: #there's only one "line" in the batting order file, so this is a way to get a hold of it.
                batters = line
            file.close()
            battingList = batters.split(';')
            del battingList[-1] #get rid of the last element which is just a newline character returned by the split method.
            for i in range(len(battingList)): #loop through the 9 spots in the order.
                hitters.append([])
                starts.append([])
                tempBattingList = battingList[i].split(',') #split each spot in the order by its commas.
                for j in range(len(tempBattingList)): #loop through the list that was create from the split in the line above.
                    if (j % 2 == 0): #even numbers will return players.
                        hitters[i].append(tempBattingList[j])
                    else: #odd numbers will return starts.
                        starts[i].append(tempBattingList[j])
        else: #if this team is a custom team
            for s in range(9): #add sublists for the part below to use
                hitters.append([])
                starts.append([])
        if (len(self.__addedPlayers[index]) == 0): #if there are no added players
            pass #do nothing
        else: #if there are added players
            for i in range(len(self.__addedPlayers[index])): #loop through all the added players for this team
                player = self.__addedPlayers[index][i] #nail down the player we're looking at
                teams = Importer.findPlayerTeam(player, self.__additionalPlayers_YearList[index][i].get(), False, self.getShowFunctions()) #determine which teams they played on during this year
                for team in teams: #loop through those teams
                    file = open(File.teamBatting(team, self.__additionalPlayers_YearList[index][i].get())) #open this team's batting file
                    spots = file.read().split(';')[:-1] #split the whole file into individual spots in the lineup
                    for j in range(len(spots)): #loop through each spot in the lineup
                        spot = spots[j].split(',') #split this spot in the lineup in order to search through it for this player
                        for k in range(len(spot)//2): #loop through each player in this spot in the lineup
                            if (spot[k*2] == player): #if there's a match
                                higher = True #initiate the while loop
                                counter = 0 #initiate a counter
                                while (higher): #loop through the starts list
                                    if not (counter == len(starts[j])): #if the loop hasn't traversed the entire list yet
                                        if (int(starts[j][counter]) <= int(spot[(k*2)+1])): #if the player's number of starts at this spot in the lineup are >= this player's starts
                                            hitters[j].insert(counter, spot[k*2]) #place the added player to this part of the lineup
                                            starts[j].insert(counter, spot[(k*2)+1]) #place the added player's number of starts into the starts list
                                            higher = False #break the loop
                                        else: #otherwise
                                            counter += 1 #increment
                                    else: #if the loop has traversed the entire list
                                        hitters[j].append(spot[k*2]) #append the player to the end of the hitters list at this spot in the lineup
                                        starts[j].append(spot[(k*2)+1]) #append the player's starts to the end of the starts lsit at this spot in the lineup
                                        higher = False #break the loop
                            else: #if there's not a match
                                continue #go back to the top
                    file.close()
            hitters, starts = self.evenOutBattingOrder(hitters, starts)
        return (hitters, starts)

    def evenOutBattingOrder(self, hitters, starts):
        if (self.getShowFunctions()):
            print ("LinuepCreator.evenOutBattingOrder")
        else:
            pass
        for j in range(9):
            if (len(hitters[j]) < 5): #if there are less than 5 players at this spot in the lineup
                if not (j == 0): #if this is not the leadoff spot
                    direction = -1
                else: #if this is the leadoff spot
                    direction = 1
                counter = 0
                pullFromList = 0
                while (len(hitters[j]) < 5):
                    if (abs(pullFromList) < 8):
                        pullFromList += 1
                        pullFromList *= direction
                        for i in range(len(hitters[j+pullFromList])):
                            if (hitters[j+pullFromList][-(counter+1)] not in hitters[j]):
                                hitters[j].append(hitters[j+pullFromList][-(counter+1)])
                                starts[j].append(0)
                            else:
                                pass
                            counter += 1
                    else:
                        break
            else:
                continue
        return (hitters, starts)

    def getPlayerRandomly(self, players, starts):
        if (self.getShowFunctions()):
            print ("LineupCreator.getPlayerRandomly")
        else:
            pass
        games = 0
        for i in range(len(starts)): #loop through the total number of starts and add them all up.
            games += int(starts[i])
        picker = random.randint(1, games) #get a random integer between 0 and the number of games played.
        tempVar = 0
        for j in range(len(starts)): #loop through the list of starts to find where the random integer falls.
            if (picker <= (int(starts[j]) + tempVar)): #if the picker is in this player's range.
                player = players[j] #return this player.
                break
            else: #if the picker is not in this player's range.
                tempVar += int(starts[j]) #move on and increment tempVar
        return (player)

    def ensurePlayerUniqueness(self, place, player, batters, players):
        if (self.getShowFunctions()):
            print ("LineupCreator.ensurePlayerUniqueness")
        else:
            pass
        if player in batters:
            player = self.getPlayerIncrementally(players[place], player)
            if (player != "restart"):
                return (self.ensurePlayerUniqueness(place, player, batters, players))
            else:
                return ("restart")
        else:
            pass
        if (place == len(batters)):
            batters.append(player)
        else:
            batters[place] = player
        return (batters)

    def getPlayerIncrementally(self, playerList, currentPlayer):
        if (self.getShowFunctions()):
            print ("LineupCreator.getPlayerIncrementally")
        else:
            pass
        currentPlayerHere = False
        stepper = 0
        while not (currentPlayerHere):
            if (playerList[stepper] == currentPlayer):
                currentPlayerHere = True
            else:
                stepper += 1
        if (stepper < len(playerList)-1):
            return (playerList[stepper+1])
        else:
            return ("restart")

    def ensurePositionUniqueness(self, index, attempt, position, place, players, battingFile, positionFile, league, batters, positions, year):
        if (self.getShowFunctions()):
            print ("LineupCreator.ensurePositionUniqueness")
        else:
            pass
        #print (batters)
        #print (positions)
        if ((position in positions) or (position in {"none", "RP"}) or (self.__homeYear >= 1973 and league == "American League" and position == "SP")):
            #print ("is " + batters[place]  + " in")
            #print (self.__addedPlayers[index])
            if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
                #print ("no\n")
                position = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), attempt, league, self.getShowFunctions())
            else: #if this is an added player to the team
                #print ("yes\n")
                for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
                    if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
                        fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
                        break
                    else: #if this player is not the player in question
                        continue #increment
                position = Importer.getPositionIncrementally(batters[place], fieldingFile, attempt, league, self.getShowFunctions())
            if (position == "none"):
                newBatter = self.getPlayerIncrementally(players[place], batters[place])
                if (newBatter != "restart"):
                    batters = self.ensurePlayerUniqueness(place, newBatter, batters, players)
                    if (batters != "restart"):
                        attempt = 1
                        #print ("is " + batters[place]  + " in")
                        #print (self.__addedPlayers[index])
                        if not (batters[place] in self.__addedPlayers[index]): #if this is not an added player to the team
                            #print ("no\n")
                            newPlayerPosition = Importer.getPositionIncrementally(batters[place], self.getFieldingFile(), attempt, league, self.getShowFunctions())
                        else: #if this is an added player to the team
                            #print ("yes\n")
                            for t in range(len(self.__addedPlayers[index])): #loop through the added players list for this team
                                if (self.__addedPlayers[index][t] == batters[place]): #if this player is the player in question
                                    fieldingFile = File.teamFielding(Importer.findPlayerTeam(batters[place], self.__additionalPlayers_YearList[index][t].get(), False, self.getShowFunctions())[0], self.__additionalPlayers_YearList[index][t].get()) #get the first team returned for purposes of finding their position
                                    break
                                else: #if this player is not the player in question
                                    continue #increment
                            newPlayerPosition = Importer.getPositionIncrementally(batters[place], fieldingFile, attempt, league, self.getShowFunctions())
                        return (self.ensurePositionUniqueness(index, 2, newPlayerPosition, place, players, battingFile, positionFile, league, batters, positions, year))
                    else:
                        return ("restart", "restart")
                else:
                    return ("restart", "restart")
            else:
                return (self.ensurePositionUniqueness(index, attempt+1, position, place, players, battingFile, positionFile, league, batters, positions, year))
        else:
            if (place == len(positions)):
                positions.append(position)
            else:
                positions[place] = position
        return (batters, positions)

    def addStartingPitcherToLineup(self, batters, positions, startingPitcher):
        if (self.getShowFunctions()):
            print ("LineupCreator.addStartingPitcherToLineup")
        else:
            pass
        for i in range(len(positions)):
            if (positions[-(i+1)] == "SP"):
                batters[-(i+1)] = startingPitcher
                break
            else:
                continue
        return (batters)

################### Setters ###################
    def setLineup(self, lineup):
        if (self.getShowFunctions()):
            print ("LineupCreator.setLineup")
        else:
            pass
        self.__lineup = lineup

    def setBattingFile(self, battingFile):
        if (self.getShowFunctions()):
            print ("LineupCreator.setBattingFile")
        else:
            pass
        self.__battingFile = battingFile

    def setPitchingFile(self, pitchingFile):
        if (self.getShowFunctions()):
            print ("LineupCreator.setPitchingFile")
        else:
            pass
        self.__pitchingFile = pitchingFile

    def setFieldingFile(self, fieldingFile):
        if (self.getShowFunctions()):
            print ("LineupCreator.setFieldingFile")
        else:
            pass
        self.__fieldingFile = fieldingFile

    def unpackAddedPlayers(self, addedPlayers):
        if (self.getShowFunctions()):
            print ("LineupCreator.unpackAddedPlayers")
        else:
            pass
        tempList = []
        for i in range(len(addedPlayers)):
            tempList.append([])
            for j in range(len(addedPlayers[i])):
                tempList[i].append(addedPlayers[i][j].get())
        return (tempList)

    def setShowFunctions(self, showFunctions):
        self.__showFunctions = showFunctions
################### Setters ###################

################### Getters ###################
    def getLineup(self):
        if (self.getShowFunctions()):
            print ("LineupCreator.getLineup")
        else:
            pass
        return (self.__lineup)

    def getBattingFile(self):
        if (self.getShowFunctions()):
            print ("LineupCreator.getBattingFile")
        else:
            pass
        return (self.__battingFile)

    def getPitchingFile(self):
        if (self.getShowFunctions()):
            print ("LineupCreator.getPitchingFile")
        else:
            pass
        return (self.__pitchingFile)

    def getFieldingFile(self):
        if (self.getShowFunctions()):
            print ("LineupCreator.getFieldingFile")
        else:
            pass
        return (self.__fieldingFile)

    def getShowFunctions(self):
        return (self.__showFunctions)
################### Getters ###################

##for game in range(1000):
##    #print (str(game+1))
##    lineup = LineupCreator(0, "DET", 1978, game+1, "National League", [[], []], [[], []], 2017, False).getLineup()
##    for i in range(9):
##        if (Utility.duplicates(lineup[0], i, False) or Utility.duplicates(lineup[1], i, False)):
##            print ('\n\n\n\n\n\n')
##        else:
##            pass
##    print (lineup)
