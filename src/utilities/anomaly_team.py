def anomaly_team(year):
    if year == 1880:
        return "CIN"
    elif year == 1882:
        return "BAL"
    else:
        raise Exception('Check on ' + str(year))
