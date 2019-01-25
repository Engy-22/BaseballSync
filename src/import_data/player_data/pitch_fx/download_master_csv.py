from urllib.request import urlretrieve


def download_master_csv(driver_logger):
    driver_logger.log('\tDownloading MLBAM player database csv')
    urlretrieve('http://crunchtimebaseball.com/master.csv', 'C:\\Users\\Anthony Raimondo\\PycharmProjects\\'
                                                            'baseball-sync\\background\\master.csv')
